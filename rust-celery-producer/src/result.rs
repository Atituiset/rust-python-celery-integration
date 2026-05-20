//! Result retrieval: event-driven via Redis Keyspace Notifications.

use anyhow::{Context, Result};
use futures_util::StreamExt;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{oneshot, Mutex};
use tokio::time::timeout;

/// Celery task result as stored in the Redis backend.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CeleryResult {
    pub status: String,
    #[serde(default)]
    pub result: serde_json::Value,
    #[serde(default)]
    pub traceback: Option<String>,
    #[serde(default)]
    pub task_id: String,
    #[serde(default)]
    pub date_done: Option<String>,
}

impl CeleryResult {
    /// Returns true if the task has reached a terminal state.
    pub fn is_final(&self) -> bool {
        self.status == "SUCCESS" || self.status == "FAILURE"
    }
}

type TaskRegistry = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;

/// Global result listener backed by a single Redis Pub/Sub connection.
///
/// Subscribes to `__keyevent@<db>__:set` and wakes up waiting task
/// coroutines via in-memory `oneshot` channels.
#[derive(Clone)]
pub struct ResultListener {
    registry: TaskRegistry,
    client: redis::Client,
}

impl ResultListener {
    /// Create and start the listener.
    ///
    /// The URL should include the backend database number, e.g.
    /// `redis://localhost:6379/7`.
    pub async fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("Failed to create Redis client")?;

        let registry: TaskRegistry = Arc::new(Mutex::new(HashMap::new()));
        let registry_clone = registry.clone();
        let client_clone = client.clone();

        tokio::spawn(async move {
            let mut pubsub = match client_clone.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[celery-redis-producer] PubSub creation failed: {}", e);
                    return;
                }
            };

            if let Err(e) = pubsub.subscribe("__keyevent@7__:set").await {
                eprintln!("[celery-redis-producer] Subscribe failed: {}", e);
                return;
            }

            let mut msg_stream = pubsub.on_message();
            while let Some(msg) = msg_stream.next().await {
                if let Ok(key) = msg.get_payload::<String>() {
                    if key.starts_with("celery-task-meta-") {
                        let task_id = key
                            .trim_start_matches("celery-task-meta-")
                            .to_string();
                        let mut map = registry_clone.lock().await;
                        if let Some(sender) = map.remove(&task_id) {
                            let _ = sender.send(());
                        }
                    }
                }
            }
        });

        Ok(Self { registry, client })
    }

    /// Wait for a task result with double-checked locking.
    ///
    /// Handles Celery's two-phase write (STARTED → SUCCESS) by re-registering
    /// the oneshot channel when an intermediate state is observed.
    pub async fn wait(
        &self,
        task_id: &str,
        timeout_secs: u64,
    ) -> Result<Option<CeleryResult>> {
        let meta_key = format!("celery-task-meta-{}", task_id);
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to connect to Redis")?;

        redis::cmd("SELECT")
            .arg(7)
            .query_async::<_, ()>(&mut conn)
            .await
            .context("SELECT DB 7 failed")?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

        async fn check_final(
            conn: &mut redis::aio::MultiplexedConnection,
            meta_key: &str,
        ) -> Option<CeleryResult> {
            let data: String = conn
                .get::<_, Option<String>>(meta_key)
                .await
                .ok()
                .flatten()?;
            let result: CeleryResult = serde_json::from_str(&data).ok()?;
            result.is_final().then_some(result)
        }

        if let Some(result) = check_final(&mut conn, &meta_key).await {
            return Ok(Some(result));
        }

        loop {
            let remaining =
                deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let mut map = self.registry.lock().await;
                map.remove(task_id);
                return Ok(None);
            }

            let (tx, rx) = oneshot::channel();
            {
                let mut map = self.registry.lock().await;
                map.insert(task_id.to_string(), tx);
            }

            if let Some(result) = check_final(&mut conn, &meta_key).await {
                let mut map = self.registry.lock().await;
                map.remove(task_id);
                return Ok(Some(result));
            }

            match timeout(remaining, rx).await {
                Ok(Ok(_)) => {
                    if let Some(result) = check_final(&mut conn, &meta_key).await {
                        return Ok(Some(result));
                    }
                }
                Ok(Err(_)) => return Ok(None),
                Err(_) => {
                    let mut map = self.registry.lock().await;
                    map.remove(task_id);
                    return Ok(None);
                }
            }
        }
    }
}
