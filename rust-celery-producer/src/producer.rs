//! Task producer: sends Celery-compatible messages to a Redis broker.

use crate::protocol::{CeleryHeaders, CeleryMessage, CeleryProperties, DeliveryInfo};
use anyhow::{Context, Result};
use base64::Engine;
use redis::AsyncCommands;
use serde_json::json;
use uuid::Uuid;

/// Celery task producer backed by Redis.
///
/// Constructs standard Celery v2 messages and `LPUSH`es them into the
/// configured Redis broker database.
pub struct Producer {
    client: redis::Client,
}

impl Producer {
    /// Create a new Producer connected to the given Redis URL.
    ///
    /// The URL should include the broker database number, e.g.
    /// `redis://localhost:6379/6`.
    pub fn new(redis_url: &str) -> Result<Self> {
        let client = redis::Client::open(redis_url)
            .context("Failed to create Redis client")?;
        Ok(Self { client })
    }

    /// Enqueue a task with the given name and JSON arguments.
    ///
    /// Returns the generated Celery task ID (UUID v4).
    pub async fn enqueue(
        &self,
        task_name: &str,
        args: serde_json::Value,
    ) -> Result<String> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await
            .context("Failed to connect to Redis")?;

        let task_id = Uuid::new_v4().to_string();
        let reply_to = Uuid::new_v4().to_string();

        let body = json!([
            args,
            {},
            {
                "callbacks": null,
                "errbacks": null,
                "chain": null,
                "chord": null,
            }
        ]);
        let body_b64 = base64::engine::general_purpose::STANDARD
            .encode(body.to_string().as_bytes());

        let message = CeleryMessage {
            body: body_b64,
            content_encoding: "utf-8".to_string(),
            content_type: "application/json".to_string(),
            headers: CeleryHeaders {
                lang: "py".to_string(),
                task: task_name.to_string(),
                id: task_id.clone(),
                root_id: task_id.clone(),
                parent_id: None,
                group: None,
                meth: None,
                shadow: None,
                eta: None,
                expires: None,
                retries: 0,
                timelimit: [None, None],
                argsrepr: format!("{:?}", args),
                kwargsrepr: "{}".to_string(),
                origin: "rust-producer".to_string(),
            },
            properties: CeleryProperties {
                correlation_id: task_id.clone(),
                reply_to,
                delivery_mode: 2,
                delivery_info: DeliveryInfo {
                    exchange: "".to_string(),
                    routing_key: "celery".to_string(),
                },
                priority: 0,
                body_encoding: "base64".to_string(),
                delivery_tag: Uuid::new_v4().to_string(),
            },
        };

        let payload = serde_json::to_string(&message)
            .context("Failed to serialize CeleryMessage")?;

        conn.lpush::<_, _, ()>("celery", payload)
            .await
            .context("LPUSH to celery queue failed")?;

        Ok(task_id)
    }
}
