use anyhow::{Context, Result};
use base64::Engine;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use futures_util::StreamExt;
use tokio::sync::{oneshot, Mutex};
use tokio::time::timeout;
use uuid::Uuid;

// =============================================================================
// Celery Protocol v2 强类型消息体
// =============================================================================

#[derive(Serialize, Debug)]
struct CeleryMessage {
    body: String,
    #[serde(rename = "content-encoding")]
    content_encoding: String,
    #[serde(rename = "content-type")]
    content_type: String,
    headers: CeleryHeaders,
    properties: CeleryProperties,
}

#[derive(Serialize, Debug)]
struct CeleryHeaders {
    lang: String,
    task: String,
    id: String,
    root_id: String,
    parent_id: Option<String>,
    group: Option<String>,
    meth: Option<String>,
    shadow: Option<String>,
    eta: Option<String>,
    expires: Option<String>,
    retries: i32,
    timelimit: [Option<i32>; 2],
    argsrepr: String,
    kwargsrepr: String,
    origin: String,
}

#[derive(Serialize, Debug)]
struct CeleryProperties {
    correlation_id: String,
    reply_to: String,
    delivery_mode: i32,
    delivery_info: DeliveryInfo,
    priority: i32,
    body_encoding: String,
    delivery_tag: String,
}

#[derive(Serialize, Debug)]
struct DeliveryInfo {
    exchange: String,
    routing_key: String,
}

// =============================================================================
// 全局事件总线: Redis Keyspace Notifications + oneshot 精确唤醒
// =============================================================================

type TaskRegistry = Arc<Mutex<HashMap<String, oneshot::Sender<()>>>>;

/// 全局结果监听器。
///
/// 维护**单一** Redis Pub/Sub 长连接，订阅 DB 7 的 `set` 事件。
/// 收到 `celery-task-meta-*` 的写入事件后，通过内存 oneshot 通道
/// 精确唤醒对应任务协程，零轮询、零无效 Redis 请求。
#[derive(Clone)]
struct ResultListener {
    registry: TaskRegistry,
}

impl ResultListener {
    /// 启动后台监听器。整个应用生命周期只应调用一次。
    async fn start(client: redis::Client) -> Result<Self> {
        let registry: TaskRegistry = Arc::new(Mutex::new(HashMap::new()));
        let registry_clone = registry.clone();

        tokio::spawn(async move {
            let mut pubsub = match client.get_async_pubsub().await {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("[Listener] Failed to create pubsub: {}", e);
                    return;
                }
            };

            if let Err(e) = pubsub.subscribe("__keyevent@7__:set").await {
                eprintln!("[Listener] Subscribe failed: {}", e);
                return;
            }

            println!("[Listener] Global Redis Keyspace listener started");

            let mut msg_stream = pubsub.on_message();
            while let Some(msg) = msg_stream.next().await {
                if let Ok(key) = msg.get_payload::<String>() {
                    if key.starts_with("celery-task-meta-") {
                        let task_id = key.trim_start_matches("celery-task-meta-").to_string();
                        let mut map = registry_clone.lock().await;
                        if let Some(sender) = map.remove(&task_id) {
                            let _ = sender.send(());
                        }
                    }
                }
            }

            eprintln!("[Listener] PubSub stream ended");
        });

        Ok(Self { registry })
    }

    /// 防竞态双重检查等待任务结果。
    ///
    /// Celery 会先写入 `STARTED` 再写入 `SUCCESS`，因此收到事件后
    /// 需检查状态，中间状态则重新注册 oneshot 继续等待。
    async fn wait(
        &self,
        conn: &mut redis::aio::MultiplexedConnection,
        task_id: &str,
        timeout_secs: u64,
    ) -> Result<Option<CeleryResult>> {
        let meta_key = format!("celery-task-meta-{}", task_id);

        redis::cmd("SELECT")
            .arg(7)
            .query_async::<_, ()>(conn)
            .await
            .context("SELECT DB 7 failed")?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout_secs);

        // 辅助：检查是否已有最终结果
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

        // 第一道防线
        if let Some(result) = check_final(conn, &meta_key).await {
            return Ok(Some(result));
        }

        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                let mut map = self.registry.lock().await;
                map.remove(task_id);
                println!("[Listener] Timeout after {}s", timeout_secs);
                return Ok(None);
            }

            // 注册 oneshot
            let (tx, rx) = oneshot::channel();
            {
                let mut map = self.registry.lock().await;
                map.insert(task_id.to_string(), tx);
            }

            // 第二道防线（注册后窗口期）
            if let Some(result) = check_final(conn, &meta_key).await {
                let mut map = self.registry.lock().await;
                map.remove(task_id);
                return Ok(Some(result));
            }

            // 等待事件唤醒
            match timeout(remaining, rx).await {
                Ok(Ok(_)) => {
                    if let Some(result) = check_final(conn, &meta_key).await {
                        return Ok(Some(result));
                    }
                    // STARTED 等中间状态：继续循环，重新注册 oneshot
                }
                Ok(Err(_)) => return Ok(None),
                Err(_) => {
                    let mut map = self.registry.lock().await;
                    map.remove(task_id);
                    println!("[Listener] Timeout after {}s", timeout_secs);
                    return Ok(None);
                }
            }
        }
    }
}

// =============================================================================
// Producer: 推送任务
// =============================================================================

async fn push_task(
    conn: &mut redis::aio::MultiplexedConnection,
    task_name: &str,
    args: serde_json::Value,
) -> Result<String> {
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
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(body.to_string().as_bytes());

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

    redis::cmd("SELECT")
        .arg(6)
        .query_async::<_, ()>(conn)
        .await
        .context("SELECT DB 6 failed")?;

    let payload = serde_json::to_string(&message)
        .context("Failed to serialize CeleryMessage")?;

    conn.lpush::<_, _, ()>("celery", payload)
        .await
        .context("LPUSH to celery queue failed")?;

    println!("[Producer] Task pushed: id={}, task={}", task_id, task_name);
    Ok(task_id)
}

// =============================================================================
// Legacy Poller: 轮询方式（保留作为 fallback）
// =============================================================================

#[allow(dead_code)]
async fn poll_result(
    conn: &mut redis::aio::MultiplexedConnection,
    task_id: &str,
    max_wait_secs: u64,
) -> Result<Option<CeleryResult>> {
    redis::cmd("SELECT")
        .arg(7)
        .query_async::<_, ()>(conn)
        .await
        .context("SELECT DB 7 failed")?;

    let key = format!("celery-task-meta-{}", task_id);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(max_wait_secs);

    while tokio::time::Instant::now() < deadline {
        let raw: Option<String> = conn.get(&key).await?;

        if let Some(data) = raw {
            let parsed: CeleryResult = serde_json::from_str(&data)?;
            println!("[Poller] Status: {}", parsed.status);

            if parsed.status == "SUCCESS" || parsed.status == "FAILURE" {
                return Ok(Some(parsed));
            }
        }

        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    println!("[Poller] Timeout after {}s", max_wait_secs);
    Ok(None)
}

#[derive(Debug, Deserialize, Serialize)]
struct CeleryResult {
    status: String,
    #[serde(default)]
    result: serde_json::Value,
    #[serde(default)]
    traceback: Option<String>,
    #[serde(default)]
    task_id: String,
    #[serde(default)]
    date_done: Option<String>,
}

impl CeleryResult {
    fn is_final(&self) -> bool {
        self.status == "SUCCESS" || self.status == "FAILURE"
    }
}

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")
        .context("Failed to create Redis client")?;

    // 启动全局事件总线（单一 Pub/Sub 长连接）
    let listener = ResultListener::start(client.clone()).await?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis")?;

    println!("[Producer] Connected to Redis");

    let args = json!([
        "/path/to/repo",
        ["file1.c", "file2.c", "main.c"]
    ]);

    let task_id = push_task(&mut conn, "scan.task", args).await?;

    // 事件驱动等待结果（零轮询）
    println!("[Listener] Waiting for result...");
    match listener.wait(&mut conn, &task_id, 30).await? {
        Some(result) => {
            println!("\n========== RESULT ==========");
            println!("Status : {}", result.status);
            println!("Task ID: {}", result.task_id);
            println!(
                "Result : {}",
                serde_json::to_string_pretty(&result.result)?
            );
            println!("============================\n");
        }
        None => {
            println!("[Listener] No result received within timeout");
        }
    }

    Ok(())
}
