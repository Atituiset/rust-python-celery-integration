use anyhow::{Context, Result};
use base64::Engine;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

// =============================================================================
// Celery Protocol v2 强类型消息体
// =============================================================================

/// Celery v2 协议顶层消息信封。
/// Rust 端通过 serde 强类型约束，彻底杜绝运行时字段拼写或类型错误。
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
// Producer: 推送任务 (生产路径 —— 兼容 Python kombu)
// =============================================================================

/// 构造标准 Celery v2 消息体并 LPUSH 到 Redis DB 6。
///
/// 所有字段均通过强类型 Struct 约束，编译期即可发现拼写/类型错误。
/// 消息格式与 Python kombu 完全对齐，经过实际 Worker 消费验证。
async fn push_task(
    conn: &mut redis::aio::MultiplexedConnection,
    task_name: &str,
    args: serde_json::Value,
) -> Result<String> {
    let task_id = Uuid::new_v4().to_string();
    let reply_to = Uuid::new_v4().to_string();

    // body = [args, kwargs, embed]
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
// Poller: 轮询结果
// =============================================================================

/// 从 Redis DB 7 轮询任务结果。
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

        sleep(Duration::from_millis(500)).await;
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

// =============================================================================
// Main
// =============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")
        .context("Failed to create Redis client")?;

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

    println!("[Poller] Waiting for result...");
    match poll_result(&mut conn, &task_id, 30).await? {
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
            println!("[Poller] No result received within timeout");
        }
    }

    Ok(())
}
