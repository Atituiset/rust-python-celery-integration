use anyhow::{Context, Result};
use base64::Engine;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

/// Push a Celery v2 protocol task to Redis DB 6 (broker).
async fn push_task(
    conn: &mut redis::aio::MultiplexedConnection,
    task_name: &str,
    args: serde_json::Value,
) -> Result<String> {
    let task_id = Uuid::new_v4().to_string();
    let reply_to = Uuid::new_v4().to_string();

    // Celery message body: [args, kwargs, embed]
    let body = json!([
        args,
        {}, // kwargs
        {
            "callbacks": null,
            "errbacks": null,
            "chain": null,
            "chord": null,
        }
    ]);
    let body_b64 = base64::engine::general_purpose::STANDARD.encode(body.to_string().as_bytes());

    let message = json!({
        "body": body_b64,
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {
            "lang": "py",
            "task": task_name,
            "id": task_id,
            "root_id": task_id,
            "parent_id": null,
            "group": null,
            "meth": null,
            "shadow": null,
            "eta": null,
            "expires": null,
            "retries": 0,
            "timelimit": [null, null],
            "argsrepr": format!("{:?}", args),
            "kwargsrepr": "{}",
            "origin": "rust-producer",
        },
        "properties": {
            "correlation_id": task_id,
            "reply_to": reply_to,
            "delivery_mode": 2,
            "delivery_info": {
                "exchange": "",
                "routing_key": "celery",
            },
            "priority": 0,
            "body_encoding": "base64",
            "delivery_tag": Uuid::new_v4().to_string(),
        }
    });

    // Switch to broker DB (6) and push
    redis::cmd("SELECT")
        .arg(6)
        .query_async::<_, ()>(conn)
        .await
        .context("SELECT DB 6 failed")?;

    conn.lpush::<_, _, ()>("celery", message.to_string())
        .await
        .context("LPUSH to celery queue failed")?;

    println!("[Producer] Task pushed: id={}, task={}", task_id, task_name);
    Ok(task_id)
}

/// Poll result from Redis DB 7 (backend).
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

#[tokio::main]
async fn main() -> Result<()> {
    // Connect to Redis
    let client = redis::Client::open("redis://127.0.0.1:6379/")
        .context("Failed to create Redis client")?;

    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .context("Failed to connect to Redis")?;

    println!("[Producer] Connected to Redis");

    // Push task
    let args = json!([
        "/path/to/repo",
        ["file1.c", "file2.c", "main.c"]
    ]);

    let task_id = push_task(&mut conn, "scan.task", args).await?;

    // Poll result
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
