use anyhow::Result;
use celery_redis_producer::{Producer, ResultListener};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = Producer::new("redis://127.0.0.1:6379/6")?;
    let listener = ResultListener::new("redis://127.0.0.1:6379/7").await?;

    let args = json!([
        "/path/to/repo",
        ["file1.c", "file2.c", "main.c"]
    ]);

    let task_id = producer.enqueue("scan.task", args).await?;
    println!("[Producer] Task pushed: id={}", task_id);

    println!("[Listener] Waiting for result...");
    match listener.wait(&task_id, 30).await? {
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
