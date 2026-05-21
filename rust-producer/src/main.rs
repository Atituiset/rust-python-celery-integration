use anyhow::Result;
use celery_redis_producer::{Producer, ResultListener};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let broker_url = std::env::var("REDIS_BROKER_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/6".to_string());
    let backend_url = std::env::var("REDIS_BACKEND_URL")
        .unwrap_or_else(|_| "redis://127.0.0.1:6379/7".to_string());

    let producer = Producer::new(&broker_url)?;
    let listener = ResultListener::new(&backend_url).await?;

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
