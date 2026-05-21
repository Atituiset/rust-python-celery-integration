//! Integration tests: requires a running Redis instance with Keyspace Notifications enabled.
//!
//! Run Redis first:
//!   redis-server --daemonize yes --port 6379
//!   redis-cli CONFIG SET notify-keyspace-events KEA
//!
//! Then run: cargo test --test integration_test

use anyhow::Result;
use celery_redis_producer::{CeleryResult, Producer, ResultListener};
use redis::AsyncCommands;
use serde_json::json;

const TEST_BROKER: &str = "redis://127.0.0.1:6379/15";
const TEST_BACKEND: &str = "redis://127.0.0.1:6379/15";

/// Clean up test keys in Redis DB 15.
async fn cleanup() -> Result<()> {
    let client = redis::Client::open(TEST_BACKEND)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    redis::cmd("FLUSHDB")
        .query_async::<_, ()>(&mut conn)
        .await?;
    Ok(())
}

#[tokio::test]
async fn test_enqueue_and_listen() -> Result<()> {
    // Ensure Redis is available
    let client = redis::Client::open(TEST_BROKER)?;
    if client.get_multiplexed_async_connection().await.is_err() {
        eprintln!("Skipping integration test: Redis not available at {}", TEST_BROKER);
        return Ok(());
    }

    cleanup().await?;

    let producer = Producer::new(TEST_BROKER)?;
    let listener = ResultListener::new(TEST_BACKEND).await?;

    let args = json!(["/test/repo", ["a.c", "b.c"]]);
    let task_id = producer.enqueue("scan.task", args).await?;

    // Simulate a Celery result write directly
    let result = CeleryResult {
        status: "SUCCESS".to_string(),
        result: json!({"status": "ok", "files": 2}),
        traceback: None,
        task_id: task_id.clone(),
        date_done: Some("2026-01-01T00:00:00Z".to_string()),
    };

    let meta_key = format!("celery-task-meta-{}", task_id);
    let mut conn = client.get_multiplexed_async_connection().await?;
    conn.set::<_, _, ()>(&meta_key, serde_json::to_string(&result)?).await?;

    // Wait for Pub/Sub notification
    let received = listener.wait(&task_id, 5).await?;

    assert!(received.is_some(), "Should receive result via Pub/Sub");
    let received = received.unwrap();
    assert_eq!(received.status, "SUCCESS");
    assert_eq!(received.task_id, task_id);

    cleanup().await?;
    Ok(())
}

#[tokio::test]
async fn test_parse_db_from_url() {
    // These tests verify the internal helper behavior via public API.
    // The ResultListener parses DB from URL on creation.

    // DB 0 (default)
    let listener = ResultListener::new("redis://localhost:6379").await;
    assert!(listener.is_ok(), "Should accept URL without DB");

    // DB 7
    let listener = ResultListener::new("redis://localhost:6379/7").await;
    assert!(listener.is_ok(), "Should accept URL with DB 7");

    // DB 15
    let listener = ResultListener::new("redis://localhost:6379/15").await;
    assert!(listener.is_ok(), "Should accept URL with DB 15");
}

#[tokio::test]
async fn test_wait_timeout_returns_none() -> Result<()> {
    let client = redis::Client::open(TEST_BACKEND)?;
    if client.get_multiplexed_async_connection().await.is_err() {
        eprintln!("Skipping integration test: Redis not available");
        return Ok(());
    }

    cleanup().await?;

    let listener = ResultListener::new(TEST_BACKEND).await?;

    // Wait for a non-existent task
    let result = listener.wait("non-existent-task-id", 1).await?;
    assert!(result.is_none(), "Should return None on timeout");

    cleanup().await?;
    Ok(())
}
