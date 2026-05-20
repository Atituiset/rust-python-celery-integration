//! Rust producer for Python Celery via Redis.
//!
//! Zero third-party Celery dependency. Manually constructs Celery Protocol v2
//! messages that are binary-compatible with Python kombu.
//!
//! # Example
//!
//! ```no_run
//! use celery_redis_producer::{Producer, ResultListener};
//! use serde_json::json;
//! use std::time::Duration;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let producer = Producer::new("redis://localhost:6379/6").await?;
//! let args = json!(["/repo", ["file.c"]]);
//! let task_id = producer.enqueue("scan.task", args).await?;
//!
//! let listener = ResultListener::new("redis://localhost:6379/7").await?;
//! let result = listener.wait(&task_id, Duration::from_secs(30)).await?;
//! # Ok(())
//! # }
//! ```

pub mod protocol;
pub mod producer;
pub mod result;

pub use producer::Producer;
pub use result::{CeleryResult, ResultListener};
