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
//! // Load from .env or environment variables
//! let broker_url = std::env::var("REDIS_BROKER_URL")
//!     .unwrap_or_else(|_| "redis://localhost:6379/6".to_string());
//! let backend_url = std::env::var("REDIS_BACKEND_URL")
//!     .unwrap_or_else(|_| "redis://localhost:6379/7".to_string());
//!
//! let producer = Producer::new(&broker_url)?;
//! let args = json!(["/repo", ["file.c"]]);
//! let task_id = producer.enqueue("scan.task", args).await?;
//!
//! let listener = ResultListener::new(&backend_url).await?;
//! let result = listener.wait(&task_id, Duration::from_secs(30)).await?;
//! # Ok(())
//! # }
//! ```

pub mod protocol;
pub mod producer;
pub mod result;

pub use producer::Producer;
pub use result::{CeleryResult, ResultListener};
