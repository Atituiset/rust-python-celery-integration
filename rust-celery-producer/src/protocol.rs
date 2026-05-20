//! Celery Protocol v2 message definitions.
//!
//! All structs are `#[derive(Serialize)]` to guarantee compile-time field
//! safety when constructing messages consumed by Python kombu.

use serde::Serialize;

/// Top-level Celery v2 protocol envelope.
#[derive(Serialize, Debug, Clone)]
pub struct CeleryMessage {
    pub body: String,
    #[serde(rename = "content-encoding")]
    pub content_encoding: String,
    #[serde(rename = "content-type")]
    pub content_type: String,
    pub headers: CeleryHeaders,
    pub properties: CeleryProperties,
}

/// Headers section of a Celery v2 message.
#[derive(Serialize, Debug, Clone)]
pub struct CeleryHeaders {
    pub lang: String,
    pub task: String,
    pub id: String,
    #[serde(rename = "root_id")]
    pub root_id: String,
    #[serde(rename = "parent_id")]
    pub parent_id: Option<String>,
    pub group: Option<String>,
    pub meth: Option<String>,
    pub shadow: Option<String>,
    pub eta: Option<String>,
    pub expires: Option<String>,
    pub retries: i32,
    pub timelimit: [Option<i32>; 2],
    pub argsrepr: String,
    pub kwargsrepr: String,
    pub origin: String,
}

/// Properties section of a Celery v2 message.
#[derive(Serialize, Debug, Clone)]
pub struct CeleryProperties {
    pub correlation_id: String,
    pub reply_to: String,
    pub delivery_mode: i32,
    pub delivery_info: DeliveryInfo,
    pub priority: i32,
    #[serde(rename = "body_encoding")]
    pub body_encoding: String,
    #[serde(rename = "delivery_tag")]
    pub delivery_tag: String,
}

/// Delivery info nested inside properties.
#[derive(Serialize, Debug, Clone)]
pub struct DeliveryInfo {
    pub exchange: String,
    pub routing_key: String,
}
