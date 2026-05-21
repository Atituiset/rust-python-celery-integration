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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_celery_message_serialization() {
        let msg = CeleryMessage {
            body: "dGVzdA==".to_string(),
            content_encoding: "utf-8".to_string(),
            content_type: "application/json".to_string(),
            headers: CeleryHeaders {
                lang: "py".to_string(),
                task: "scan.task".to_string(),
                id: "test-id".to_string(),
                root_id: "test-id".to_string(),
                parent_id: None,
                group: None,
                meth: None,
                shadow: None,
                eta: None,
                expires: None,
                retries: 0,
                timelimit: [None, None],
                argsrepr: "[\"/repo\", [\"a.c\"]]".to_string(),
                kwargsrepr: "{}".to_string(),
                origin: "rust-producer".to_string(),
            },
            properties: CeleryProperties {
                correlation_id: "test-id".to_string(),
                reply_to: "reply-id".to_string(),
                delivery_mode: 2,
                delivery_info: DeliveryInfo {
                    exchange: "".to_string(),
                    routing_key: "celery".to_string(),
                },
                priority: 0,
                body_encoding: "base64".to_string(),
                delivery_tag: "tag-1".to_string(),
            },
        };

        let json = serde_json::to_string(&msg).unwrap();

        // Verify all required fields are present
        assert!(json.contains("content-encoding"), "missing content-encoding");
        assert!(json.contains("content-type"), "missing content-type");
        assert!(json.contains("correlation_id"), "missing correlation_id");
        assert!(json.contains("delivery_tag"), "missing delivery_tag");
        assert!(json.contains("routing_key\":\"celery\""), "missing routing_key");
        assert!(json.contains("body_encoding"), "missing body_encoding");
    }

    #[test]
    fn test_headers_defaults() {
        let headers = CeleryHeaders {
            lang: "py".to_string(),
            task: "test.task".to_string(),
            id: "uuid".to_string(),
            root_id: "uuid".to_string(),
            parent_id: None,
            group: None,
            meth: None,
            shadow: None,
            eta: None,
            expires: None,
            retries: 0,
            timelimit: [None, None],
            argsrepr: "[]".to_string(),
            kwargsrepr: "{}".to_string(),
            origin: "test".to_string(),
        };

        let json = serde_json::to_string(&headers).unwrap();
        assert!(json.contains("\"lang\":\"py\""));
        assert!(json.contains("\"task\":\"test.task\""));
        assert!(json.contains("\"retries\":0"));
    }
}
