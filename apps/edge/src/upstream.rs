use auth::sign_origin_request;
use axum::http::{header::HeaderName, header::HeaderValue, HeaderMap, Method};
use chrono::Utc;
use std::time::Duration;
use thiserror::Error;
use tracing::{debug, error};

#[derive(Debug, Error)]
pub enum UpstreamError {
    #[error("Upstream connection or HTTP error: {0}")]
    Network(#[from] reqwest::Error),
    #[allow(dead_code)]
    #[error("Invalid header format: {0}")]
    InvalidHeader(String),
}

#[derive(Clone)]
pub struct UpstreamClient {
    client: reqwest::Client,
}

impl Default for UpstreamClient {
    fn default() -> Self {
        Self::new()
    }
}

impl UpstreamClient {
    pub fn new() -> Self {
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(50)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Duration::from_secs(15))
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to initialize upstream HTTP connection pool");

        Self { client }
    }

    /// Fetches a media asset from Origin using the persistent connection pool,
    /// adding HMAC origin protection headers and propagating range/cache headers.
    pub async fn fetch_stream(
        &self,
        origin_base_url: &str,
        origin_auth_secret: &str,
        edge_id: &str,
        method: Method,
        path_and_query: &str,
        client_headers: &HeaderMap,
    ) -> Result<reqwest::Response, UpstreamError> {
        let target_url = format!(
            "{}{}",
            origin_base_url.trim_end_matches('/'),
            path_and_query
        );

        let timestamp = Utc::now().timestamp();
        let nonce = uuid::Uuid::new_v4().simple().to_string();
        let signature = sign_origin_request(
            origin_auth_secret.as_bytes(),
            method.as_str(),
            path_and_query,
            timestamp,
            edge_id,
            &nonce,
        );

        let mut req_builder = self
            .client
            .request(method, &target_url)
            .header("X-Edge-ID", edge_id)
            .header("X-Timestamp", timestamp.to_string())
            .header("X-Nonce", nonce)
            .header("X-Signature", signature);

        // Propagate essential client headers to Origin
        let headers_to_forward = [
            "range",
            "if-none-match",
            "if-modified-since",
            "accept",
            "user-agent",
        ];

        for name in &headers_to_forward {
            if let Some(val) = client_headers.get(*name) {
                if let Ok(header_name) = HeaderName::from_bytes(name.as_bytes()) {
                    if let Ok(header_val) = HeaderValue::from_bytes(val.as_bytes()) {
                        req_builder = req_builder.header(header_name, header_val);
                    }
                }
            }
        }

        debug!(
            url = %target_url,
            edge_id = %edge_id,
            "Forwarding request to origin with HMAC signature"
        );

        let response = req_builder.send().await.map_err(|e| {
            error!(error = %e, url = %target_url, "Failed to connect to origin");
            UpstreamError::Network(e)
        })?;

        Ok(response)
    }
}
