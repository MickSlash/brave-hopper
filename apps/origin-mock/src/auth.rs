use auth::{verify_origin_request_hardened, AuthError, NonceTracker, OriginRequestParams};
use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use tracing::warn;

#[derive(Clone)]
pub struct OriginAuthState {
    pub primary_secret: Arc<Vec<u8>>,
    pub fallback_secret: Option<Arc<Vec<u8>>>,
    pub nonce_tracker: NonceTracker,
    pub max_skew_secs: i64,
}

/// Axum middleware that validates HMAC signatures on protected origin endpoints,
/// enforcing anti-replay nonce tracking and supporting dual-key secret rotation.
pub async fn verify_origin_hmac(
    state: OriginAuthState,
    req: Request<Body>,
    next: Next,
) -> Response {
    let headers = req.headers();

    let edge_id = match headers.get("X-Edge-ID").and_then(|h| h.to_str().ok()) {
        Some(id) => id,
        None => {
            warn!("Origin request rejected: missing X-Edge-ID header");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing X-Edge-ID header" })),
            )
                .into_response();
        }
    };

    let timestamp_str = match headers.get("X-Timestamp").and_then(|h| h.to_str().ok()) {
        Some(ts) => ts,
        None => {
            warn!("Origin request rejected: missing X-Timestamp header");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing X-Timestamp header" })),
            )
                .into_response();
        }
    };

    let timestamp: i64 = match timestamp_str.parse() {
        Ok(ts) => ts,
        Err(_) => {
            warn!("Origin request rejected: invalid timestamp format");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Invalid X-Timestamp header" })),
            )
                .into_response();
        }
    };

    let nonce = match headers.get("X-Nonce").and_then(|h| h.to_str().ok()) {
        Some(n) => n,
        None => {
            warn!("Origin request rejected: missing X-Nonce header");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing X-Nonce header" })),
            )
                .into_response();
        }
    };

    let signature_hex = match headers.get("X-Signature").and_then(|h| h.to_str().ok()) {
        Some(sig) => sig,
        None => {
            warn!("Origin request rejected: missing X-Signature header");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "Missing X-Signature header" })),
            )
                .into_response();
        }
    };

    let method = req.method().as_str();
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(req.uri().path());

    let params = OriginRequestParams {
        method,
        path_and_query,
        timestamp,
        edge_id,
        nonce,
        signature_hex,
    };

    let current_time = Utc::now().timestamp();
    let fallback = state.fallback_secret.as_ref().map(|f| f.as_slice());

    match verify_origin_request_hardened(
        &state.primary_secret,
        fallback,
        &params,
        &state.nonce_tracker,
        state.max_skew_secs,
        current_time,
    ) {
        Ok(()) => next.run(req).await,
        Err(AuthError::NonceReplayed { edge_id, nonce }) => {
            warn!(
                edge_id = %edge_id,
                nonce = %nonce,
                "Origin replay attack prevented: duplicate nonce detected"
            );
            (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "Replay attack detected: nonce already used",
                    "code": "NONCE_REPLAYED"
                })),
            )
                .into_response()
        }
        Err(AuthError::TimestampDriftExceeded {
            drift_secs,
            max_skew_secs,
        }) => {
            warn!(
                drift_secs = drift_secs,
                max_skew_secs = max_skew_secs,
                edge_id = %edge_id,
                "Origin request timestamp drift exceeded allowable window"
            );
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({
                    "error": format!("Timestamp drift exceeded ({}s > {}s)", drift_secs, max_skew_secs),
                    "code": "TIMESTAMP_DRIFT_EXCEEDED"
                })),
            )
                .into_response()
        }
        Err(err) => {
            warn!(error = %err, edge_id = %edge_id, "Origin HMAC verification failed");
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": format!("Unauthorized: {}", err) })),
            )
                .into_response()
        }
    }
}
