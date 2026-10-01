use axum::{
    extract::{ConnectInfo, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use chrono::Utc;
use protocol::StreamTokenClaims;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::net::SocketAddr;

use crate::{
    registry::{ScheduledEdge, SchedulingCriteria},
    state::AppState,
};

#[derive(Debug, Deserialize)]
pub struct PlaybackRequest {
    pub stream_id: String,
    pub preferred_edge: Option<String>,
    pub preferred_region: Option<String>,
    pub client_ip: Option<String>,
    #[serde(default = "default_validity_secs")]
    pub validity_secs: i64,
    #[serde(default = "default_redirect")]
    pub redirect: bool,
    #[serde(default = "default_manifest")]
    pub manifest: String,
}

fn default_validity_secs() -> i64 {
    3600 // 1 hour
}

fn default_redirect() -> bool {
    true
}

fn default_manifest() -> String {
    "master.m3u8".to_string()
}

#[derive(Debug, Serialize)]
pub struct PlaybackResponse {
    pub stream_id: String,
    pub selected_edge: ScheduledEdge,
    pub playback_url: String,
    pub expires_at: i64,
    pub signature: String,
}

/// GET /api/v1/streams/play?stream_id=...&redirect=true|false
pub async fn handle_playback(
    State(state): State<AppState>,
    Query(query): Query<PlaybackRequest>,
    headers: HeaderMap,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
) -> Response {
    let criteria = SchedulingCriteria {
        preferred_edge: query.preferred_edge,
        preferred_region: query.preferred_region,
    };

    let edge = match state.registry.schedule_edge(&criteria).await {
        Some(e) => e,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "error": "No healthy edge nodes available in cluster",
                    "code": "EDGE_CLUSTER_UNAVAILABLE"
                })),
            )
                .into_response();
        }
    };

    // Determine client IP for optional IP pinning
    let resolved_client_ip = if let Some(ip) = query.client_ip {
        Some(ip)
    } else if let Some(forwarded) = headers.get("x-forwarded-for") {
        forwarded
            .to_str()
            .ok()
            .and_then(|s| s.split(',').next())
            .map(|s| s.trim().to_string())
    } else if let Some(real_ip) = headers.get("x-real-ip") {
        real_ip.to_str().ok().map(|s| s.trim().to_string())
    } else {
        Some(peer_addr.ip().to_string())
    };

    let now = Utc::now().timestamp();
    let expires_at = now + query.validity_secs;

    let claims = StreamTokenClaims::new(
        &query.stream_id,
        &edge.name,
        resolved_client_ip.clone(),
        expires_at,
    );

    let signature =
        auth::sign_client_stream_token(state.config.client_signing_secret.as_bytes(), &claims);

    let ip_param = resolved_client_ip
        .as_ref()
        .map(|ip| format!("&ip={}", ip))
        .unwrap_or_default();

    let playback_query = format!(
        "expires={}&sig={}{}&edge_id={}",
        expires_at, signature, ip_param, edge.name
    );

    let playback_url = if edge.public_port == 443 {
        format!(
            "https://{}/stream/{}/{}?{}",
            edge.hostname, query.stream_id, query.manifest, playback_query
        )
    } else if edge.public_port == 80 {
        format!(
            "http://{}/stream/{}/{}?{}",
            edge.hostname, query.stream_id, query.manifest, playback_query
        )
    } else {
        format!(
            "http://{}:{}/stream/{}/{}?{}",
            edge.hostname, edge.public_port, query.stream_id, query.manifest, playback_query
        )
    };

    if query.redirect {
        (
            StatusCode::FOUND,
            [(header::LOCATION, playback_url.clone())],
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            Json(PlaybackResponse {
                stream_id: query.stream_id,
                selected_edge: edge,
                playback_url,
                expires_at,
                signature,
            }),
        )
            .into_response()
    }
}
