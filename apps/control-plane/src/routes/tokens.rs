use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use chrono::Utc;
use protocol::StreamTokenClaims;
use serde::{Deserialize, Serialize};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct SignTokenRequest {
    pub stream_id: String,
    #[serde(default = "default_edge_wildcard")]
    pub edge_id: String,
    pub client_ip: Option<String>,
    #[serde(default = "default_validity_secs")]
    pub validity_secs: i64,
}

fn default_edge_wildcard() -> String {
    "*".to_string()
}

fn default_validity_secs() -> i64 {
    3600 // 1 hour default
}

#[derive(Debug, Serialize)]
pub struct SignTokenResponse {
    pub stream_id: String,
    pub edge_id: String,
    pub client_ip: Option<String>,
    pub expires_at: i64,
    pub signature: String,
    pub playback_query: String,
}

/// POST /api/v1/tokens/sign
pub async fn sign_token_post(
    State(state): State<AppState>,
    Json(payload): Json<SignTokenRequest>,
) -> impl IntoResponse {
    sign_token_internal(&state, payload)
}

/// GET /api/v1/tokens/sign?stream_id=...&validity_secs=...
pub async fn sign_token_get(
    State(state): State<AppState>,
    Query(payload): Query<SignTokenRequest>,
) -> impl IntoResponse {
    sign_token_internal(&state, payload)
}

fn sign_token_internal(
    state: &AppState,
    req: SignTokenRequest,
) -> (StatusCode, Json<SignTokenResponse>) {
    let now = Utc::now().timestamp();
    let expires_at = now + req.validity_secs;

    let claims = StreamTokenClaims::new(
        &req.stream_id,
        &req.edge_id,
        req.client_ip.clone(),
        expires_at,
    );

    let signature =
        auth::sign_client_stream_token(state.config.client_signing_secret.as_bytes(), &claims);

    let ip_param = req
        .client_ip
        .as_ref()
        .map(|ip| format!("&ip={}", ip))
        .unwrap_or_default();

    let edge_param = if req.edge_id != "*" && !req.edge_id.is_empty() {
        format!("&edge_id={}", req.edge_id)
    } else {
        String::new()
    };

    let playback_query = format!(
        "expires={}&sig={}{}{}",
        expires_at, signature, ip_param, edge_param
    );

    (
        StatusCode::OK,
        Json(SignTokenResponse {
            stream_id: req.stream_id,
            edge_id: req.edge_id,
            client_ip: req.client_ip,
            expires_at,
            signature,
            playback_query,
        }),
    )
}
