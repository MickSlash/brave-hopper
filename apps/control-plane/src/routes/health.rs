use crate::state::AppState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

pub async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let payload = json!({
        "status": "ok",
        "service": "stream-control",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.uptime_secs(),
    });
    (StatusCode::OK, Json(payload))
}

pub async fn readiness_check(State(_state): State<AppState>) -> impl IntoResponse {
    let payload = json!({
        "status": "ready",
        "service": "stream-control",
    });
    (StatusCode::OK, Json(payload))
}
