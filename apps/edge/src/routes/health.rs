use crate::state::EdgeState;
use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use protocol::NodeStatus;
use serde_json::json;

pub async fn health_check(State(state): State<EdgeState>) -> impl IntoResponse {
    let payload = json!({
        "status": "ok",
        "service": "stream-edge",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.uptime_secs(),
        "node_status": state.get_status(),
        "active_connections": state.metrics.active_connections.load(std::sync::atomic::Ordering::Relaxed),
        "active_streams": state.metrics.active_streams.load(std::sync::atomic::Ordering::Relaxed),
    });
    (StatusCode::OK, Json(payload))
}

pub async fn readiness_check(State(state): State<EdgeState>) -> impl IntoResponse {
    let current_status = state.get_status();
    let node_id = *state.node_id.read().await;

    match current_status {
        NodeStatus::Online | NodeStatus::Degraded => {
            let payload = json!({
                "status": "ready",
                "service": "stream-edge",
                "node_id": node_id,
                "node_status": current_status,
            });
            (StatusCode::OK, Json(payload))
        }
        NodeStatus::Offline | NodeStatus::Draining => {
            let payload = json!({
                "status": "not_ready",
                "service": "stream-edge",
                "node_id": node_id,
                "node_status": current_status,
            });
            (StatusCode::SERVICE_UNAVAILABLE, Json(payload))
        }
    }
}
