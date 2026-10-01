use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use protocol::{HeartbeatPayload, RegisterEdgeRequest};
use subtle::ConstantTimeEq;
use tracing::{info, warn};
use uuid::Uuid;

use crate::{registry::RegistryError, state::AppState};

/// Extracts the Bearer token from the HTTP Authorization header.
fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get("Authorization")
        .and_then(|val| val.to_str().ok())
        .and_then(|auth| auth.strip_prefix("Bearer "))
}

pub async fn register_edge(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<RegisterEdgeRequest>,
) -> impl IntoResponse {
    let token = match extract_bearer_token(&headers) {
        Some(t) => t,
        None => {
            warn!("Edge registration rejected: missing Authorization Bearer header");
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Missing Authorization Bearer token" })),
            );
        }
    };

    // Constant-time check of provisioning token
    let expected_token = state.config.edge_provisioning_token.as_bytes();
    if !bool::from(expected_token.ct_eq(token.as_bytes())) {
        warn!("Edge registration rejected: invalid provisioning token");
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Invalid provisioning token" })),
        );
    }

    info!(node_name = %payload.node_name, hostname = %payload.hostname, "Processing edge registration");
    let response = state
        .registry
        .register(
            payload,
            &state.config.origin_url,
            &state.config.origin_auth_secret,
            &state.config.client_signing_secret,
        )
        .await;

    (
        StatusCode::OK,
        Json(serde_json::to_value(response).unwrap()),
    )
}

pub async fn edge_heartbeat(
    State(state): State<AppState>,
    Path(node_id): Path<Uuid>,
    headers: HeaderMap,
    Json(payload): Json<HeartbeatPayload>,
) -> impl IntoResponse {
    let token = match extract_bearer_token(&headers) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "Missing Authorization Bearer token" })),
            );
        }
    };

    match state
        .registry
        .record_heartbeat(node_id, token, payload)
        .await
    {
        Ok(resp) => (StatusCode::OK, Json(serde_json::to_value(resp).unwrap())),
        Err(RegistryError::NodeNotFound(id)) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Node not found: {}", id) })),
        ),
        Err(RegistryError::Unauthorized(_)) => (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({ "error": "Unauthorized" })),
        ),
    }
}

pub async fn list_edges(State(state): State<AppState>) -> impl IntoResponse {
    let nodes = state.registry.list_nodes().await;
    (StatusCode::OK, Json(serde_json::to_value(nodes).unwrap()))
}

pub async fn drain_edge(
    State(state): State<AppState>,
    Path(id_or_name): Path<String>,
) -> impl IntoResponse {
    let node_id = match state.registry.resolve_node_id(&id_or_name).await {
        Some(id) => id,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": format!("Edge not found: {}", id_or_name) })),
            );
        }
    };

    if state
        .registry
        .set_command(node_id, protocol::NodeCommand::Drain)
        .await
    {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "success",
                "message": format!("Edge {} marked for DRAINING", id_or_name),
                "node_id": node_id
            })),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Edge not found" })),
        )
    }
}

pub async fn undrain_edge(
    State(state): State<AppState>,
    Path(id_or_name): Path<String>,
) -> impl IntoResponse {
    let node_id = match state.registry.resolve_node_id(&id_or_name).await {
        Some(id) => id,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(serde_json::json!({ "error": format!("Edge not found: {}", id_or_name) })),
            );
        }
    };

    if state
        .registry
        .set_command(node_id, protocol::NodeCommand::Resume)
        .await
    {
        (
            StatusCode::OK,
            Json(serde_json::json!({
                "status": "success",
                "message": format!("Edge {} resumed to ONLINE", id_or_name),
                "node_id": node_id
            })),
        )
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "Edge not found" })),
        )
    }
}
