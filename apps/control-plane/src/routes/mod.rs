pub mod dashboard;
pub mod health;
pub mod internal;
pub mod metrics;
pub mod playback;
pub mod tokens;

use crate::state::AppState;
use axum::{
    routing::{get, post},
    Router,
};

pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/", get(dashboard::dashboard_home))
        .route("/dashboard", get(dashboard::dashboard_home))
        .route("/dashboard/cards", get(dashboard::dashboard_cards))
        .route("/dashboard/edges", get(dashboard::dashboard_edges))
        .route("/health", get(health::health_check))
        .route("/ready", get(health::readiness_check))
        .route("/internal/edges/register", post(internal::register_edge))
        .route(
            "/internal/edges/{id}/heartbeat",
            post(internal::edge_heartbeat),
        )
        .route("/internal/edges", get(internal::list_edges))
        .route("/api/v1/edges/{id}/drain", post(internal::drain_edge))
        .route("/api/v1/edges/{id}/undrain", post(internal::undrain_edge))
        .route("/api/v1/tokens/sign", post(tokens::sign_token_post))
        .route("/api/v1/tokens/sign", get(tokens::sign_token_get))
        .route("/api/v1/streams/play", get(playback::handle_playback))
        .route("/api/v1/metrics/cluster", get(metrics::get_cluster_metrics))
        .route("/api/v1/metrics/history", get(metrics::get_cluster_history))
        .route("/api/v1/metrics/summary", get(metrics::get_cluster_summary))
        .route("/api/v1/metrics/events", get(metrics::cluster_metrics_sse))
        .with_state(state)
}
