pub mod health;
pub mod stream;

use crate::{state::EdgeState, upstream::UpstreamClient};
use axum::{routing::get, Router};
use std::sync::Arc;

pub fn create_router(state: EdgeState, upstream: Arc<UpstreamClient>) -> Router {
    let health_router = Router::new()
        .route("/health", get(health::health_check))
        .route("/ready", get(health::readiness_check))
        .with_state(state.clone());

    let stream_state = stream::StreamRouteState {
        edge_state: state,
        upstream,
    };

    let stream_router = Router::new()
        .route(
            "/stream/{stream_id}/{*path}",
            get(stream::handle_stream_request),
        )
        .with_state(stream_state);

    health_router.merge(stream_router)
}
