use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use protocol::{ClusterMetricsSnapshot, ClusterSummary, TimeSeriesPoint};

use crate::state::AppState;

/// GET /api/v1/metrics/cluster
/// Returns the latest consolidated telemetry snapshot of the streaming cluster.
pub async fn get_cluster_metrics(
    State(state): State<AppState>,
) -> (StatusCode, Json<ClusterMetricsSnapshot>) {
    let snapshot = state.telemetry.current_snapshot().await;
    (StatusCode::OK, Json(snapshot))
}

/// GET /api/v1/metrics/history
/// Returns historical rolling time-series points (bandwidth, streams, CPU, cache hit ratio).
pub async fn get_cluster_history(
    State(state): State<AppState>,
) -> (StatusCode, Json<Vec<TimeSeriesPoint>>) {
    let history = state.telemetry.history().await;
    (StatusCode::OK, Json(history))
}

/// GET /api/v1/metrics/summary
/// Returns protocol-compatible ClusterSummary for legacy monitoring integrations.
pub async fn get_cluster_summary(State(state): State<AppState>) -> impl IntoResponse {
    let snap = state.telemetry.current_snapshot().await;
    let summary = ClusterSummary {
        nodes_online: snap.nodes_online,
        nodes_degraded: snap.nodes_degraded,
        nodes_offline: snap.nodes_offline,
        nodes_draining: snap.nodes_draining,
        active_streams: snap.active_streams,
        active_connections: snap.active_connections,
        current_traffic_bps: (snap.bandwidth_out_mbps * 1_000_000.0) as u64,
        origin_traffic_bps: (snap.bandwidth_in_mbps * 1_000_000.0) as u64,
        cache_hit_ratio: snap.cache_hit_ratio,
        total_edge_bytes_served: 0,
        origin_bytes_transferred: 0,
        cache_bytes_served: 0,
        origin_bandwidth_saved_bytes: 0,
    };
    (StatusCode::OK, Json(summary))
}

use axum::response::sse::{Event, KeepAlive, Sse};
use std::time::Duration;

/// GET /api/v1/metrics/events
/// Streams real-time cluster telemetry updates using Server-Sent Events (SSE).
pub async fn cluster_metrics_sse(
    State(state): State<AppState>,
) -> Sse<impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>>> {
    let rx = state.telemetry.subscribe();
    let initial_snapshot = state.telemetry.current_snapshot().await;

    let stream = futures_util::stream::unfold(
        (rx, Some(initial_snapshot)),
        |(mut rx, mut pending)| async move {
            // First emit the current state immediately upon client connection
            if let Some(initial) = pending.take() {
                let json = serde_json::to_string(&initial).unwrap_or_default();
                let event = Event::default().event("cluster_update").data(json);
                return Some((Ok(event), (rx, None)));
            }

            // Then await subsequent periodic updates from broadcast channel
            loop {
                match rx.recv().await {
                    Ok(snapshot) => {
                        let json = serde_json::to_string(&snapshot).unwrap_or_default();
                        let event = Event::default().event("cluster_update").data(json);
                        return Some((Ok(event), (rx, None)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return None;
                    }
                }
            }
        },
    );

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}
