use askama::Template;
use axum::{
    extract::State,
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use protocol::{ClusterMetricsSnapshot, EdgeNodeInfo, NodeStatus};

use crate::state::AppState;

#[derive(Debug, Clone)]
pub struct EdgeTableRowView {
    pub name: String,
    pub status: String,
    pub hostname: String,
    pub public_port: u16,
    pub weight: f64,
    pub cpu_pct: f32,
    pub ram_formatted: String,
    pub active_streams: usize,
    pub active_connections: usize,
    pub bandwidth_out_mbps: f64,
    pub cache_formatted: String,
    pub origin_latency_ms: f32,
    pub origin_errors: u64,
    pub version: String,
}

impl EdgeTableRowView {
    pub fn from_node_info(info: &EdgeNodeInfo) -> Self {
        let status_str = match info.status {
            NodeStatus::Online => "ONLINE",
            NodeStatus::Degraded => "DEGRADED",
            NodeStatus::Offline => "OFFLINE",
            NodeStatus::Draining => "DRAINING",
        }
        .to_string();

        let (
            cpu_pct,
            ram_formatted,
            active_streams,
            active_connections,
            bandwidth_out_mbps,
            cache_formatted,
            origin_latency_ms,
            origin_errors,
        ) = if let Some(ref t) = info.telemetry {
            let ram = format!("{} / {} MB", t.memory_used_mb, t.memory_total_mb);
            let bw = (t.bandwidth_out_bps as f64 / 1_000_000.0 * 100.0).round() / 100.0;
            let cache = format!(
                "{} MB ({}%)",
                t.cache_used_mb,
                (t.cache_hit_ratio * 100.0).round() as u32
            );
            (
                (t.cpu_percent * 10.0).round() / 10.0,
                ram,
                t.active_streams,
                t.active_connections,
                bw,
                cache,
                (t.origin_latency_ms * 10.0).round() / 10.0,
                t.origin_errors_count,
            )
        } else {
            (
                0.0,
                "N/A".to_string(),
                0,
                0,
                0.0,
                "0 MB (0%)".to_string(),
                0.0,
                0,
            )
        };

        Self {
            name: info.name.clone(),
            status: status_str,
            hostname: info.hostname.clone(),
            public_port: info.public_port,
            weight: info.weight,
            cpu_pct,
            ram_formatted,
            active_streams,
            active_connections,
            bandwidth_out_mbps,
            cache_formatted,
            origin_latency_ms,
            origin_errors,
            version: info.version.clone(),
        }
    }
}

#[derive(Template)]
#[template(path = "dashboard.html")]
pub struct DashboardTemplate {
    pub uptime_formatted: String,
    pub version: &'static str,
    pub snapshot: ClusterMetricsSnapshot,
    pub edges: Vec<EdgeTableRowView>,
}

#[derive(Template)]
#[template(path = "cards.html")]
pub struct CardsTemplate {
    pub snapshot: ClusterMetricsSnapshot,
}

#[derive(Template)]
#[template(path = "edge_table.html")]
pub struct EdgeTableTemplate {
    pub edges: Vec<EdgeTableRowView>,
}

fn format_duration(secs: u64) -> String {
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let minutes = (secs % 3600) / 60;
    let seconds = secs % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else {
        format!("{}m {}s", minutes, seconds)
    }
}

/// GET / and GET /dashboard
pub async fn dashboard_home(State(state): State<AppState>) -> Response {
    let nodes = state.registry.list_nodes().await;
    let snapshot = state.telemetry.current_snapshot().await;
    let edges: Vec<EdgeTableRowView> = nodes.iter().map(EdgeTableRowView::from_node_info).collect();

    let template = DashboardTemplate {
        uptime_formatted: format_duration(state.uptime_secs()),
        version: env!("CARGO_PKG_VERSION"),
        snapshot,
        edges,
    };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "Failed to render dashboard template");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error rendering dashboard",
            )
                .into_response()
        }
    }
}

/// GET /dashboard/cards (HTMX partial swap)
pub async fn dashboard_cards(State(state): State<AppState>) -> Response {
    let snapshot = state.telemetry.current_snapshot().await;
    let template = CardsTemplate { snapshot };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "Failed to render cards partial");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error rendering cards",
            )
                .into_response()
        }
    }
}

/// GET /dashboard/edges (HTMX partial swap)
pub async fn dashboard_edges(State(state): State<AppState>) -> Response {
    let nodes = state.registry.list_nodes().await;
    let edges: Vec<EdgeTableRowView> = nodes.iter().map(EdgeTableRowView::from_node_info).collect();
    let template = EdgeTableTemplate { edges };

    match template.render() {
        Ok(html) => Html(html).into_response(),
        Err(err) => {
            tracing::error!(error = %err, "Failed to render edge table partial");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Server Error rendering edge table",
            )
                .into_response()
        }
    }
}
