use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub nodes_online: usize,
    pub nodes_degraded: usize,
    pub nodes_offline: usize,
    pub nodes_draining: usize,
    pub active_streams: usize,
    pub active_connections: usize,
    pub current_traffic_bps: u64,
    pub origin_traffic_bps: u64,
    pub cache_hit_ratio: f32,
    pub total_edge_bytes_served: u64,
    pub origin_bytes_transferred: u64,
    pub cache_bytes_served: u64,
    pub origin_bandwidth_saved_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterMetricsSnapshot {
    pub timestamp: i64,
    pub nodes_total: usize,
    pub nodes_online: usize,
    pub nodes_degraded: usize,
    pub nodes_offline: usize,
    pub nodes_draining: usize,
    pub active_streams: usize,
    pub active_connections: usize,
    pub bandwidth_in_mbps: f64,
    pub bandwidth_out_mbps: f64,
    pub cache_used_mb: u64,
    pub cache_capacity_mb: u64,
    pub cache_hit_ratio: f32,
    pub average_cpu_pct: f32,
    pub average_ram_pct: f32,
    pub origin_requests_total: u64,
    pub origin_errors_total: u64,
    pub origin_error_rate_pct: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeSeriesPoint {
    pub timestamp: i64,
    pub bandwidth_out_mbps: f64,
    pub active_streams: usize,
    pub active_connections: usize,
    pub cache_hit_ratio: f32,
    pub avg_cpu_pct: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDiscoveryRequest {
    pub stream_id: String,
    pub client_ip: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamDiscoveryResponse {
    pub stream_url: String,
    pub edge_id: String,
    pub expires_at: i64,
    pub token: String,
}
