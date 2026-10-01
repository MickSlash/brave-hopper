use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum NodeStatus {
    Online,
    Degraded,
    Offline,
    Draining,
}

impl Default for NodeStatus {
    fn default() -> Self {
        Self::Online
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NodeCommand {
    None,
    Drain,
    Resume,
    ReloadConfig,
    Terminate,
}

impl Default for NodeCommand {
    fn default() -> Self {
        Self::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterEdgeRequest {
    pub node_name: String,
    pub hostname: String,
    pub public_port: u16,
    pub internal_port: u16,
    pub version: String,
    pub cpu_count: u32,
    pub ram_total_mb: u64,
    pub cache_capacity_gb: u64,
    pub max_connections: usize,
    pub max_streams: usize,
    pub max_bandwidth_mbps: u64,
    #[serde(default = "default_weight")]
    pub weight: f64,
    pub monthly_bandwidth_limit_gb: Option<u64>,
}

fn default_weight() -> f64 {
    1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterEdgeResponse {
    pub node_id: Uuid,
    pub heartbeat_interval_secs: u64,
    pub auth_secret: String,
    pub origin_base_url: String,
    pub origin_auth_secret: String,
    #[serde(default)]
    pub client_signing_secret: String,
    pub assigned_weight: f64,
    pub status: NodeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatPayload {
    pub timestamp: i64,
    pub uptime_secs: u64,
    pub status: NodeStatus,
    pub cpu_percent: f32,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub active_connections: usize,
    pub active_streams: usize,
    pub bandwidth_in_bps: u64,
    pub bandwidth_out_bps: u64,
    pub cache_used_mb: u64,
    pub cache_capacity_mb: u64,
    pub cache_hit_ratio: f32,
    pub origin_latency_ms: f32,
    pub origin_requests_count: u64,
    pub origin_errors_count: u64,
    pub monthly_bandwidth_used_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatResponse {
    pub acknowledged: bool,
    pub next_heartbeat_secs: u64,
    pub command: NodeCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeNodeInfo {
    pub node_id: Uuid,
    pub name: String,
    pub hostname: String,
    pub public_port: u16,
    pub status: NodeStatus,
    pub weight: f64,
    pub version: String,
    pub last_heartbeat_timestamp: i64,
    pub telemetry: Option<HeartbeatPayload>,
}
