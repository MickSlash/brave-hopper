use chrono::Utc;
use protocol::{
    EdgeNodeInfo, HeartbeatPayload, HeartbeatResponse, NodeCommand, NodeStatus,
    RegisterEdgeRequest, RegisterEdgeResponse,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("Node not found: {0}")]
    NodeNotFound(Uuid),
    #[error("Invalid authentication token for node {0}")]
    Unauthorized(Uuid),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct EdgeNode {
    pub id: Uuid,
    pub name: String,
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
    pub weight: f64,
    pub monthly_bandwidth_limit_gb: Option<u64>,
    pub auth_secret: String,
    pub status: NodeStatus,
    pub pending_command: NodeCommand,
    pub last_heartbeat: Option<Instant>,
    pub last_heartbeat_timestamp: i64,
    pub latest_telemetry: Option<HeartbeatPayload>,
}

#[derive(Default)]
pub struct EdgeRegistry {
    nodes: RwLock<HashMap<Uuid, EdgeNode>>,
    name_to_id: RwLock<HashMap<String, Uuid>>,
}

impl EdgeRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Registers a new edge node or updates an existing one if the node_name already exists.
    pub async fn register(
        &self,
        req: RegisterEdgeRequest,
        origin_base_url: &str,
        origin_auth_secret: &str,
        client_signing_secret: &str,
    ) -> RegisterEdgeResponse {
        let mut name_map = self.name_to_id.write().await;
        let mut nodes = self.nodes.write().await;

        let node_id = if let Some(&existing_id) = name_map.get(&req.node_name) {
            info!(
                node_id = %existing_id,
                node_name = %req.node_name,
                "Updating existing edge registration"
            );
            existing_id
        } else {
            let new_id = Uuid::new_v4();
            name_map.insert(req.node_name.clone(), new_id);
            info!(
                node_id = %new_id,
                node_name = %req.node_name,
                "New edge registered successfully"
            );
            new_id
        };

        // Generate a random 32-byte secret for this specific edge
        let auth_secret = hex::encode(Uuid::new_v4().as_bytes());

        let node = EdgeNode {
            id: node_id,
            name: req.node_name,
            hostname: req.hostname,
            public_port: req.public_port,
            internal_port: req.internal_port,
            version: req.version,
            cpu_count: req.cpu_count,
            ram_total_mb: req.ram_total_mb,
            cache_capacity_gb: req.cache_capacity_gb,
            max_connections: req.max_connections,
            max_streams: req.max_streams,
            max_bandwidth_mbps: req.max_bandwidth_mbps,
            weight: req.weight,
            monthly_bandwidth_limit_gb: req.monthly_bandwidth_limit_gb,
            auth_secret: auth_secret.clone(),
            status: NodeStatus::Online,
            pending_command: NodeCommand::None,
            last_heartbeat: Some(Instant::now()),
            last_heartbeat_timestamp: Utc::now().timestamp(),
            latest_telemetry: None,
        };

        nodes.insert(node_id, node);

        RegisterEdgeResponse {
            node_id,
            heartbeat_interval_secs: 15,
            auth_secret,
            origin_base_url: origin_base_url.to_string(),
            origin_auth_secret: origin_auth_secret.to_string(),
            client_signing_secret: client_signing_secret.to_string(),
            assigned_weight: req.weight,
            status: NodeStatus::Online,
        }
    }

    /// Records incoming heartbeat from an edge node after verifying its secret.
    pub async fn record_heartbeat(
        &self,
        node_id: Uuid,
        auth_token: &str,
        payload: HeartbeatPayload,
    ) -> Result<HeartbeatResponse, RegistryError> {
        let mut nodes = self.nodes.write().await;
        let node = nodes
            .get_mut(&node_id)
            .ok_or(RegistryError::NodeNotFound(node_id))?;

        if !bool::from(node.auth_secret.as_bytes().ct_eq(auth_token.as_bytes())) {
            warn!(node_id = %node_id, "Heartbeat authentication token rejected");
            return Err(RegistryError::Unauthorized(node_id));
        }

        node.last_heartbeat = Some(Instant::now());
        node.last_heartbeat_timestamp = payload.timestamp;
        node.latest_telemetry = Some(payload);

        // Node command & status handling
        if node.pending_command == NodeCommand::Drain {
            node.status = NodeStatus::Draining;
        } else if node.pending_command == NodeCommand::Resume || node.status != NodeStatus::Draining
        {
            node.status = NodeStatus::Online;
        }

        let command = node.pending_command;
        if command == NodeCommand::Resume {
            node.pending_command = NodeCommand::None;
        }

        Ok(HeartbeatResponse {
            acknowledged: true,
            next_heartbeat_secs: 15,
            command,
        })
    }

    /// Scans nodes and adjusts status based on elapsed time since last heartbeat.
    /// Returns count of degraded and offline nodes.
    pub async fn reap_stale_nodes(&self, timeout: Duration) -> (usize, usize) {
        let mut nodes = self.nodes.write().await;
        let mut degraded_count = 0;
        let mut offline_count = 0;
        let now = Instant::now();

        for node in nodes.values_mut() {
            if node.status == NodeStatus::Draining {
                continue;
            }

            if let Some(last_seen) = node.last_heartbeat {
                let elapsed = now.duration_since(last_seen);
                if elapsed > timeout * 2 {
                    if node.status != NodeStatus::Offline {
                        warn!(
                            node_id = %node.id,
                            name = %node.name,
                            elapsed_secs = elapsed.as_secs(),
                            "Node transitioned to OFFLINE due to missed heartbeats"
                        );
                        node.status = NodeStatus::Offline;
                    }
                    offline_count += 1;
                } else if elapsed > timeout {
                    if node.status != NodeStatus::Degraded {
                        warn!(
                            node_id = %node.id,
                            name = %node.name,
                            elapsed_secs = elapsed.as_secs(),
                            "Node transitioned to DEGRADED due to delayed heartbeat"
                        );
                        node.status = NodeStatus::Degraded;
                    }
                    degraded_count += 1;
                }
            } else {
                node.status = NodeStatus::Offline;
                offline_count += 1;
            }
        }

        (degraded_count, offline_count)
    }

    /// Lists all registered edge nodes formatted for cluster reporting.
    pub async fn list_nodes(&self) -> Vec<EdgeNodeInfo> {
        let nodes = self.nodes.read().await;
        nodes
            .values()
            .map(|n| EdgeNodeInfo {
                node_id: n.id,
                name: n.name.clone(),
                hostname: n.hostname.clone(),
                public_port: n.public_port,
                status: n.status,
                weight: n.weight,
                version: n.version.clone(),
                last_heartbeat_timestamp: n.last_heartbeat_timestamp,
                telemetry: n.latest_telemetry.clone(),
            })
            .collect()
    }

    /// Retrieves an edge node info by ID.
    #[allow(dead_code)]
    pub async fn get_node(&self, id: Uuid) -> Option<EdgeNodeInfo> {
        let nodes = self.nodes.read().await;
        nodes.get(&id).map(|n| EdgeNodeInfo {
            node_id: n.id,
            name: n.name.clone(),
            hostname: n.hostname.clone(),
            public_port: n.public_port,
            status: n.status,
            weight: n.weight,
            version: n.version.clone(),
            last_heartbeat_timestamp: n.last_heartbeat_timestamp,
            telemetry: n.latest_telemetry.clone(),
        })
    }

    /// Resolves an edge node ID from either UUID or node name.
    pub async fn resolve_node_id(&self, id_or_name: &str) -> Option<Uuid> {
        if let Ok(parsed) = Uuid::parse_str(id_or_name) {
            let nodes = self.nodes.read().await;
            if nodes.contains_key(&parsed) {
                return Some(parsed);
            }
        }
        let name_map = self.name_to_id.read().await;
        name_map.get(id_or_name).copied()
    }

    /// Sets a pending command for a node (e.g. Drain or resume Online).
    pub async fn set_command(&self, id: Uuid, cmd: NodeCommand) -> bool {
        let mut nodes = self.nodes.write().await;
        if let Some(node) = nodes.get_mut(&id) {
            node.pending_command = cmd;
            match cmd {
                NodeCommand::Drain => {
                    node.status = NodeStatus::Draining;
                    info!(node_id = %id, name = %node.name, "Edge node marked for DRAINING");
                }
                NodeCommand::Resume | NodeCommand::None => {
                    node.status = NodeStatus::Online;
                    info!(node_id = %id, name = %node.name, "Edge node resumed to ONLINE");
                }
                _ => {}
            }
            true
        } else {
            false
        }
    }

    /// Evaluates healthy, online edge nodes and selects the least loaded node
    /// based on active streams, bandwidth saturation, CPU, memory, and weight.
    pub async fn schedule_edge(&self, criteria: &SchedulingCriteria) -> Option<ScheduledEdge> {
        let nodes = self.nodes.read().await;
        let now = Instant::now();
        let max_stale_duration = Duration::from_secs(45);

        let mut scored_candidates: Vec<(ScheduledEdge, f64)> = Vec::new();

        for node in nodes.values() {
            // Only select nodes that are explicitly Online
            if node.status != NodeStatus::Online {
                continue;
            }

            // Exclude nodes with stale heartbeats
            if let Some(last_seen) = node.last_heartbeat {
                if now.duration_since(last_seen) > max_stale_duration {
                    continue;
                }
            } else {
                continue;
            }

            let (cpu_pct, mem_used, mem_total, streams, bw_bps) =
                if let Some(ref t) = node.latest_telemetry {
                    (
                        t.cpu_percent,
                        t.memory_used_mb,
                        t.memory_total_mb,
                        t.active_streams,
                        t.bandwidth_out_bps,
                    )
                } else {
                    (0.0, 0, node.ram_total_mb, 0, 0)
                };

            let s_cpu = (cpu_pct / 100.0).clamp(0.0, 1.0) as f64;
            let s_ram = if mem_total > 0 {
                (mem_used as f64 / mem_total as f64).clamp(0.0, 1.0)
            } else {
                0.0
            };
            let s_stream = (streams as f64 / (node.max_streams.max(1) as f64)).clamp(0.0, 1.0);
            let bw_mbps = (bw_bps as f64) / 1_000_000.0;
            let s_bw = (bw_mbps / (node.max_bandwidth_mbps.max(1) as f64)).clamp(0.0, 1.0);

            // Composite load calculation (weighted by critical bottleneck: streams & bandwidth)
            let raw_load = 0.35 * s_stream + 0.25 * s_bw + 0.25 * s_cpu + 0.15 * s_ram;
            let mut weighted_score = (raw_load + 0.01) / node.weight.max(0.1);

            // Region and edge affinity weighting
            if let Some(ref pref_edge) = criteria.preferred_edge {
                if node.name.eq_ignore_ascii_case(pref_edge)
                    || node.id.to_string().eq_ignore_ascii_case(pref_edge)
                {
                    weighted_score *= 0.01; // Explicit target priority
                }
            } else if let Some(ref pref_region) = criteria.preferred_region {
                if node
                    .name
                    .to_lowercase()
                    .contains(&pref_region.to_lowercase())
                {
                    weighted_score *= 0.7; // Regional affinity hint
                }
            }

            scored_candidates.push((
                ScheduledEdge {
                    node_id: node.id,
                    name: node.name.clone(),
                    hostname: node.hostname.clone(),
                    public_port: node.public_port,
                    load_score: (raw_load * 1000.0).round() / 1000.0,
                },
                weighted_score,
            ));
        }

        // Return candidate with the lowest weighted score (least loaded)
        scored_candidates
            .into_iter()
            .min_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
            .map(|(edge, _)| edge)
    }
}

#[derive(Debug, Clone, Default)]
pub struct SchedulingCriteria {
    pub preferred_edge: Option<String>,
    pub preferred_region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledEdge {
    pub node_id: Uuid,
    pub name: String,
    pub hostname: String,
    pub public_port: u16,
    pub load_score: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_registration_and_heartbeat() {
        let registry = EdgeRegistry::new();
        let req = RegisterEdgeRequest {
            node_name: "edge-de-01".to_string(),
            hostname: "edge01.example.com".to_string(),
            public_port: 8443,
            internal_port: 8444,
            version: "0.1.0".to_string(),
            cpu_count: 4,
            ram_total_mb: 4096,
            cache_capacity_gb: 100,
            max_connections: 5000,
            max_streams: 500,
            max_bandwidth_mbps: 1000,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };

        let resp = registry
            .register(
                req,
                "http://origin:9000",
                "origin-secret",
                "client-signing-secret",
            )
            .await;
        assert_eq!(resp.status, NodeStatus::Online);

        let heartbeat_payload = HeartbeatPayload {
            timestamp: 1700000000,
            uptime_secs: 100,
            status: NodeStatus::Online,
            cpu_percent: 15.0,
            memory_used_mb: 200,
            memory_total_mb: 4096,
            active_connections: 10,
            active_streams: 5,
            bandwidth_in_bps: 1000000,
            bandwidth_out_bps: 5000000,
            cache_used_mb: 1000,
            cache_capacity_mb: 100000,
            cache_hit_ratio: 0.85,
            origin_latency_ms: 12.0,
            origin_requests_count: 50,
            origin_errors_count: 0,
            monthly_bandwidth_used_bytes: 50000000,
        };

        let hb_res = registry
            .record_heartbeat(resp.node_id, &resp.auth_secret, heartbeat_payload)
            .await;
        assert!(hb_res.is_ok());

        // Invalid token test
        let bad_token_res = registry
            .record_heartbeat(
                resp.node_id,
                "wrong-secret",
                HeartbeatPayload {
                    timestamp: 1700000010,
                    uptime_secs: 110,
                    status: NodeStatus::Online,
                    cpu_percent: 15.0,
                    memory_used_mb: 200,
                    memory_total_mb: 4096,
                    active_connections: 10,
                    active_streams: 5,
                    bandwidth_in_bps: 1000000,
                    bandwidth_out_bps: 5000000,
                    cache_used_mb: 1000,
                    cache_capacity_mb: 100000,
                    cache_hit_ratio: 0.85,
                    origin_latency_ms: 12.0,
                    origin_requests_count: 50,
                    origin_errors_count: 0,
                    monthly_bandwidth_used_bytes: 50000000,
                },
            )
            .await;
        assert!(bad_token_res.is_err());
    }

    #[tokio::test]
    async fn test_stale_node_reaping() {
        let registry = EdgeRegistry::new();
        let req = RegisterEdgeRequest {
            node_name: "edge-timeout".to_string(),
            hostname: "edge.timeout.com".to_string(),
            public_port: 8443,
            internal_port: 8444,
            version: "0.1.0".to_string(),
            cpu_count: 2,
            ram_total_mb: 2048,
            cache_capacity_gb: 50,
            max_connections: 1000,
            max_streams: 100,
            max_bandwidth_mbps: 500,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };

        let resp = registry
            .register(
                req,
                "http://origin:9000",
                "origin-secret",
                "client-signing-secret",
            )
            .await;

        // Manually push back last_heartbeat to simulate expiration
        {
            let mut nodes = registry.nodes.write().await;
            let node = nodes.get_mut(&resp.node_id).unwrap();
            node.last_heartbeat = Some(Instant::now() - Duration::from_secs(40));
        }

        let (degraded, offline) = registry.reap_stale_nodes(Duration::from_secs(15)).await;
        assert_eq!(offline, 1);
        assert_eq!(degraded, 0);

        let node_info = registry.get_node(resp.node_id).await.unwrap();
        assert_eq!(node_info.status, NodeStatus::Offline);
    }

    #[tokio::test]
    async fn test_schedule_edge_selection() {
        let registry = EdgeRegistry::new();

        // Register Node 1: Heavy load (90% streams)
        let req1 = RegisterEdgeRequest {
            node_name: "edge-de-heavy".to_string(),
            hostname: "de-heavy.cdn.net".to_string(),
            public_port: 8081,
            internal_port: 8082,
            version: "0.1.0".to_string(),
            cpu_count: 4,
            ram_total_mb: 4096,
            cache_capacity_gb: 100,
            max_connections: 1000,
            max_streams: 100,
            max_bandwidth_mbps: 1000,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };
        let resp1 = registry
            .register(req1, "http://origin", "sec", "client_sec")
            .await;

        let hb1 = HeartbeatPayload {
            timestamp: Utc::now().timestamp(),
            uptime_secs: 500,
            status: NodeStatus::Online,
            cpu_percent: 75.0,
            memory_used_mb: 3000,
            memory_total_mb: 4096,
            active_connections: 900,
            active_streams: 90, // 90% saturated
            bandwidth_in_bps: 10_000_000,
            bandwidth_out_bps: 800_000_000,
            cache_used_mb: 50000,
            cache_capacity_mb: 100000,
            cache_hit_ratio: 0.85,
            origin_latency_ms: 12.0,
            origin_requests_count: 1000,
            origin_errors_count: 0,
            monthly_bandwidth_used_bytes: 10_000_000_000,
        };
        registry
            .record_heartbeat(resp1.node_id, &resp1.auth_secret, hb1)
            .await
            .unwrap();

        // Register Node 2: Light load (10% streams)
        let req2 = RegisterEdgeRequest {
            node_name: "edge-fr-light".to_string(),
            hostname: "fr-light.cdn.net".to_string(),
            public_port: 8081,
            internal_port: 8082,
            version: "0.1.0".to_string(),
            cpu_count: 4,
            ram_total_mb: 4096,
            cache_capacity_gb: 100,
            max_connections: 1000,
            max_streams: 100,
            max_bandwidth_mbps: 1000,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };
        let resp2 = registry
            .register(req2, "http://origin", "sec", "client_sec")
            .await;

        let hb2 = HeartbeatPayload {
            timestamp: Utc::now().timestamp(),
            uptime_secs: 500,
            status: NodeStatus::Online,
            cpu_percent: 10.0,
            memory_used_mb: 500,
            memory_total_mb: 4096,
            active_connections: 100,
            active_streams: 10, // only 10% saturated
            bandwidth_in_bps: 1_000_000,
            bandwidth_out_bps: 50_000_000,
            cache_used_mb: 10000,
            cache_capacity_mb: 100000,
            cache_hit_ratio: 0.95,
            origin_latency_ms: 8.0,
            origin_requests_count: 500,
            origin_errors_count: 0,
            monthly_bandwidth_used_bytes: 1_000_000_000,
        };
        registry
            .record_heartbeat(resp2.node_id, &resp2.auth_secret, hb2)
            .await
            .unwrap();

        // General scheduling: least loaded node (edge-fr-light) should be selected
        let criteria = SchedulingCriteria::default();
        let scheduled = registry
            .schedule_edge(&criteria)
            .await
            .expect("Must find a node");
        assert_eq!(scheduled.name, "edge-fr-light");

        // Targeted scheduling: explicit request for "edge-de-heavy" overrides general least-loaded
        let targeted_criteria = SchedulingCriteria {
            preferred_edge: Some("edge-de-heavy".to_string()),
            preferred_region: None,
        };
        let scheduled_targeted = registry
            .schedule_edge(&targeted_criteria)
            .await
            .expect("Must find node");
        assert_eq!(scheduled_targeted.name, "edge-de-heavy");
    }

    #[tokio::test]
    async fn test_drain_and_resume_node() {
        let registry = EdgeRegistry::new();

        let req1 = RegisterEdgeRequest {
            node_name: "edge-drain-01".to_string(),
            hostname: "edge1.example.com".to_string(),
            public_port: 8081,
            internal_port: 8082,
            version: "0.1.0".to_string(),
            cpu_count: 4,
            ram_total_mb: 4096,
            cache_capacity_gb: 100,
            max_connections: 1000,
            max_streams: 100,
            max_bandwidth_mbps: 1000,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };
        let resp1 = registry
            .register(req1, "http://origin", "sec", "client_sec")
            .await;

        let req2 = RegisterEdgeRequest {
            node_name: "edge-drain-02".to_string(),
            hostname: "edge2.example.com".to_string(),
            public_port: 8081,
            internal_port: 8082,
            version: "0.1.0".to_string(),
            cpu_count: 4,
            ram_total_mb: 4096,
            cache_capacity_gb: 100,
            max_connections: 1000,
            max_streams: 100,
            max_bandwidth_mbps: 1000,
            weight: 1.0,
            monthly_bandwidth_limit_gb: None,
        };
        let resp2 = registry
            .register(req2, "http://origin", "sec", "client_sec")
            .await;

        let hb = HeartbeatPayload {
            timestamp: Utc::now().timestamp(),
            uptime_secs: 100,
            status: NodeStatus::Online,
            cpu_percent: 10.0,
            memory_used_mb: 500,
            memory_total_mb: 4096,
            active_connections: 10,
            active_streams: 2,
            bandwidth_in_bps: 1_000_000,
            bandwidth_out_bps: 5_000_000,
            cache_used_mb: 1000,
            cache_capacity_mb: 100000,
            cache_hit_ratio: 0.9,
            origin_latency_ms: 10.0,
            origin_requests_count: 100,
            origin_errors_count: 0,
            monthly_bandwidth_used_bytes: 1_000_000,
        };

        registry
            .record_heartbeat(resp1.node_id, &resp1.auth_secret, hb.clone())
            .await
            .unwrap();
        registry
            .record_heartbeat(resp2.node_id, &resp2.auth_secret, hb.clone())
            .await
            .unwrap();

        // 1. Drain edge-drain-01
        assert!(
            registry
                .set_command(resp1.node_id, NodeCommand::Drain)
                .await
        );
        let node1_info = registry.get_node(resp1.node_id).await.unwrap();
        assert_eq!(node1_info.status, NodeStatus::Draining);

        // Heartbeat retains Draining status and returns Drain command
        let hb_res = registry
            .record_heartbeat(resp1.node_id, &resp1.auth_secret, hb.clone())
            .await
            .unwrap();
        assert_eq!(hb_res.command, NodeCommand::Drain);

        // Scheduler MUST NOT select edge-drain-01, only edge-drain-02
        let scheduled = registry
            .schedule_edge(&SchedulingCriteria::default())
            .await
            .expect("Must schedule online node");
        assert_eq!(scheduled.name, "edge-drain-02");

        // Targeted request to edge-drain-01 must also fail (it's draining)
        let targeted_drain = registry
            .schedule_edge(&SchedulingCriteria {
                preferred_edge: Some("edge-drain-01".to_string()),
                preferred_region: None,
            })
            .await;
        // Since preferred_edge is draining, scheduler skips it and picks edge-drain-02
        assert_eq!(targeted_drain.unwrap().name, "edge-drain-02");

        // 2. Resume edge-drain-01
        assert!(
            registry
                .set_command(resp1.node_id, NodeCommand::Resume)
                .await
        );
        let node1_info = registry.get_node(resp1.node_id).await.unwrap();
        assert_eq!(node1_info.status, NodeStatus::Online);

        // Heartbeat sends Resume command and pending_command clears
        let hb_res_resume = registry
            .record_heartbeat(resp1.node_id, &resp1.auth_secret, hb.clone())
            .await
            .unwrap();
        assert_eq!(hb_res_resume.command, NodeCommand::Resume);

        // Targeted request to edge-drain-01 now succeeds
        let scheduled_edge1 = registry
            .schedule_edge(&SchedulingCriteria {
                preferred_edge: Some("edge-drain-01".to_string()),
                preferred_region: None,
            })
            .await
            .unwrap();
        assert_eq!(scheduled_edge1.name, "edge-drain-01");
    }
}
