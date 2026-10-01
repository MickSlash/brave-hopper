use chrono::Utc;
use protocol::{ClusterMetricsSnapshot, EdgeNodeInfo, NodeStatus, TimeSeriesPoint};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;

/// In-memory rolling telemetry aggregator for cluster-wide metrics.
/// Zero database dependency: maintains live aggregates and ring-buffered time-series in RAM.
pub struct TelemetryAggregator {
    max_history_points: usize,
    history: RwLock<VecDeque<TimeSeriesPoint>>,
    latest_snapshot: RwLock<ClusterMetricsSnapshot>,
    events_tx: tokio::sync::broadcast::Sender<ClusterMetricsSnapshot>,
}

impl TelemetryAggregator {
    pub fn new(max_history_points: usize) -> Arc<Self> {
        let (events_tx, _) = tokio::sync::broadcast::channel(32);
        Arc::new(Self {
            max_history_points,
            history: RwLock::new(VecDeque::with_capacity(max_history_points)),
            latest_snapshot: RwLock::new(ClusterMetricsSnapshot::default()),
            events_tx,
        })
    }

    /// Evaluates current cluster state from active edge nodes, updates historical rolling
    /// ring-buffer, and stores the latest snapshot.
    pub async fn update(&self, nodes: &[EdgeNodeInfo]) -> ClusterMetricsSnapshot {
        let now = Utc::now().timestamp();

        let nodes_total = nodes.len();
        let mut nodes_online = 0;
        let mut nodes_degraded = 0;
        let mut nodes_offline = 0;
        let mut nodes_draining = 0;

        let mut active_streams = 0;
        let mut active_connections = 0;
        let mut total_bw_in_bps: u64 = 0;
        let mut total_bw_out_bps: u64 = 0;
        let mut total_cache_used_mb: u64 = 0;
        let mut total_cache_capacity_mb: u64 = 0;

        let mut origin_requests_total: u64 = 0;
        let mut origin_errors_total: u64 = 0;

        let mut cpu_sum: f32 = 0.0;
        let mut ram_pct_sum: f32 = 0.0;
        let mut active_node_count = 0;

        let mut weighted_hit_ratio_sum: f64 = 0.0;
        let mut weight_sum: f64 = 0.0;

        for node in nodes {
            match node.status {
                NodeStatus::Online => nodes_online += 1,
                NodeStatus::Degraded => nodes_degraded += 1,
                NodeStatus::Offline => nodes_offline += 1,
                NodeStatus::Draining => nodes_draining += 1,
            }

            if let Some(ref t) = node.telemetry {
                if node.status == NodeStatus::Online || node.status == NodeStatus::Degraded {
                    active_streams += t.active_streams;
                    active_connections += t.active_connections;
                    total_bw_in_bps += t.bandwidth_in_bps;
                    total_bw_out_bps += t.bandwidth_out_bps;
                    total_cache_used_mb += t.cache_used_mb;
                    total_cache_capacity_mb += t.cache_capacity_mb;

                    origin_requests_total += t.origin_requests_count;
                    origin_errors_total += t.origin_errors_count;

                    cpu_sum += t.cpu_percent;
                    if t.memory_total_mb > 0 {
                        ram_pct_sum += (t.memory_used_mb as f32 / t.memory_total_mb as f32) * 100.0;
                    }
                    active_node_count += 1;

                    // Weight cache hit ratio by traffic / activity (or node weight)
                    let node_weight = node.weight.max(0.1);
                    weighted_hit_ratio_sum += (t.cache_hit_ratio as f64) * node_weight;
                    weight_sum += node_weight;
                }
            }
        }

        let bandwidth_in_mbps = (total_bw_in_bps as f64 / 1_000_000.0 * 1000.0).round() / 1000.0;
        let bandwidth_out_mbps = (total_bw_out_bps as f64 / 1_000_000.0 * 1000.0).round() / 1000.0;

        let average_cpu_pct = if active_node_count > 0 {
            (cpu_sum / active_node_count as f32 * 10.0).round() / 10.0
        } else {
            0.0
        };

        let average_ram_pct = if active_node_count > 0 {
            (ram_pct_sum / active_node_count as f32 * 10.0).round() / 10.0
        } else {
            0.0
        };

        let cache_hit_ratio = if weight_sum > 0.0 {
            ((weighted_hit_ratio_sum / weight_sum) as f32 * 100.0).round() / 100.0
        } else {
            0.0
        };

        let origin_error_rate_pct = if origin_requests_total > 0 {
            ((origin_errors_total as f32 / origin_requests_total as f32) * 100.0 * 100.0).round()
                / 100.0
        } else {
            0.0
        };

        let snapshot = ClusterMetricsSnapshot {
            timestamp: now,
            nodes_total,
            nodes_online,
            nodes_degraded,
            nodes_offline,
            nodes_draining,
            active_streams,
            active_connections,
            bandwidth_in_mbps,
            bandwidth_out_mbps,
            cache_used_mb: total_cache_used_mb,
            cache_capacity_mb: total_cache_capacity_mb,
            cache_hit_ratio,
            average_cpu_pct,
            average_ram_pct,
            origin_requests_total,
            origin_errors_total,
            origin_error_rate_pct,
        };

        // Record time-series historical data point
        let point = TimeSeriesPoint {
            timestamp: now,
            bandwidth_out_mbps,
            active_streams,
            active_connections,
            cache_hit_ratio,
            avg_cpu_pct: average_cpu_pct,
        };

        {
            let mut hist = self.history.write().await;
            if hist.len() >= self.max_history_points {
                hist.pop_front();
            }
            hist.push_back(point);
        }

        {
            let mut snap = self.latest_snapshot.write().await;
            *snap = snapshot.clone();
        }

        // Broadcast snapshot update to live SSE subscribers
        let _ = self.events_tx.send(snapshot.clone());

        snapshot
    }

    /// Subscribes to real-time cluster telemetry updates via Tokio broadcast channel.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<ClusterMetricsSnapshot> {
        self.events_tx.subscribe()
    }

    /// Retrieves the most recent cluster snapshot.
    pub async fn current_snapshot(&self) -> ClusterMetricsSnapshot {
        self.latest_snapshot.read().await.clone()
    }

    /// Retrieves historical rolling telemetry points.
    pub async fn history(&self) -> Vec<TimeSeriesPoint> {
        self.history.read().await.iter().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::HeartbeatPayload;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_telemetry_aggregator_rollup() {
        let aggregator = TelemetryAggregator::new(5);

        let hb1 = HeartbeatPayload {
            timestamp: 100,
            uptime_secs: 200,
            status: NodeStatus::Online,
            cpu_percent: 20.0,
            memory_used_mb: 2048,
            memory_total_mb: 4096, // 50% RAM
            active_connections: 50,
            active_streams: 30,
            bandwidth_in_bps: 10_000_000,  // 10 Mbps
            bandwidth_out_bps: 50_000_000, // 50 Mbps
            cache_used_mb: 1000,
            cache_capacity_mb: 5000,
            cache_hit_ratio: 0.80,
            origin_latency_ms: 10.0,
            origin_requests_count: 100,
            origin_errors_count: 2,
            monthly_bandwidth_used_bytes: 1_000_000,
        };

        let node1 = EdgeNodeInfo {
            node_id: Uuid::new_v4(),
            name: "edge-01".to_string(),
            hostname: "127.0.0.1".to_string(),
            public_port: 8081,
            status: NodeStatus::Online,
            weight: 1.0,
            version: "0.1.0".to_string(),
            last_heartbeat_timestamp: 100,
            telemetry: Some(hb1),
        };

        let hb2 = HeartbeatPayload {
            timestamp: 100,
            uptime_secs: 200,
            status: NodeStatus::Online,
            cpu_percent: 40.0,
            memory_used_mb: 1024,
            memory_total_mb: 2048, // 50% RAM
            active_connections: 150,
            active_streams: 70,
            bandwidth_in_bps: 20_000_000,   // 20 Mbps
            bandwidth_out_bps: 150_000_000, // 150 Mbps
            cache_used_mb: 2000,
            cache_capacity_mb: 5000,
            cache_hit_ratio: 0.90,
            origin_latency_ms: 8.0,
            origin_requests_count: 200,
            origin_errors_count: 1,
            monthly_bandwidth_used_bytes: 2_000_000,
        };

        let node2 = EdgeNodeInfo {
            node_id: Uuid::new_v4(),
            name: "edge-02".to_string(),
            hostname: "127.0.0.1".to_string(),
            public_port: 8082,
            status: NodeStatus::Online,
            weight: 1.0,
            version: "0.1.0".to_string(),
            last_heartbeat_timestamp: 100,
            telemetry: Some(hb2),
        };

        let snapshot = aggregator.update(&[node1, node2]).await;

        assert_eq!(snapshot.nodes_total, 2);
        assert_eq!(snapshot.nodes_online, 2);
        assert_eq!(snapshot.active_streams, 100);
        assert_eq!(snapshot.active_connections, 200);
        assert_eq!(snapshot.bandwidth_in_mbps, 30.0);
        assert_eq!(snapshot.bandwidth_out_mbps, 200.0);
        assert_eq!(snapshot.cache_used_mb, 3000);
        assert_eq!(snapshot.cache_capacity_mb, 10000);
        assert_eq!(snapshot.average_cpu_pct, 30.0);
        assert_eq!(snapshot.average_ram_pct, 50.0);
        assert_eq!(snapshot.origin_requests_total, 300);
        assert_eq!(snapshot.origin_errors_total, 3);
        assert_eq!(snapshot.origin_error_rate_pct, 1.0); // 3 / 300 = 1.0%

        let history = aggregator.history().await;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].active_streams, 100);
        assert_eq!(history[0].bandwidth_out_mbps, 200.0);
    }
}
