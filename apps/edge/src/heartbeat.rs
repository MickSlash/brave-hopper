use chrono::Utc;
use protocol::{HeartbeatPayload, NodeCommand, NodeStatus, RegisterEdgeRequest};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::{control_client::ControlClient, state::EdgeState, system_sampler::SystemSampler};

/// Background task managing the lifecycle of the edge node:
/// 1. Registers with the control plane with exponential backoff on startup.
/// 2. Regularly transmits heartbeats with system & streaming metrics.
/// 3. Responds to administrative commands (e.g. Drain).
pub async fn run_edge_heartbeat_loop(state: EdgeState) {
    let control_url = state.config.control.url.clone();
    let provisioning_token = state.config.edge.token.clone();
    let client = ControlClient::new(control_url);
    let sampler = Arc::new(SystemSampler::new());

    // Step 1: Initial Registration with exponential backoff
    let mut backoff = Duration::from_secs(2);
    let reg_response = loop {
        let (_cpu, _mem_used, mem_total, cpu_count) = sampler.sample();

        let listen_port = state
            .config
            .edge
            .listen_addr
            .split(':')
            .nth(1)
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(8081);

        let weight = if state.config.edge.low_resource_mode {
            0.2
        } else {
            1.0
        };

        let hostname = state
            .config
            .edge
            .public_hostname
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let public_port = state.config.edge.public_port.unwrap_or(listen_port);

        let req = RegisterEdgeRequest {
            node_name: state.config.edge.name.clone(),
            hostname,
            public_port,
            internal_port: listen_port,
            version: env!("CARGO_PKG_VERSION").to_string(),
            cpu_count: cpu_count as u32,
            ram_total_mb: mem_total,
            cache_capacity_gb: state.config.cache.max_size_gb,
            max_connections: state.config.limits.max_connections,
            max_streams: state.config.limits.max_streams,
            max_bandwidth_mbps: state.config.limits.max_bandwidth_mbps,
            weight,
            monthly_bandwidth_limit_gb: None,
        };

        match client.register(&provisioning_token, &req).await {
            Ok(resp) => {
                state
                    .apply_registration(
                        resp.node_id,
                        resp.auth_secret.clone(),
                        resp.origin_base_url.clone(),
                        resp.origin_auth_secret.clone(),
                        resp.client_signing_secret.clone(),
                    )
                    .await;
                info!(
                    node_id = %resp.node_id,
                    status = ?resp.status,
                    "Edge registered and initialized"
                );
                break resp;
            }
            Err(e) => {
                warn!(
                    error = %e,
                    retry_in = ?backoff,
                    "Failed to register with control plane, retrying..."
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(15));
            }
        }
    };

    // Step 2: Periodic Heartbeat Loop
    let node_id = reg_response.node_id;
    let mut interval = tokio::time::interval(Duration::from_secs(
        reg_response.heartbeat_interval_secs.max(5),
    ));

    loop {
        interval.tick().await;

        let auth_secret = {
            let guard = state.auth_secret.read().await;
            guard.clone()
        };

        let auth_secret = match auth_secret {
            Some(s) => s,
            None => {
                error!("Edge missing auth_secret, cannot send heartbeat");
                continue;
            }
        };

        let (cpu, mem_used, mem_total, _) = sampler.sample();
        let metrics_snap = state.metrics.snapshot();

        let payload = HeartbeatPayload {
            timestamp: Utc::now().timestamp(),
            uptime_secs: state.uptime_secs(),
            status: state.get_status(),
            cpu_percent: cpu,
            memory_used_mb: mem_used,
            memory_total_mb: mem_total,
            active_connections: metrics_snap.active_connections,
            active_streams: metrics_snap.active_streams,
            bandwidth_in_bps: 0, // Computed dynamically in Milestone 10
            bandwidth_out_bps: 0,
            cache_used_mb: state.cache.used_bytes() / (1024 * 1024),
            cache_capacity_mb: state.config.cache.max_size_gb * 1024,
            cache_hit_ratio: metrics_snap.cache_hit_ratio,
            origin_latency_ms: 0.0,
            origin_requests_count: metrics_snap.origin_requests,
            origin_errors_count: metrics_snap.origin_errors,
            monthly_bandwidth_used_bytes: metrics_snap.bytes_out,
        };

        match client.send_heartbeat(node_id, &auth_secret, &payload).await {
            Ok(resp) => {
                debug!(
                    acknowledged = resp.acknowledged,
                    "Heartbeat delivered successfully"
                );
                if resp.command == NodeCommand::Drain && state.get_status() != NodeStatus::Draining
                {
                    warn!("Received DRAIN command from control plane, transitioning status to DRAINING");
                    state.set_status(NodeStatus::Draining);
                } else if resp.command == NodeCommand::Resume
                    && state.get_status() != NodeStatus::Online
                {
                    info!("Received RESUME command from control plane, transitioning status to ONLINE");
                    state.set_status(NodeStatus::Online);
                }
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "Failed to deliver heartbeat to control plane (cluster will use cached config)"
                );
            }
        }
    }
}
