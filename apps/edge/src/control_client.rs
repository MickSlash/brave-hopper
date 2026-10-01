use protocol::{HeartbeatPayload, HeartbeatResponse, RegisterEdgeRequest, RegisterEdgeResponse};
use std::time::Duration;
use tracing::{debug, error, info};
use uuid::Uuid;

#[derive(Clone)]
pub struct ControlClient {
    client: reqwest::Client,
    base_url: String,
}

impl ControlClient {
    pub fn new(base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .connect_timeout(Duration::from_secs(3))
            .build()
            .expect("Failed to create reqwest HTTP client for control plane");

        Self { client, base_url }
    }

    /// Registers the edge node with the control plane using the provisioning token.
    pub async fn register(
        &self,
        provisioning_token: &str,
        req: &RegisterEdgeRequest,
    ) -> Result<RegisterEdgeResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/internal/edges/register",
            self.base_url.trim_end_matches('/')
        );
        info!(url = %url, node_name = %req.node_name, "Attempting registration with control plane");

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", provisioning_token))
            .json(req)
            .send()
            .await?;

        if response.status().is_success() {
            let reg_resp: RegisterEdgeResponse = response.json().await?;
            info!(
                node_id = %reg_resp.node_id,
                heartbeat_interval = reg_resp.heartbeat_interval_secs,
                "Registered successfully with control plane"
            );
            Ok(reg_resp)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            error!(status = %status, body = %body, "Registration failed");
            Err(format!("Control plane registration returned {}: {}", status, body).into())
        }
    }

    /// Transmits periodic heartbeat with aggregated node telemetry.
    pub async fn send_heartbeat(
        &self,
        node_id: Uuid,
        auth_secret: &str,
        payload: &HeartbeatPayload,
    ) -> Result<HeartbeatResponse, Box<dyn std::error::Error + Send + Sync>> {
        let url = format!(
            "{}/internal/edges/{}/heartbeat",
            self.base_url.trim_end_matches('/'),
            node_id
        );
        debug!(url = %url, node_id = %node_id, "Sending heartbeat to control plane");

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", auth_secret))
            .json(payload)
            .send()
            .await?;

        if response.status().is_success() {
            let hb_resp: HeartbeatResponse = response.json().await?;
            Ok(hb_resp)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Control plane heartbeat returned {}: {}", status, body).into())
        }
    }
}
