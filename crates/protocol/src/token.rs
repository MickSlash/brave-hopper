use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTokenClaims {
    pub stream_id: String,
    pub edge_id: String,
    pub client_ip: Option<String>,
    pub expires_at: i64,
}

impl StreamTokenClaims {
    pub fn new(
        stream_id: impl Into<String>,
        edge_id: impl Into<String>,
        client_ip: Option<String>,
        expires_at: i64,
    ) -> Self {
        Self {
            stream_id: stream_id.into(),
            edge_id: edge_id.into(),
            client_ip,
            expires_at,
        }
    }

    /// Generates canonical signature payload: "stream_id:client_ip:expires_at:edge_id"
    pub fn canonical_message(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.stream_id,
            self.client_ip.as_deref().unwrap_or(""),
            self.expires_at,
            self.edge_id
        )
    }
}
