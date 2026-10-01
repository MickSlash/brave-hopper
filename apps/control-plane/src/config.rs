use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlConfig {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    #[serde(default = "default_admin_secret")]
    pub admin_secret: String,

    #[serde(default = "default_edge_token")]
    pub edge_provisioning_token: String,

    #[serde(default = "default_client_secret")]
    pub client_signing_secret: String,

    #[serde(default = "default_origin_url")]
    pub origin_url: String,

    #[serde(default = "default_origin_secret")]
    pub origin_auth_secret: String,

    #[serde(default = "default_heartbeat_timeout")]
    pub heartbeat_timeout_secs: u64,

    pub database_url: Option<String>,
}

fn default_listen_addr() -> String {
    "127.0.0.1:8080".to_string()
}
fn default_admin_secret() -> String {
    "default-insecure-admin-secret-change-me".to_string()
}
fn default_edge_token() -> String {
    "default-edge-provisioning-token".to_string()
}
fn default_client_secret() -> String {
    "default-client-signing-secret".to_string()
}
fn default_origin_url() -> String {
    "http://127.0.0.1:9000".to_string()
}
fn default_origin_secret() -> String {
    "default-origin-shared-secret".to_string()
}
fn default_heartbeat_timeout() -> u64 {
    30
}

impl Default for ControlConfig {
    fn default() -> Self {
        Self {
            listen_addr: default_listen_addr(),
            admin_secret: default_admin_secret(),
            edge_provisioning_token: default_edge_token(),
            client_signing_secret: default_client_secret(),
            origin_url: default_origin_url(),
            origin_auth_secret: default_origin_secret(),
            heartbeat_timeout_secs: default_heartbeat_timeout(),
            database_url: None,
        }
    }
}

impl ControlConfig {
    pub fn load_from_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = fs::read_to_string(path)?;
        let config: ControlConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
