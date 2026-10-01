use serde::{Deserialize, Serialize};
use std::{fs, path::Path};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EdgeConfig {
    #[serde(default)]
    pub edge: NodeSettings,

    #[serde(default)]
    pub control: ControlSettings,

    #[serde(default)]
    pub cache: CacheSettings,

    #[serde(default)]
    pub limits: ResourceLimits,

    #[serde(default)]
    pub origin: OriginSettings,

    #[serde(default)]
    pub auth: AuthSettings,

    #[serde(default)]
    pub rate_limit: RateLimitSettings,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSettings {
    #[serde(default = "default_node_name")]
    pub name: String,

    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,

    #[serde(default)]
    pub token: String,

    #[serde(default)]
    pub low_resource_mode: bool,

    #[serde(default)]
    pub public_hostname: Option<String>,

    #[serde(default)]
    pub public_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlSettings {
    #[serde(default = "default_control_url")]
    pub url: String,

    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSettings {
    #[serde(default = "default_cache_path")]
    pub path: String,

    #[serde(default = "default_cache_max_size_gb")]
    pub max_size_gb: u64,

    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,

    #[serde(default = "default_max_streams")]
    pub max_streams: usize,

    #[serde(default = "default_max_bandwidth_mbps")]
    pub max_bandwidth_mbps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OriginSettings {
    #[serde(default = "default_origin_url")]
    pub url: String,

    #[serde(default)]
    pub auth_secret: String,

    #[serde(default = "default_origin_path_prefix")]
    pub path_prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSettings {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_client_signing_secret")]
    pub client_signing_secret: String,

    #[serde(default)]
    pub enforce_ip: bool,
}

fn default_client_signing_secret() -> String {
    "default-client-signing-secret".to_string()
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            client_signing_secret: default_client_signing_secret(),
            enforce_ip: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitSettings {
    #[serde(default = "default_rate_limit_enabled")]
    pub enabled: bool,

    #[serde(default = "default_rate_limit_rps")]
    pub requests_per_second: f64,

    #[serde(default = "default_rate_limit_burst")]
    pub burst_capacity: f64,
}

fn default_rate_limit_enabled() -> bool {
    true
}

fn default_rate_limit_rps() -> f64 {
    50.0
}

fn default_rate_limit_burst() -> f64 {
    100.0
}

impl Default for RateLimitSettings {
    fn default() -> Self {
        Self {
            enabled: default_rate_limit_enabled(),
            requests_per_second: default_rate_limit_rps(),
            burst_capacity: default_rate_limit_burst(),
        }
    }
}

fn default_node_name() -> String {
    "edge-01".to_string()
}
fn default_listen_addr() -> String {
    "127.0.0.1:8081".to_string()
}
fn default_control_url() -> String {
    "http://127.0.0.1:8080".to_string()
}
fn default_heartbeat_interval() -> u64 {
    15
}
fn default_cache_path() -> String {
    "./cache/stream-edge".to_string()
}
fn default_cache_max_size_gb() -> u64 {
    50
}
fn default_cache_ttl_secs() -> u64 {
    86400
}
fn default_max_connections() -> usize {
    5000
}
fn default_max_streams() -> usize {
    500
}
fn default_max_bandwidth_mbps() -> u64 {
    1000
}
fn default_origin_url() -> String {
    "http://127.0.0.1:9000".to_string()
}
fn default_origin_path_prefix() -> String {
    "media".to_string()
}

impl Default for NodeSettings {
    fn default() -> Self {
        Self {
            name: default_node_name(),
            listen_addr: default_listen_addr(),
            token: String::new(),
            low_resource_mode: false,
            public_hostname: None,
            public_port: None,
        }
    }
}

impl Default for ControlSettings {
    fn default() -> Self {
        Self {
            url: default_control_url(),
            heartbeat_interval_secs: default_heartbeat_interval(),
        }
    }
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            path: default_cache_path(),
            max_size_gb: default_cache_max_size_gb(),
            ttl_secs: default_cache_ttl_secs(),
        }
    }
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_connections: default_max_connections(),
            max_streams: default_max_streams(),
            max_bandwidth_mbps: default_max_bandwidth_mbps(),
        }
    }
}

impl Default for OriginSettings {
    fn default() -> Self {
        Self {
            url: default_origin_url(),
            auth_secret: String::new(),
            path_prefix: default_origin_path_prefix(),
        }
    }
}

impl EdgeConfig {
    pub fn load_from_file<P: AsRef<Path>>(
        path: P,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = fs::read_to_string(path)?;
        let config: EdgeConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
