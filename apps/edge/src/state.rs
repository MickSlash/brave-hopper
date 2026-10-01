use metrics::EdgeMetrics;
use protocol::NodeStatus;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::cache::CacheManager;
use crate::config::EdgeConfig;
use crate::singleflight::SingleFlight;

#[derive(Clone)]
pub struct EdgeState {
    pub config: Arc<EdgeConfig>,
    pub metrics: Arc<EdgeMetrics>,
    pub cache: Arc<CacheManager>,
    pub single_flight: Arc<SingleFlight>,
    pub node_id: Arc<RwLock<Option<Uuid>>>,
    pub auth_secret: Arc<RwLock<Option<String>>>,
    pub origin_base_url: Arc<RwLock<String>>,
    pub origin_auth_secret: Arc<RwLock<String>>,
    pub client_signing_secret: Arc<RwLock<String>>,
    pub auth_enabled: Arc<AtomicBool>,
    pub enforce_ip: Arc<AtomicBool>,
    pub rate_limiter: Arc<crate::limiter::RateLimiter>,
    pub status: Arc<AtomicU8>,
    pub start_time: Instant,
}

impl EdgeState {
    pub fn new(config: EdgeConfig) -> Self {
        let initial_origin = config.origin.url.clone();
        let initial_origin_secret = config.origin.auth_secret.clone();
        let cache_path = std::path::PathBuf::from(&config.cache.path);
        let cache_max_bytes = config.cache.max_size_gb * 1024 * 1024 * 1024;
        let cache_ttl = std::time::Duration::from_secs(config.cache.ttl_secs);
        let cache = CacheManager::new(cache_path, cache_max_bytes, cache_ttl);
        let single_flight = Arc::new(SingleFlight::new());

        let initial_client_secret = config.auth.client_signing_secret.clone();
        let auth_enabled = Arc::new(AtomicBool::new(config.auth.enabled));
        let enforce_ip = Arc::new(AtomicBool::new(config.auth.enforce_ip));
        let rate_limiter = Arc::new(crate::limiter::RateLimiter::new(
            config.rate_limit.enabled,
            config.rate_limit.requests_per_second,
            config.rate_limit.burst_capacity,
        ));

        Self {
            config: Arc::new(config),
            metrics: Arc::new(EdgeMetrics::new()),
            cache,
            single_flight,
            node_id: Arc::new(RwLock::new(None)),
            auth_secret: Arc::new(RwLock::new(None)),
            origin_base_url: Arc::new(RwLock::new(initial_origin)),
            origin_auth_secret: Arc::new(RwLock::new(initial_origin_secret)),
            client_signing_secret: Arc::new(RwLock::new(initial_client_secret)),
            auth_enabled,
            enforce_ip,
            rate_limiter,
            status: Arc::new(AtomicU8::new(node_status_to_u8(NodeStatus::Online))),
            start_time: Instant::now(),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn get_status(&self) -> NodeStatus {
        u8_to_node_status(self.status.load(Ordering::Relaxed))
    }

    pub fn set_status(&self, status: NodeStatus) {
        self.status
            .store(node_status_to_u8(status), Ordering::Relaxed);
    }

    pub async fn apply_registration(
        &self,
        node_id: Uuid,
        auth_secret: String,
        origin_base_url: String,
        origin_auth_secret: String,
        client_signing_secret: String,
    ) {
        *self.node_id.write().await = Some(node_id);
        *self.auth_secret.write().await = Some(auth_secret);
        *self.origin_base_url.write().await = origin_base_url;
        *self.origin_auth_secret.write().await = origin_auth_secret;
        if !client_signing_secret.is_empty() {
            *self.client_signing_secret.write().await = client_signing_secret;
        }
        self.set_status(NodeStatus::Online);
    }
}

fn node_status_to_u8(status: NodeStatus) -> u8 {
    match status {
        NodeStatus::Online => 0,
        NodeStatus::Degraded => 1,
        NodeStatus::Offline => 2,
        NodeStatus::Draining => 3,
    }
}

fn u8_to_node_status(val: u8) -> NodeStatus {
    match val {
        0 => NodeStatus::Online,
        1 => NodeStatus::Degraded,
        2 => NodeStatus::Offline,
        3 => NodeStatus::Draining,
        _ => NodeStatus::Online,
    }
}
