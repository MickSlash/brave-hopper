use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Ultra-lightweight, zero-allocation metrics engine for Edge nodes.
/// Uses atomic integers for uncontended parallel updates on the hot path.
#[derive(Debug, Default)]
pub struct EdgeMetrics {
    pub requests: AtomicU64,
    pub active_connections: AtomicUsize,
    pub active_streams: AtomicUsize,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub origin_requests: AtomicU64,
    pub origin_errors: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeMetricsSnapshot {
    pub requests: u64,
    pub active_connections: usize,
    pub active_streams: usize,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub origin_requests: u64,
    pub origin_errors: u64,
    pub cache_hit_ratio: f32,
}

impl EdgeMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    #[inline]
    pub fn inc_requests(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_active_connections(&self) -> usize {
        self.active_connections.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[inline]
    pub fn dec_active_connections(&self) -> usize {
        self.active_connections
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                Some(val.saturating_sub(1))
            })
            .unwrap_or(0)
    }

    #[inline]
    pub fn inc_active_streams(&self) -> usize {
        self.active_streams.fetch_add(1, Ordering::Relaxed) + 1
    }

    #[inline]
    pub fn dec_active_streams(&self) -> usize {
        self.active_streams
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                Some(val.saturating_sub(1))
            })
            .unwrap_or(0)
    }

    #[inline]
    pub fn add_bytes_in(&self, bytes: u64) {
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_bytes_out(&self, bytes: u64) {
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_cache_hit(&self) {
        self.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_cache_miss(&self) {
        self.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_origin_requests(&self) {
        self.origin_requests.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn inc_origin_errors(&self) {
        self.origin_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> EdgeMetricsSnapshot {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;
        let cache_hit_ratio = if total > 0 {
            hits as f32 / total as f32
        } else {
            0.0
        };

        EdgeMetricsSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            active_connections: self.active_connections.load(Ordering::Relaxed),
            active_streams: self.active_streams.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            cache_hits: hits,
            cache_misses: misses,
            origin_requests: self.origin_requests.load(Ordering::Relaxed),
            origin_errors: self.origin_errors.load(Ordering::Relaxed),
            cache_hit_ratio,
        }
    }
}
