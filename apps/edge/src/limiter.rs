use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq)]
pub enum RateLimitResult {
    Allowed { remaining: f64, limit: f64 },
    Denied { retry_after_secs: u64, limit: f64 },
}

#[derive(Debug, Clone)]
struct ClientBucket {
    tokens: f64,
    last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    enabled: bool,
    requests_per_second: f64,
    burst_capacity: f64,
    buckets: Arc<RwLock<HashMap<String, ClientBucket>>>,
}

impl RateLimiter {
    pub fn new(enabled: bool, requests_per_second: f64, burst_capacity: f64) -> Self {
        Self {
            enabled,
            requests_per_second: requests_per_second.max(0.1),
            burst_capacity: burst_capacity.max(1.0),
            buckets: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Evaluates if a client IP is allowed to proceed using the Token Bucket algorithm.
    pub fn check(&self, client_ip: &str) -> RateLimitResult {
        if !self.enabled {
            return RateLimitResult::Allowed {
                remaining: self.burst_capacity,
                limit: self.burst_capacity,
            };
        }

        let now = Instant::now();
        let mut buckets = self.buckets.write().unwrap();

        let bucket = buckets
            .entry(client_ip.to_string())
            .or_insert_with(|| ClientBucket {
                tokens: self.burst_capacity,
                last_update: now,
            });

        // Calculate token refill based on elapsed time
        let elapsed = now.duration_since(bucket.last_update).as_secs_f64();
        bucket.tokens =
            (bucket.tokens + elapsed * self.requests_per_second).min(self.burst_capacity);
        bucket.last_update = now;

        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            RateLimitResult::Allowed {
                remaining: bucket.tokens,
                limit: self.burst_capacity,
            }
        } else {
            let deficit = 1.0 - bucket.tokens;
            let wait_secs = (deficit / self.requests_per_second).ceil().max(1.0) as u64;
            RateLimitResult::Denied {
                retry_after_secs: wait_secs,
                limit: self.burst_capacity,
            }
        }
    }

    /// Garbage collects stale client buckets that have been inactive longer than `max_idle`.
    pub fn sweep_stale(&self, max_idle: Duration) -> usize {
        let mut buckets = self.buckets.write().unwrap();
        let before = buckets.len();
        let now = Instant::now();
        buckets.retain(|_, bucket| now.duration_since(bucket.last_update) <= max_idle);
        before - buckets.len()
    }

    /// Returns the number of currently tracked client IPs.
    #[allow(dead_code)]
    pub fn tracked_count(&self) -> usize {
        self.buckets.read().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_allows_burst_then_denies() {
        // Burst 3, 1 RPS
        let limiter = RateLimiter::new(true, 1.0, 3.0);
        let ip = "192.168.1.100";

        // Request 1: Allowed (remaining 2)
        match limiter.check(ip) {
            RateLimitResult::Allowed { remaining, limit } => {
                assert_eq!(limit, 3.0);
                assert!((remaining - 2.0).abs() < 0.01);
            }
            _ => panic!("Expected allowed"),
        }

        // Request 2: Allowed (remaining 1)
        match limiter.check(ip) {
            RateLimitResult::Allowed { remaining, .. } => {
                assert!((remaining - 1.0).abs() < 0.01);
            }
            _ => panic!("Expected allowed"),
        }

        // Request 3: Allowed (remaining 0)
        match limiter.check(ip) {
            RateLimitResult::Allowed { remaining, .. } => {
                assert!(remaining < 0.01);
            }
            _ => panic!("Expected allowed"),
        }

        // Request 4: Denied (burst exhausted)
        match limiter.check(ip) {
            RateLimitResult::Denied {
                retry_after_secs,
                limit,
            } => {
                assert_eq!(limit, 3.0);
                assert_eq!(retry_after_secs, 1);
            }
            _ => panic!("Expected denied"),
        }
    }

    #[test]
    fn test_rate_limiter_refills_over_time() {
        // Burst 2, 10 RPS (1 token every 100ms)
        let limiter = RateLimiter::new(true, 10.0, 2.0);
        let ip = "10.0.0.1";

        assert!(matches!(limiter.check(ip), RateLimitResult::Allowed { .. }));
        assert!(matches!(limiter.check(ip), RateLimitResult::Allowed { .. }));
        // Exhausted
        assert!(matches!(limiter.check(ip), RateLimitResult::Denied { .. }));

        // Sleep 150ms to allow refill of 1.5 tokens
        std::thread::sleep(Duration::from_millis(150));

        // Now allowed again!
        assert!(matches!(limiter.check(ip), RateLimitResult::Allowed { .. }));
    }

    #[test]
    fn test_rate_limiter_stale_eviction() {
        let limiter = RateLimiter::new(true, 10.0, 5.0);
        limiter.check("1.1.1.1");
        limiter.check("2.2.2.2");
        assert_eq!(limiter.tracked_count(), 2);

        // Immediate sweep with 1s max_idle sweeps nothing
        let swept = limiter.sweep_stale(Duration::from_secs(1));
        assert_eq!(swept, 0);
        assert_eq!(limiter.tracked_count(), 2);

        // Sleep 50ms and sweep with 10ms max_idle
        std::thread::sleep(Duration::from_millis(50));
        let swept = limiter.sweep_stale(Duration::from_millis(10));
        assert_eq!(swept, 2);
        assert_eq!(limiter.tracked_count(), 0);
    }

    #[test]
    fn test_rate_limiter_disabled() {
        let limiter = RateLimiter::new(false, 1.0, 1.0);
        let ip = "192.168.1.1";

        for _ in 0..10 {
            assert!(matches!(limiter.check(ip), RateLimitResult::Allowed { .. }));
        }
    }
}
