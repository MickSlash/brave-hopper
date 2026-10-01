use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Default)]
pub struct RequestMetric {
    pub duration_ms: f64,
    pub status: u16,
    pub bytes: usize,
    pub cache_hit: bool,
    pub cache_miss: bool,
    pub error: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub target_url: String,
    pub concurrency: usize,
    pub total_requests: usize,
    pub successful_requests: usize,
    pub failed_requests: usize,
    pub total_time_secs: f64,
    pub requests_per_sec: f64,
    pub total_bytes_transferred: u64,
    pub transfer_mbps: f64,
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub cache_hit_ratio: f64,
    pub min_latency_ms: f64,
    pub mean_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub max_latency_ms: f64,
}

impl BenchmarkReport {
    pub fn compute(
        target_url: String,
        concurrency: usize,
        elapsed: Duration,
        mut metrics: Vec<RequestMetric>,
    ) -> Self {
        let total_requests = metrics.len();
        let total_time_secs = elapsed.as_secs_f64().max(0.0001);
        let requests_per_sec = (total_requests as f64) / total_time_secs;

        let mut successful_requests = 0;
        let mut failed_requests = 0;
        let mut total_bytes_transferred = 0u64;
        let mut cache_hits = 0;
        let mut cache_misses = 0;
        let mut durations: Vec<f64> = Vec::with_capacity(total_requests);

        for m in metrics.drain(..) {
            if m.error || m.status >= 500 {
                failed_requests += 1;
            } else if m.status >= 200 && m.status < 400 {
                successful_requests += 1;
            } else {
                failed_requests += 1;
            }

            total_bytes_transferred += m.bytes as u64;
            if m.cache_hit {
                cache_hits += 1;
            } else if m.cache_miss {
                cache_misses += 1;
            }

            if !m.error {
                durations.push(m.duration_ms);
            }
        }

        durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        let min_latency_ms = durations.first().copied().unwrap_or(0.0);
        let max_latency_ms = durations.last().copied().unwrap_or(0.0);
        let mean_latency_ms = if !durations.is_empty() {
            durations.iter().sum::<f64>() / (durations.len() as f64)
        } else {
            0.0
        };

        let percentile = |p: f64| -> f64 {
            if durations.is_empty() {
                return 0.0;
            }
            let idx = ((durations.len() as f64) * p).floor() as usize;
            durations[idx.min(durations.len() - 1)]
        };

        let p50_latency_ms = percentile(0.50);
        let p90_latency_ms = percentile(0.90);
        let p95_latency_ms = percentile(0.95);
        let p99_latency_ms = percentile(0.99);

        let transfer_mbps =
            (total_bytes_transferred as f64 * 8.0) / (total_time_secs * 1_000_000.0);
        let cache_sum = cache_hits + cache_misses;
        let cache_hit_ratio = if cache_sum > 0 {
            (cache_hits as f64 / cache_sum as f64) * 100.0
        } else {
            0.0
        };

        Self {
            target_url,
            concurrency,
            total_requests,
            successful_requests,
            failed_requests,
            total_time_secs: (total_time_secs * 1000.0).round() / 1000.0,
            requests_per_sec: (requests_per_sec * 10.0).round() / 10.0,
            total_bytes_transferred,
            transfer_mbps: (transfer_mbps * 100.0).round() / 100.0,
            cache_hits,
            cache_misses,
            cache_hit_ratio: (cache_hit_ratio * 10.0).round() / 10.0,
            min_latency_ms: (min_latency_ms * 100.0).round() / 100.0,
            mean_latency_ms: (mean_latency_ms * 100.0).round() / 100.0,
            p50_latency_ms: (p50_latency_ms * 100.0).round() / 100.0,
            p90_latency_ms: (p90_latency_ms * 100.0).round() / 100.0,
            p95_latency_ms: (p95_latency_ms * 100.0).round() / 100.0,
            p99_latency_ms: (p99_latency_ms * 100.0).round() / 100.0,
            max_latency_ms: (max_latency_ms * 100.0).round() / 100.0,
        }
    }

    pub fn print_summary(&self) {
        println!();
        println!("============================================================");
        println!("               STREAM CDN BENCHMARK REPORT                  ");
        println!("============================================================");
        println!("Target URL:             {}", self.target_url);
        println!("Concurrency Level:      {} workers", self.concurrency);
        println!("Total Requests:         {}", self.total_requests);
        println!("Successful (2xx/206):   {}", self.successful_requests);
        println!("Failed / Errors:        {}", self.failed_requests);
        println!(
            "Test Duration:          {:.3} seconds",
            self.total_time_secs
        );
        println!(
            "Throughput (RPS):       {:.1} req/sec",
            self.requests_per_sec
        );
        println!(
            "Transfer Rate:          {:.2} Mbps ({:.2} MB total)",
            self.transfer_mbps,
            self.total_bytes_transferred as f64 / (1024.0 * 1024.0)
        );
        println!("------------------------------------------------------------");
        println!("Cache Statistics:");
        println!("  Cache Hits:           {}", self.cache_hits);
        println!("  Cache Misses:         {}", self.cache_misses);
        println!("  Cache Hit Ratio:      {:.1}%", self.cache_hit_ratio);
        println!("------------------------------------------------------------");
        println!("Latency Percentiles (end-to-end round trip):");
        println!("  Min Latency:          {:.2} ms", self.min_latency_ms);
        println!("  Mean Latency:         {:.2} ms", self.mean_latency_ms);
        println!("  p50 (Median):         {:.2} ms", self.p50_latency_ms);
        println!("  p90:                  {:.2} ms", self.p90_latency_ms);
        println!("  p95:                  {:.2} ms", self.p95_latency_ms);
        println!("  p99:                  {:.2} ms", self.p99_latency_ms);
        println!("  Max Latency:          {:.2} ms", self.max_latency_ms);
        println!("============================================================");
        println!();
    }
}
