use clap::Parser;
use futures_util::StreamExt;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

mod stats;
use stats::{BenchmarkReport, RequestMetric};

#[derive(Parser, Debug)]
#[command(name = "stream-bench")]
#[command(author = "Senior Rust Backend Engineer")]
#[command(version = "0.1.0")]
#[command(
    about = "Ultra-high performance synthetic load generator & benchmarking tool for Stream CDN"
)]
struct Args {
    /// Target stream URL to benchmark (e.g. http://127.0.0.1:8081/stream/hls-demo/seg-01.m4s)
    #[arg(short, long)]
    url: String,

    /// Number of concurrent worker tasks
    #[arg(short, long, default_value_t = 50)]
    concurrency: usize,

    /// Total number of requests to execute
    #[arg(short = 'n', long, default_value_t = 1000)]
    requests: usize,

    /// Optional HTTP Range header to benchmark partial content delivery (e.g. bytes=0-65535)
    #[arg(long)]
    range: Option<String>,

    /// Number of warmup requests to evaluate cache prime vs hit latency
    #[arg(long, default_value_t = 3)]
    warmup: usize,

    /// Output full benchmark report as JSON
    #[arg(long)]
    json: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(args.concurrency * 2)
        .pool_idle_timeout(std::time::Duration::from_secs(60))
        .tcp_keepalive(std::time::Duration::from_secs(15))
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let mut custom_headers = HeaderMap::new();
    if let Some(ref range_val) = args.range {
        custom_headers.insert(
            HeaderName::from_static("range"),
            HeaderValue::from_str(range_val)?,
        );
    }

    println!("Starting Stream CDN Benchmark against: {}", args.url);
    println!(
        "Configuration: {} requests across {} concurrent workers",
        args.requests, args.concurrency
    );

    // Warmup phase
    if args.warmup > 0 {
        println!("Running {} warmup requests...", args.warmup);
        for i in 1..=args.warmup {
            let start = Instant::now();
            let mut req = client.get(&args.url);
            for (k, v) in &custom_headers {
                req = req.header(k, v);
            }
            match req.send().await {
                Ok(resp) => {
                    let cache_header = resp
                        .headers()
                        .get("x-cache-status")
                        .and_then(|v| v.to_str().ok())
                        .unwrap_or("NONE")
                        .to_string();
                    let status = resp.status().as_u16();
                    let bytes = resp.bytes().await.map(|b| b.len()).unwrap_or(0);
                    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
                    println!(
                        "  [Warmup {}/{}] Status: {}, Cache: {}, Size: {} bytes, Latency: {:.2} ms",
                        i, args.warmup, status, cache_header, bytes, elapsed_ms
                    );
                }
                Err(e) => {
                    eprintln!("  [Warmup {}/{}] Error: {}", i, args.warmup, e);
                }
            }
        }
        println!("Warmup complete. Commencing stress benchmark...\n");
    }

    let completed_counter = Arc::new(AtomicUsize::new(0));
    let target_url = Arc::new(args.url.clone());
    let client = Arc::new(client);
    let custom_headers = Arc::new(custom_headers);

    let (metrics_tx, mut metrics_rx) = tokio::sync::mpsc::unbounded_channel::<RequestMetric>();

    let start_time = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);

    for _ in 0..args.concurrency {
        let client_c = client.clone();
        let url_c = target_url.clone();
        let headers_c = custom_headers.clone();
        let counter_c = completed_counter.clone();
        let tx = metrics_tx.clone();
        let total_target = args.requests;

        let handle = tokio::spawn(async move {
            loop {
                let current_idx = counter_c.fetch_add(1, Ordering::Relaxed);
                if current_idx >= total_target {
                    break;
                }

                let req_start = Instant::now();
                let mut req = client_c.get(&*url_c);
                for (k, v) in &*headers_c {
                    req = req.header(k, v);
                }

                let metric = match req.send().await {
                    Ok(resp) => {
                        let status = resp.status().as_u16();
                        let cache_status = resp
                            .headers()
                            .get("x-cache-status")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("");
                        let cache_hit = cache_status.eq_ignore_ascii_case("HIT");
                        let cache_miss = cache_status.eq_ignore_ascii_case("MISS");

                        let mut body_stream = resp.bytes_stream();
                        let mut bytes = 0;
                        let mut stream_err = false;
                        while let Some(chunk) = body_stream.next().await {
                            match chunk {
                                Ok(c) => bytes += c.len(),
                                Err(_) => {
                                    stream_err = true;
                                    break;
                                }
                            }
                        }

                        let duration_ms = req_start.elapsed().as_secs_f64() * 1000.0;
                        RequestMetric {
                            duration_ms,
                            status,
                            bytes,
                            cache_hit,
                            cache_miss,
                            error: stream_err,
                        }
                    }
                    Err(_) => {
                        let duration_ms = req_start.elapsed().as_secs_f64() * 1000.0;
                        RequestMetric {
                            duration_ms,
                            status: 0,
                            bytes: 0,
                            cache_hit: false,
                            cache_miss: false,
                            error: true,
                        }
                    }
                };

                let _ = tx.send(metric);
            }
        });

        handles.push(handle);
    }

    drop(metrics_tx);

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start_time.elapsed();

    let mut collected = Vec::with_capacity(args.requests);
    while let Some(m) = metrics_rx.recv().await {
        collected.push(m);
    }

    let report = BenchmarkReport::compute(args.url, args.concurrency, elapsed, collected);

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        report.print_summary();
    }

    Ok(())
}
