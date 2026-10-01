use clap::Parser;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing::info;

mod cache;
mod config;
mod control_client;
mod engine;
mod heartbeat;
mod limiter;
mod routes;
mod singleflight;
mod state;
mod system_sampler;
mod upstream;

use config::EdgeConfig;
use state::EdgeState;

#[derive(Parser, Debug)]
#[command(name = "stream-edge")]
#[command(author = "Senior Rust Backend Engineer")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Ultra-lean high-performance streaming edge node")]
struct Args {
    /// Path to TOML configuration file
    #[arg(short, long, env = "EDGE_CONFIG_PATH")]
    config: Option<PathBuf>,

    /// Control plane base URL
    #[arg(long, env = "CONTROL_PLANE_URL")]
    control: Option<String>,

    /// Edge provisioning token
    #[arg(long, env = "EDGE_TOKEN")]
    token: Option<String>,

    /// Unique edge name/identifier
    #[arg(long, env = "EDGE_NAME")]
    name: Option<String>,

    /// Address and port to bind
    #[arg(short, long, env = "EDGE_BIND")]
    bind: Option<String>,

    /// Enable ultra-low-resource mode (Atom 230 / 512MB RAM mode)
    #[arg(long, env = "LOW_RESOURCE_MODE")]
    low_resource: bool,

    /// Require cryptographic signed tokens for all client stream playback
    #[arg(long, env = "EDGE_REQUIRE_AUTH")]
    auth: bool,

    /// Emit logs in structured JSON format
    #[arg(long, env = "EDGE_LOG_JSON")]
    json_logs: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    common::init_logging("stream-edge", args.json_logs);

    let mut config = if let Some(ref config_path) = args.config {
        info!(path = ?config_path, "Loading configuration file");
        EdgeConfig::load_from_file(config_path)?
    } else {
        info!("No configuration file specified, using defaults");
        EdgeConfig::default()
    };

    // Apply CLI overrides
    if let Some(ctrl) = args.control.clone() {
        config.control.url = ctrl;
    }
    if let Some(tok) = args.token.clone() {
        config.edge.token = tok;
    }
    if let Some(nm) = args.name.clone() {
        config.edge.name = nm;
    }
    if let Some(b) = args.bind.clone() {
        config.edge.listen_addr = b;
    }
    if args.low_resource {
        config.edge.low_resource_mode = true;
    }
    if args.auth {
        config.auth.enabled = true;
    }

    let is_low_resource = config.edge.low_resource_mode;

    // Build Tokio runtime according to hardware profile
    let mut rt_builder = tokio::runtime::Builder::new_multi_thread();
    if is_low_resource {
        info!("Enabling LOW_RESOURCE_MODE: 1 worker thread, reduced resource limits");
        rt_builder.worker_threads(1);
        config.limits.max_connections = 500;
        config.limits.max_streams = 50;
        config.limits.max_bandwidth_mbps = 50;
    }

    let runtime = rt_builder.enable_all().thread_name("edge-worker").build()?;

    runtime.block_on(async_main(config))
}

async fn async_main(config: EdgeConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listen_addr = config.edge.listen_addr.clone();
    let state = EdgeState::new(config);

    // Initialize filesystem cache and rebuild in-memory index
    state.cache.init().await?;

    // Spawn background heartbeat and registration loop
    tokio::spawn(heartbeat::run_edge_heartbeat_loop(state.clone()));

    // Spawn background rate limiter garbage collector sweeping idle IP buckets every 60s
    let limiter = state.rate_limiter.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let swept = limiter.sweep_stale(std::time::Duration::from_secs(60));
            if swept > 0 {
                tracing::debug!(swept = swept, "Swept stale client IP rate limiter buckets");
            }
        }
    });

    let upstream = std::sync::Arc::new(upstream::UpstreamClient::new());
    let app = routes::create_router(state, upstream);

    let listener = TcpListener::bind(&listen_addr).await?;
    info!(bind = %listen_addr, "stream-edge listening for incoming traffic");

    let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    axum::serve(listener, service)
        .with_graceful_shutdown(common::wait_for_shutdown_signal("stream-edge"))
        .await?;

    info!("stream-edge shutdown complete");
    Ok(())
}
