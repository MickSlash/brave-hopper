use clap::Parser;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tracing::info;

use std::time::Duration;

mod config;
mod registry;
mod routes;
mod state;
mod telemetry;

use config::ControlConfig;
use state::AppState;

#[derive(Parser, Debug)]
#[command(name = "stream-control")]
#[command(author = "Senior Rust Backend Engineer")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "High-performance streaming control plane")]
struct Args {
    /// Path to TOML configuration file
    #[arg(short, long, env = "CONTROL_CONFIG_PATH")]
    config: Option<PathBuf>,

    /// Address and port to bind (overrides config)
    #[arg(short, long, env = "CONTROL_BIND")]
    bind: Option<String>,

    /// Emit logs in structured JSON format
    #[arg(long, env = "CONTROL_LOG_JSON")]
    json_logs: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    common::init_logging("stream-control", args.json_logs);

    let mut config = if let Some(config_path) = args.config {
        info!(path = ?config_path, "Loading configuration file");
        ControlConfig::load_from_file(config_path)?
    } else {
        info!("No configuration file specified, using defaults");
        ControlConfig::default()
    };

    if let Some(bind_override) = args.bind {
        config.listen_addr = bind_override;
    }

    let listen_addr = config.listen_addr.clone();
    let state = AppState::new(config);

    // Spawn background supervisor to reap stale nodes and roll up cluster telemetry
    let supervisor_registry = state.registry.clone();
    let supervisor_telemetry = state.telemetry.clone();
    let reaper_timeout = Duration::from_secs(state.config.heartbeat_timeout_secs);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            interval.tick().await;
            supervisor_registry.reap_stale_nodes(reaper_timeout).await;
            let nodes = supervisor_registry.list_nodes().await;
            supervisor_telemetry.update(&nodes).await;
        }
    });

    let app = routes::create_router(state);

    let listener = TcpListener::bind(&listen_addr).await?;
    info!(bind = %listen_addr, "stream-control listening for connections");

    let service = app.into_make_service_with_connect_info::<std::net::SocketAddr>();

    axum::serve(listener, service)
        .with_graceful_shutdown(common::wait_for_shutdown_signal("stream-control"))
        .await?;

    info!("stream-control shutdown complete");
    Ok(())
}
