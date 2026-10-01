use axum::{extract::State, middleware, response::IntoResponse, routing::get, Json, Router};
use clap::Parser;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

mod auth;
mod routes;

#[derive(Parser, Debug)]
#[command(name = "stream-origin")]
#[command(author = "Senior Rust Backend Engineer")]
#[command(version = env!("CARGO_PKG_VERSION"))]
#[command(about = "Protected Origin Mock Server for HLS & Media Streaming")]
struct Args {
    /// Address and port to bind
    #[arg(short, long, env = "ORIGIN_BIND", default_value = "127.0.0.1:9000")]
    bind: String,

    /// Primary shared secret for edge HMAC authentication
    #[arg(
        short,
        long,
        env = "ORIGIN_AUTH_SECRET",
        default_value = "dev-origin-hmac-shared-secret"
    )]
    secret: String,

    /// Optional fallback shared secret for zero-downtime secret rotation
    #[arg(long, env = "ORIGIN_PREVIOUS_AUTH_SECRET")]
    previous_secret: Option<String>,

    /// Emit logs in structured JSON format
    #[arg(long, env = "ORIGIN_LOG_JSON")]
    json_logs: bool,
}

async fn auth_status(State(auth_state): State<auth::OriginAuthState>) -> impl IntoResponse {
    let dual_key = auth_state.fallback_secret.is_some();
    let nonces = auth_state.nonce_tracker.len();
    Json(serde_json::json!({
        "status": "ok",
        "dual_key_rotation_active": dual_key,
        "tracked_nonces_count": nonces,
        "max_skew_secs": auth_state.max_skew_secs
    }))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();
    common::init_logging("stream-origin", args.json_logs);

    let nonce_tracker = ::auth::NonceTracker::new();

    // Spawn background sweep task to prune expired nonces every 30 seconds
    let sweep_tracker = nonce_tracker.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
        loop {
            interval.tick().await;
            let now = chrono::Utc::now().timestamp();
            let swept = sweep_tracker.sweep_expired(now);
            if swept > 0 {
                tracing::debug!(swept = swept, "Swept expired nonces from origin cache");
            }
        }
    });

    let auth_state = auth::OriginAuthState {
        primary_secret: Arc::new(args.secret.into_bytes()),
        fallback_secret: args.previous_secret.map(|s| Arc::new(s.into_bytes())),
        nonce_tracker,
        max_skew_secs: 30,
    };

    let auth_state_for_mw = auth_state.clone();

    // Protected media routes
    let protected_media_routes = Router::new()
        .route("/media/{*path}", get(routes::media_handler))
        .layer(middleware::from_fn(move |req, next| {
            let state = auth_state_for_mw.clone();
            async move { auth::verify_origin_hmac(state, req, next).await }
        }));

    // Main router
    let app = Router::new()
        .route("/health", get(routes::health_check))
        .route("/internal/auth/status", get(auth_status))
        .merge(protected_media_routes)
        .with_state(auth_state);

    let listener = TcpListener::bind(&args.bind).await?;
    info!(
        bind = %args.bind,
        "stream-origin server listening (hardened HMAC with anti-replay nonce tracking and dual-key rotation)"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(common::wait_for_shutdown_signal("stream-origin"))
        .await?;

    info!("stream-origin shutdown complete");
    Ok(())
}
