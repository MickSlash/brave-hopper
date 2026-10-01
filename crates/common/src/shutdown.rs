use tokio::signal;
use tracing::info;

/// Listens for OS shutdown signals (Ctrl+C on all OSes, SIGTERM on Unix).
pub async fn wait_for_shutdown_signal(service_name: &str) {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!(service = %service_name, "Received Ctrl+C, initiating graceful shutdown");
        }
        _ = terminate => {
            info!(service = %service_name, "Received SIGTERM, initiating graceful shutdown");
        }
    }
}
