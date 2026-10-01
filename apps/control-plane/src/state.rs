use crate::config::ControlConfig;
use crate::registry::EdgeRegistry;
use crate::telemetry::TelemetryAggregator;
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ControlConfig>,
    pub registry: Arc<EdgeRegistry>,
    pub telemetry: Arc<TelemetryAggregator>,
    pub start_time: Instant,
}

impl AppState {
    pub fn new(config: ControlConfig) -> Self {
        Self {
            config: Arc::new(config),
            registry: EdgeRegistry::new(),
            telemetry: TelemetryAggregator::new(120), // 120 points buffer
            start_time: Instant::now(),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}
