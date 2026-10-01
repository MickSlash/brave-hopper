use std::sync::Mutex;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

/// Ultra-lightweight system metrics sampler.
/// Configured to refresh only CPU usage and RAM metrics on demand (during heartbeats),
/// avoiding unnecessary background polling or high resource consumption.
pub struct SystemSampler {
    sys: Mutex<System>,
}

impl Default for SystemSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl SystemSampler {
    pub fn new() -> Self {
        let mut sys = System::new_with_specifics(
            RefreshKind::nothing()
                .with_cpu(CpuRefreshKind::nothing().with_cpu_usage())
                .with_memory(MemoryRefreshKind::nothing().with_ram()),
        );
        sys.refresh_memory();
        sys.refresh_cpu_usage();
        Self {
            sys: Mutex::new(sys),
        }
    }

    /// Samples system metrics: (cpu_percent, memory_used_mb, memory_total_mb, cpu_count)
    pub fn sample(&self) -> (f32, u64, u64, usize) {
        if let Ok(mut sys) = self.sys.lock() {
            sys.refresh_memory();
            sys.refresh_cpu_usage();
            let cpu = sys.global_cpu_usage();
            let mem_used_mb = sys.used_memory() / (1024 * 1024);
            let mem_total_mb = sys.total_memory() / (1024 * 1024);
            let cpu_count = sys.cpus().len().max(1);
            (cpu, mem_used_mb, mem_total_mb, cpu_count)
        } else {
            (0.0, 0, 0, 1)
        }
    }
}
