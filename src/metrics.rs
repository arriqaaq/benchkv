// benchmark/src/metrics.rs
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;

#[derive(Default, Clone)] // Added Clone derive
pub struct Metrics {
    read_latencies: Vec<Duration>,
    update_latencies: Vec<Duration>,
    insert_latencies: Vec<Duration>,
    scan_latencies: Vec<Duration>,
    read_modify_write_latencies: Vec<Duration>,
    delete_latencies: Vec<Duration>,
}

#[derive(Default, Clone)] // Added Clone derive
pub struct ConcurrentMetrics {
    metrics: Arc<Mutex<Metrics>>,
}

impl ConcurrentMetrics {
    pub async fn record_read(&self, duration: Duration) {
        self.metrics.lock().await.read_latencies.push(duration);
    }

    pub async fn record_update(&self, duration: Duration) {
        self.metrics.lock().await.update_latencies.push(duration);
    }

    pub async fn record_insert(&self, duration: Duration) {
        self.metrics.lock().await.insert_latencies.push(duration);
    }

    pub async fn record_scan(&self, duration: Duration) {
        self.metrics.lock().await.scan_latencies.push(duration);
    }

    pub async fn record_delete(&self, duration: Duration) {
        self.metrics.lock().await.delete_latencies.push(duration);
    }

    pub async fn record_read_modify_write(&self, duration: Duration) {
        self.metrics
            .lock()
            .await
            .read_modify_write_latencies
            .push(duration);
    }

    pub async fn get_metrics(&self) -> Metrics {
        let guard = self.metrics.lock().await;
        Metrics {
            read_latencies: guard.read_latencies.clone(),
            update_latencies: guard.update_latencies.clone(),
            insert_latencies: guard.insert_latencies.clone(),
            scan_latencies: guard.scan_latencies.clone(),
            read_modify_write_latencies: guard.read_modify_write_latencies.clone(),
            delete_latencies: guard.delete_latencies.clone(),
        }
    }
}

impl Metrics {
    pub fn record_read(&mut self, duration: Duration) {
        self.read_latencies.push(duration);
    }

    pub fn record_update(&mut self, duration: Duration) {
        self.update_latencies.push(duration);
    }

    pub fn record_insert(&mut self, duration: Duration) {
        self.insert_latencies.push(duration);
    }

    pub fn record_scan(&mut self, duration: Duration) {
        self.scan_latencies.push(duration);
    }

    pub fn record_read_modify_write(&mut self, duration: Duration) {
        self.read_modify_write_latencies.push(duration);
    }

    pub fn record_delete(&mut self, duration: Duration) {
        self.delete_latencies.push(duration);
    }

    fn format_duration(nanos: f64) -> String {
        if nanos < 1_000.0 {
            format!("{:.2} ns", nanos)
        } else if nanos < 1_000_000.0 {
            format!("{:.2} µs", nanos / 1_000.0)
        } else if nanos < 1_000_000_000.0 {
            format!("{:.2} ms", nanos / 1_000_000.0)
        } else {
            format!("{:.2} s", nanos / 1_000_000_000.0)
        }
    }

    fn calculate_stats(latencies: &[Duration]) -> (String, String, String, String, String, u64) {
        if latencies.is_empty() {
            return (
                "N/A".to_string(),
                "N/A".to_string(),
                "N/A".to_string(),
                "N/A".to_string(),
                "N/A".to_string(),
                0,
            );
        }

        let count = latencies.len() as u64;

        // Handle case with very few samples
        if count < 2 {
            let value = latencies[0].as_nanos() as f64;
            return (
                Self::format_duration(value),
                Self::format_duration(value),
                Self::format_duration(value),
                Self::format_duration(value),
                Self::format_duration(value),
                count,
            );
        }

        // Calculate average using streaming method to avoid overflow
        let mut avg = 0.0;
        for (i, d) in latencies.iter().enumerate() {
            let delta = (d.as_nanos() as f64 - avg) / (i + 1) as f64;
            avg += delta;
        }

        // Sort for percentiles
        let mut sorted: Vec<_> = latencies.to_vec();
        sorted.sort_unstable();

        // Calculate percentile indices more accurately
        let p50_idx = ((count - 1) * 50 / 100) as usize;
        let p95_idx = ((count - 1) * 95 / 100) as usize;
        let p99_idx = ((count - 1) * 99 / 100) as usize;

        // Get values
        let min = sorted[0].as_nanos() as f64;
        let max = sorted[sorted.len() - 1].as_nanos() as f64;
        let p50 = sorted[p50_idx].as_nanos() as f64;
        let p95 = sorted[p95_idx].as_nanos() as f64;
        let p99 = sorted[p99_idx].as_nanos() as f64;

        // Calculate throughput
        let throughput = count as f64 / (sorted.iter().sum::<Duration>().as_secs_f64());

        // Include throughput in display
        println!("Throughput: {:.2} ops/sec", throughput);

        (
            Self::format_duration(min),
            Self::format_duration(max),
            Self::format_duration(p50),
            Self::format_duration(p95),
            Self::format_duration(p99),
            count,
        )
    }
}

impl Display for Metrics {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\nDetailed Latency Statistics:")?;
        writeln!(
            f,
            "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
            "Operation", "Count", "Min", "Max", "Median", "P95", "P99"
        )?;
        writeln!(f, "{:-<87}", "")?;

        if !self.read_latencies.is_empty() {
            let (min, max, p50, p95, p99, count) = Self::calculate_stats(&self.read_latencies);
            writeln!(
                f,
                "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
                "Reads", count, min, max, p50, p95, p99
            )?;
        }

        if !self.update_latencies.is_empty() {
            let (min, max, p50, p95, p99, count) = Self::calculate_stats(&self.update_latencies);
            writeln!(
                f,
                "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
                "Updates", count, min, max, p50, p95, p99
            )?;
        }

        if !self.insert_latencies.is_empty() {
            let (min, max, p50, p95, p99, count) = Self::calculate_stats(&self.insert_latencies);
            writeln!(
                f,
                "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
                "Inserts", count, min, max, p50, p95, p99
            )?;
        }

        if !self.scan_latencies.is_empty() {
            let (min, max, p50, p95, p99, count) = Self::calculate_stats(&self.scan_latencies);
            writeln!(
                f,
                "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
                "Scans", count, min, max, p50, p95, p99
            )?;
        }

        if !self.read_modify_write_latencies.is_empty() {
            let (min, max, p50, p95, p99, count) =
                Self::calculate_stats(&self.read_modify_write_latencies);
            writeln!(
                f,
                "{:<15} {:<12} {:<12} {:<12} {:<12} {:<12} {:<12}",
                "Read-Modify", count, min, max, p50, p95, p99
            )?;
        }

        Ok(())
    }
}
