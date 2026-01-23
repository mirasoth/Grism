//! Metrics collection for query execution.

#![allow(clippy::let_underscore_lock)] // Some locks need to be held for the full scope
#![allow(clippy::significant_drop_tightening)] // Guards must stay alive for their scope

use std::collections::HashMap;
use std::fmt::Write;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Metrics for a single operator execution.
#[derive(Debug, Clone, Default)]
pub struct OperatorMetrics {
    /// Number of input rows processed.
    pub rows_in: u64,
    /// Number of output rows produced.
    pub rows_out: u64,
    /// Total execution time.
    pub exec_time: Duration,
    /// Peak memory usage in bytes.
    pub memory_bytes: usize,
    /// Number of batches processed.
    pub batches: u64,
}

impl OperatorMetrics {
    /// Create new metrics.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            rows_in: 0,
            rows_out: 0,
            exec_time: Duration::new(0, 0),
            memory_bytes: 0,
            batches: 0,
        }
    }

    /// Add rows processed.
    pub fn add_rows_in(&mut self, count: usize) {
        self.rows_in += count as u64;
    }

    /// Add rows produced.
    pub fn add_rows_out(&mut self, count: usize) {
        self.rows_out += count as u64;
    }

    /// Add execution time.
    pub fn add_time(&mut self, duration: Duration) {
        self.exec_time += duration;
    }

    /// Increment batch count.
    pub fn add_batch(&mut self) {
        self.batches += 1;
    }

    /// Update peak memory.
    pub fn update_memory(&mut self, bytes: usize) {
        self.memory_bytes = self.memory_bytes.max(bytes);
    }

    /// Get selectivity (`rows_out` / `rows_in`).
    pub fn selectivity(&self) -> f64 {
        if self.rows_in == 0 {
            1.0
        } else {
            self.rows_out as f64 / self.rows_in as f64
        }
    }

    /// Get throughput (`rows_in` / `exec_time`).
    pub fn throughput(&self) -> f64 {
        let secs = self.exec_time.as_secs_f64();
        if secs == 0.0 {
            0.0
        } else {
            self.rows_in as f64 / secs
        }
    }
}

impl std::fmt::Display for OperatorMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rows_in={}, rows_out={}, time={:?}, memory={}B, batches={}",
            self.rows_in, self.rows_out, self.exec_time, self.memory_bytes, self.batches
        )
    }
}

/// Sink for collecting operator metrics.
#[derive(Debug, Clone, Default)]
pub struct MetricsSink {
    metrics: Arc<RwLock<HashMap<String, OperatorMetrics>>>,
}

impl MetricsSink {
    /// Create a new metrics sink.
    #[must_use]
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Record metrics for an operator.
    pub fn record(&self, operator_id: &str, metrics: OperatorMetrics) {
        self.metrics
            .write()
            .expect("metrics lock poisoned")
            .insert(operator_id.to_string(), metrics);
    }

    /// Update metrics for an operator using a closure.
    #[allow(clippy::let_underscore_lock)] // Guard must stay alive for the scope
    pub fn update<F>(&self, operator_id: &str, f: F)
    where
        F: FnOnce(&mut OperatorMetrics),
    {
        let mut guard = self.metrics.write().expect("metrics lock poisoned");
        let metrics = guard.entry(operator_id.to_string()).or_default();
        f(metrics);
    }

    /// Get metrics for an operator.
    pub fn get(&self, operator_id: &str) -> Option<OperatorMetrics> {
        self.metrics
            .read()
            .expect("metrics lock poisoned")
            .get(operator_id)
            .cloned()
    }

    /// Get all metrics.
    pub fn all(&self) -> HashMap<String, OperatorMetrics> {
        self.metrics.read().expect("metrics lock poisoned").clone()
    }

    /// Clear all metrics.
    pub fn clear(&self) {
        self.metrics.write().expect("metrics lock poisoned").clear();
    }

    /// Get total rows processed across all operators.
    pub fn total_rows_in(&self) -> u64 {
        self.metrics
            .read()
            .expect("metrics lock poisoned")
            .values()
            .map(|m| m.rows_in)
            .sum()
    }

    /// Get total execution time across all operators.
    pub fn total_time(&self) -> Duration {
        self.metrics
            .read()
            .expect("metrics lock poisoned")
            .values()
            .map(|m| m.exec_time)
            .sum()
    }

    /// Format metrics for EXPLAIN ANALYZE.
    pub fn format_analyze(&self) -> String {
        let metrics = self.metrics.read().expect("metrics lock poisoned");
        let mut output = String::new();

        for (op, m) in metrics.iter() {
            let _ = writeln!(
                output,
                "{}: rows_in={}, rows_out={}, time={:?}, memory={}B",
                op, m.rows_in, m.rows_out, m.exec_time, m.memory_bytes
            );
        }

        if output.is_empty() {
            output.push_str("No metrics collected.\n");
        }

        output
    }
}

/// Timer for measuring operator execution time.
#[derive(Debug)]
pub struct ExecutionTimer {
    start: Instant,
}

impl ExecutionTimer {
    /// Start a new timer.
    #[must_use]
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time without stopping.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Stop and return elapsed time.
    #[must_use]
    pub fn stop(self) -> Duration {
        self.start.elapsed()
    }
}

impl Default for ExecutionTimer {
    fn default() -> Self {
        Self::start()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_metrics() {
        let mut metrics = OperatorMetrics::new();
        metrics.add_rows_in(1000);
        metrics.add_rows_out(500);
        metrics.add_batch();

        assert_eq!(metrics.rows_in, 1000);
        assert_eq!(metrics.rows_out, 500);
        assert_eq!(metrics.batches, 1);
        assert!((metrics.selectivity() - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_metrics_sink() {
        let sink = MetricsSink::new();

        let mut m1 = OperatorMetrics::new();
        m1.rows_in = 1000;
        m1.rows_out = 900;
        sink.record("FilterExec", m1);

        let mut m2 = OperatorMetrics::new();
        m2.rows_in = 900;
        m2.rows_out = 3;
        sink.record("ProjectExec", m2);

        assert!(sink.get("FilterExec").is_some());
        assert_eq!(sink.total_rows_in(), 1900);
    }

    #[test]
    fn test_metrics_update() {
        let sink = MetricsSink::new();

        sink.update("ScanExec", |m| {
            m.add_rows_in(100);
            m.add_rows_out(100);
        });

        sink.update("ScanExec", |m| {
            m.add_rows_in(50);
            m.add_rows_out(50);
        });

        let metrics = sink.get("ScanExec").unwrap();
        assert_eq!(metrics.rows_in, 150);
    }

    #[test]
    fn test_execution_timer() {
        let timer = ExecutionTimer::start();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = timer.stop();
        assert!(elapsed >= std::time::Duration::from_millis(10));
    }
}
