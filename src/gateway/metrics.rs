use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::{sync::Arc, time::Instant};
use tokio::sync::OwnedSemaphorePermit;

pub(super) struct Metrics {
    pub(super) registry: Registry,
    pub(super) requests: IntCounterVec,
    pub(super) attempts: IntCounterVec,
    pub(super) failovers: IntCounter,
    pub(super) in_flight: IntGauge,
    pub(super) duration: HistogramVec,
    pub(super) circuit_open: IntGaugeVec,
    pub(super) stream_errors: IntCounter,
    pub(super) queued: IntGauge,
    pub(super) admissions: IntCounterVec,
}

impl Metrics {
    pub(super) fn new() -> Self {
        let registry = Registry::new();
        let requests = IntCounterVec::new(
            Opts::new("ai_requests_total", "Gateway requests by result"),
            &["result"],
        )
        .expect("requests metric");
        let attempts = IntCounterVec::new(
            Opts::new("ai_backend_attempts_total", "Backend attempts by result"),
            &["backend", "outcome"],
        )
        .expect("attempts metric");
        let failovers = IntCounter::new("ai_failovers_total", "Successful failovers")
            .expect("failovers metric");
        let in_flight = IntGauge::new("ai_inflight_requests", "Requests holding a gateway permit")
            .expect("inflight metric");
        let duration = HistogramVec::new(
            HistogramOpts::new("ai_request_duration_seconds", "Gateway request duration")
                .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
            &["mode"],
        )
        .expect("duration metric");
        let circuit_open = IntGaugeVec::new(
            Opts::new(
                "ai_circuit_open",
                "1 when a backend circuit is open, 0 otherwise",
            ),
            &["backend"],
        )
        .expect("circuit metric");
        let stream_errors = IntCounter::new(
            "ai_stream_errors_total",
            "Errors while forwarding SSE bodies",
        )
        .expect("stream errors metric");
        let queued = IntGauge::new(
            "ai_queued_requests",
            "Requests waiting for a concurrency permit",
        )
        .expect("queued metric");
        let admissions = IntCounterVec::new(
            Opts::new("ai_admissions_total", "Admission outcomes"),
            &["outcome"],
        )
        .expect("admissions metric");
        registry
            .register(Box::new(queued.clone()))
            .expect("register queued");
        registry
            .register(Box::new(admissions.clone()))
            .expect("register admissions");
        registry
            .register(Box::new(requests.clone()))
            .expect("register requests");
        registry
            .register(Box::new(attempts.clone()))
            .expect("register attempts");
        registry
            .register(Box::new(failovers.clone()))
            .expect("register failovers");
        registry
            .register(Box::new(in_flight.clone()))
            .expect("register inflight");
        registry
            .register(Box::new(duration.clone()))
            .expect("register duration");
        registry
            .register(Box::new(circuit_open.clone()))
            .expect("register circuit");
        registry
            .register(Box::new(stream_errors.clone()))
            .expect("register stream errors");
        Self {
            registry,
            requests,
            attempts,
            failovers,
            in_flight,
            duration,
            circuit_open,
            stream_errors,
            queued,
            admissions,
        }
    }

    pub(super) fn record(&self, result: &'static str) {
        self.requests.with_label_values(&[result]).inc();
    }

    pub(super) fn attempt(&self, backend: &'static str, outcome: &'static str) {
        self.attempts.with_label_values(&[backend, outcome]).inc();
    }
}

// RAII: even on client disconnect, a streaming request releases its permit and
// updates its duration/in-flight metrics when the response body is dropped.
pub(super) struct RequestGuard {
    metrics: Arc<Metrics>,
    started: Instant,
    mode: &'static str,
    _permit: OwnedSemaphorePermit,
}

impl RequestGuard {
    pub(super) fn new(
        metrics: Arc<Metrics>,
        mode: &'static str,
        permit: OwnedSemaphorePermit,
    ) -> Self {
        metrics.in_flight.inc();
        Self {
            metrics,
            started: Instant::now(),
            mode,
            _permit: permit,
        }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.metrics.in_flight.dec();
        self.metrics
            .duration
            .with_label_values(&[self.mode])
            .observe(self.started.elapsed().as_secs_f64());
    }
}

// Ensure queued metric decrements even if a client disconnects while waiting.
pub(super) struct QueueGaugeGuard(Arc<Metrics>);
impl QueueGaugeGuard {
    pub(super) fn new(metrics: Arc<Metrics>) -> Self {
        metrics.queued.inc();
        Self(metrics)
    }
}
impl Drop for QueueGaugeGuard {
    fn drop(&mut self) {
        self.0.queued.dec();
    }
}
