use axum::{
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures_util::StreamExt;
use prometheus::{Encoder, TextEncoder};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use tracing::{info, info_span, warn, Instrument};
use tracing_subscriber::EnvFilter;

#[path = "../gateway/circuit.rs"]
mod circuit;
use circuit::Backend;

/// Per-instance token bucket: use a shared Redis/edge limiter for global quotas.
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
    rate_per_sec: f64,
    capacity: f64,
}

impl TokenBucket {
    fn new(rate_per_sec: f64, capacity: f64) -> Self {
        Self {
            tokens: capacity,
            last_refill: Instant::now(),
            rate_per_sec,
            capacity,
        }
    }

    fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.capacity);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .ok()
            .filter(|&n| n > 0 || name == "MAX_WAITING")
            .unwrap_or_else(|| panic!("invalid numeric setting {name}")),
        Err(_) => default,
    }
}

#[path = "../gateway/metrics.rs"]
mod telemetry;
use telemetry::{Metrics, QueueGaugeGuard, RequestGuard};

struct AppState {
    client: reqwest::Client,
    backends: Vec<Arc<Backend>>,
    next: AtomicUsize,
    model: String,
    upstream_key: Option<String>,
    header_timeout: Duration,
    body_timeout: Duration,
    generation_budget: Duration,
    stream_idle: Duration,
    capacity: usize,
    request_seq: AtomicU64,
    instance: String,
    api_key: String,
    permits: Arc<Semaphore>,
    queue_slots: Arc<Semaphore>,
    queue_wait: Duration,
    bucket: Mutex<TokenBucket>,
    accepting: AtomicBool,
    metrics: Arc<Metrics>,
}

fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(json!({"error": message}))).into_response()
}

async fn health() -> &'static str {
    "ok"
}

async fn ready(State(state): State<Arc<AppState>>) -> Response {
    if state.accepting.load(Ordering::Acquire) {
        (StatusCode::OK, "ready").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "draining").into_response()
    }
}

fn overloaded(status: StatusCode, message: &str) -> Response {
    let mut response = error(status, message);
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
    response
}

async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    for backend in &state.backends {
        state
            .metrics
            .circuit_open
            .with_label_values(&[backend.name])
            .set(i64::from(matches!(backend.status().0, "open" | "probing")));
    }
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    if encoder
        .encode(&state.metrics.registry.gather(), &mut buffer)
        .is_err()
    {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "metrics encoding failed");
    }
    ([(header::CONTENT_TYPE, encoder.format_type())], buffer).into_response()
}

async fn backends(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({
        "model": state.model,
        "accepting": state.accepting.load(Ordering::Acquire),
        "active": state.metrics.in_flight.get(),
        "capacity": state.capacity,
        "queued": state.metrics.queued.get(),
        "failovers": state.metrics.failovers.get(),
        "admitted": state.metrics.admissions.with_label_values(&["accepted"]).get(),
        "backends": state.backends.iter().map(|b| {
            let (circuit, failures) = b.status();
            json!({"name": b.name, "failures": failures, "circuit": circuit,
                "available": matches!(circuit, "closed" | "probe-ready"),
                "accepted_headers": state.metrics.attempts.with_label_values(&[b.name, "success"]).get()})
        }).collect::<Vec<_>>()
    }))
}

// Authentication is deliberately enforced before admission or forwarding. Health and
// metrics remain unauthenticated; expose those endpoints only on trusted networks.
fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(raw) = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    else {
        return false;
    };
    let Some(token) = raw.strip_prefix("Bearer ") else {
        return false;
    };
    use subtle::ConstantTimeEq;
    bool::from(token.as_bytes().ct_eq(expected.as_bytes()))
}

async fn authenticate(
    State(state): State<Arc<AppState>>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    if !authorized(req.headers(), &state.api_key) {
        state.metrics.record("unauthorized");
        return error(StatusCode::UNAUTHORIZED, "unauthorized");
    }
    next.run(req).await
}

async fn chat(State(state): State<Arc<AppState>>, Json(payload): Json<Value>) -> Response {
    let request_id = format!(
        "{}-{}",
        state.instance,
        state.request_seq.fetch_add(1, Ordering::Relaxed)
    );
    let stream_requested = payload.get("stream").and_then(Value::as_bool) == Some(true);
    let span = info_span!("inference_request", request_id = %request_id, stream = stream_requested);
    let mut response = chat_inner(state, payload, stream_requested)
        .instrument(span)
        .await;
    if let Ok(value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert("x-request-id", value);
    }
    response
}

async fn chat_inner(state: Arc<AppState>, payload: Value, stream_requested: bool) -> Response {
    if payload.get("model").and_then(Value::as_str) != Some(state.model.as_str())
        || !payload
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|m| {
                !m.is_empty()
                    && m.iter().all(|item| {
                        matches!(item["role"].as_str(), Some("system" | "user" | "assistant"))
                            && item["content"]
                                .as_str()
                                .is_some_and(|v| !v.trim().is_empty())
                    })
            })
        || payload.get("stream").is_some_and(|v| !v.is_boolean())
    {
        state.metrics.record("invalid");
        return error(StatusCode::BAD_REQUEST, "invalid model or messages");
    }

    if !state.accepting.load(Ordering::Acquire) {
        state.metrics.record("draining");
        return overloaded(StatusCode::SERVICE_UNAVAILABLE, "gateway draining");
    }

    // Admission order: rate limit, then concurrency, then a bounded waiting room.
    // The token bucket is local to this gateway replica, not a cluster-wide quota.
    if !state.bucket.lock().expect("bucket lock").allow() {
        state.metrics.record("rate_limited");
        state
            .metrics
            .admissions
            .with_label_values(&["rate_limited"])
            .inc();
        return overloaded(StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded");
    }

    let permit = match state.permits.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            let waiting_slot = match state.queue_slots.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    state.metrics.record("queue_full");
                    state
                        .metrics
                        .admissions
                        .with_label_values(&["queue_full"])
                        .inc();
                    return overloaded(StatusCode::SERVICE_UNAVAILABLE, "gateway queue full");
                }
            };
            let queue_guard = QueueGaugeGuard::new(state.metrics.clone());
            let waited =
                tokio::time::timeout(state.queue_wait, state.permits.clone().acquire_owned()).await;
            drop(queue_guard);
            drop(waiting_slot);
            match waited {
                Ok(Ok(p)) => p,
                _ => {
                    state.metrics.record("queue_timeout");
                    state
                        .metrics
                        .admissions
                        .with_label_values(&["queue_timeout"])
                        .inc();
                    return overloaded(StatusCode::SERVICE_UNAVAILABLE, "gateway queue timeout");
                }
            }
        }
    };
    if !state.accepting.load(Ordering::Acquire) {
        drop(permit);
        state.metrics.record("draining");
        return overloaded(StatusCode::SERVICE_UNAVAILABLE, "gateway draining");
    }
    state
        .metrics
        .admissions
        .with_label_values(&["accepted"])
        .inc();
    let guard = RequestGuard::new(
        state.metrics.clone(),
        if stream_requested {
            "stream"
        } else {
            "nonstream"
        },
        permit,
    );
    let deadline = tokio::time::Instant::now() + state.generation_budget;
    let count = state.backends.len();
    let start = state.next.fetch_add(1, Ordering::Relaxed) % count;

    for offset in 0..count {
        let backend = &state.backends[(start + offset) % count];
        let Some(attempt) = backend.acquire() else {
            info!(backend = backend.name, "backend skipped: circuit open");
            continue;
        };

        let url = format!("{}/v1/chat/completions", backend.url);
        let mut request = state.client.post(url).json(&payload);
        if let Some(key) = &state.upstream_key {
            request = request.bearer_auth(key);
        }
        let sent = tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + state.header_timeout),
            request.send(),
        )
        .await;

        let upstream = match sent {
            Ok(Ok(r)) if r.status().is_success() => {
                state.metrics.attempt(backend.name, "success");
                r
            }
            Ok(Ok(r)) if r.status().is_server_error() => {
                state.metrics.attempt(backend.name, "http_5xx");
                attempt.finish(false);
                warn!(backend = backend.name, status = %r.status(), "backend server error");
                state.metrics.record("upstream_error");
                return error(
                    StatusCode::BAD_GATEWAY,
                    "backend server error; not replayed",
                );
            }
            Ok(Ok(r)) => {
                state.metrics.attempt(backend.name, "rejected");
                if r.status().is_client_error() {
                    attempt.finish(true);
                }
                state.metrics.record("upstream_error");
                warn!(backend = backend.name, status = %r.status(), "unexpected upstream status");
                return error(
                    StatusCode::BAD_GATEWAY,
                    &format!("upstream returned {}", r.status()),
                );
            }
            Ok(Err(e)) => {
                state.metrics.attempt(backend.name, "transport_error");
                attempt.finish(false);
                warn!(backend = backend.name, error = %e, "backend unavailable");
                // Connect failures are retryable; other transport errors may have
                // reached the model and must not be replayed without idempotency.
                if e.is_connect() {
                    continue;
                }
                state.metrics.record("upstream_error");
                return error(
                    StatusCode::BAD_GATEWAY,
                    "upstream transport failure; not replayed",
                );
            }
            Err(_) => {
                state.metrics.attempt(backend.name, "timeout");
                attempt.finish(false);
                warn!(backend = backend.name, "backend response headers timed out");
                state.metrics.record("upstream_timeout");
                return error(StatusCode::GATEWAY_TIMEOUT, "backend timeout; not replayed");
            }
        };

        info!(
            backend = backend.name,
            failover = offset > 0,
            "backend selected"
        );
        if offset > 0 {
            state.metrics.failovers.inc();
        }

        let mut headers = HeaderMap::new();
        headers.insert("x-backend", HeaderValue::from_static(backend.name));
        headers.insert(
            "x-failover",
            HeaderValue::from_static(if offset > 0 { "true" } else { "false" }),
        );

        if stream_requested {
            let is_sse = upstream
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("text/event-stream"));
            if !is_sse {
                attempt.finish(false);
                state.metrics.record("upstream_error");
                return error(StatusCode::BAD_GATEWAY, "upstream did not return SSE");
            }
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/event-stream"),
            );
            attempt.finish(true);
            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static("no-cache, no-transform"),
            );
            state.metrics.record("stream_started");
            let metrics = state.metrics.clone();
            let body_stream = async_stream::stream! {
                let _guard = guard;
                let mut chunks = upstream.bytes_stream();
                loop {
                    match tokio::time::timeout_at(deadline.min(tokio::time::Instant::now() + state.stream_idle), chunks.next()).await {
                        Ok(Some(Ok(bytes))) if tokio::time::Instant::now() < deadline => yield Ok::<_, std::io::Error>(bytes),
                        Ok(Some(Err(e))) => {
                            metrics.stream_errors.inc();
                            warn!(error = %e, "stream forwarding failed");
                            yield Err(std::io::Error::other(e));
                            break;
                        }
                        Ok(None) => break,
                        Ok(Some(Ok(_))) | Err(_) => {
                            metrics.stream_errors.inc();
                            warn!("stream deadline exceeded");
                            yield Err(std::io::Error::other("stream deadline exceeded"));
                            break;
                        }
                    }
                }
            };
            return (headers, Body::from_stream(body_stream)).into_response();
        }

        // Separate body deadline. SSE streams instead have an idle-chunk timeout.
        let result = tokio::time::timeout_at(
            deadline.min(tokio::time::Instant::now() + state.body_timeout),
            read_json(upstream),
        )
        .await;
        drop(guard);
        return match result {
            Ok(Ok(bytes)) => {
                attempt.finish(true);
                state.metrics.record("success");
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                (headers, bytes).into_response()
            }
            Ok(Err(e)) => {
                attempt.finish(false);
                state.metrics.record("upstream_error");
                warn!(error = %e, "upstream body failed");
                error(StatusCode::BAD_GATEWAY, "upstream body failed")
            }
            Err(_) => {
                attempt.finish(false);
                state.metrics.record("upstream_timeout");
                error(
                    StatusCode::GATEWAY_TIMEOUT,
                    "upstream body deadline exceeded",
                )
            }
        };
    }

    state.metrics.record("unavailable");
    warn!("no healthy backend available");
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "no healthy backend available",
    )
}

async fn read_json(response: reqwest::Response) -> Result<Vec<u8>, &'static str> {
    let mut chunks = response.bytes_stream();
    let mut body = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let bytes = chunk.map_err(|_| "response interrupted")?;
        if bytes.len() > (2 * 1024 * 1024) - body.len() {
            return Err("response too large");
        }
        body.extend_from_slice(&bytes);
    }
    serde_json::from_slice::<Value>(&body).map_err(|_| "invalid upstream JSON")?;
    Ok(body)
}

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    if std::env::var("LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }

    let first = std::env::var("BACKEND_A").unwrap_or_else(|_| "http://127.0.0.1:9000".into());
    let second = std::env::var("BACKEND_B").unwrap_or_else(|_| "http://127.0.0.1:9001".into());
    let instance = std::env::var("HOSTNAME").unwrap_or_else(|_| "gateway".into());
    // Refuse to boot with an unauthenticated inference API.
    let api_key =
        std::env::var("GATEWAY_API_KEY").expect("set GATEWAY_API_KEY (use a secret in production)");
    assert!(!api_key.is_empty(), "GATEWAY_API_KEY must not be empty");
    let mut workers = vec![Arc::new(Backend::new("A", first))];
    if !second.is_empty() {
        workers.push(Arc::new(Backend::new("B", second)));
    }
    let capacity = env_usize("MAX_INFLIGHT", 16);
    let state = Arc::new(AppState {
        capacity,
        generation_budget: Duration::from_millis(env_usize("GENERATION_BUDGET_MS", 120000) as u64),
        header_timeout: Duration::from_millis(env_usize("HEADER_TIMEOUT_MS", 5000) as u64),
        body_timeout: Duration::from_millis(env_usize("BODY_TIMEOUT_MS", 30000) as u64),
        stream_idle: Duration::from_millis(env_usize("STREAM_IDLE_MS", 10000) as u64),
        model: std::env::var("MODEL_NAME").unwrap_or_else(|_| "mock-llm".into()),
        upstream_key: std::env::var("UPSTREAM_API_KEY")
            .ok()
            .filter(|k| !k.is_empty()),
        client: reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(Duration::from_millis(250))
            .build()
            .expect("client"),
        backends: workers,
        next: AtomicUsize::new(0),
        request_seq: AtomicU64::new(1),
        instance,
        api_key,
        permits: Arc::new(Semaphore::new(capacity)),
        queue_slots: Arc::new(Semaphore::new(env_usize("MAX_WAITING", 8))),
        queue_wait: Duration::from_millis(env_usize("QUEUE_WAIT_MS", 150) as u64),
        bucket: Mutex::new(TokenBucket::new(
            env_usize("RATE_LIMIT_RPS", 100) as f64,
            env_usize("RATE_LIMIT_BURST", 30) as f64,
        )),
        accepting: AtomicBool::new(true),
        metrics: Arc::new(Metrics::new()),
    });
    let app = Router::new()
        .route(
            "/",
            get(|| async { Html(include_str!("../../ui/index.html")) }),
        )
        .route(
            "/console.js",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/javascript")],
                    include_str!("../../ui/console.js"),
                )
            }),
        )
        .route(
            "/console.css",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "text/css")],
                    include_str!("../../ui/console.css"),
                )
            }),
        )
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/backends", get(backends))
        .route(
            "/v1/chat/completions",
            post(chat).route_layer(axum::middleware::from_fn_with_state(
                state.clone(),
                authenticate,
            )),
        )
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind(
        std::env::var("LISTEN_ADDR").unwrap_or_else(|_| "0.0.0.0:8001".into()),
    )
    .await
    .expect("bind router");
    info!(address = %listener.local_addr().expect("listener address"), "gateway ready");
    // The shutdown timer must start AFTER SIGTERM, not at server startup.
    // A watchdog caps draining; Kubernetes terminationGracePeriodSeconds is 30s.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let shutdown = async move {
        shutdown_signal(state).await;
        let _ = shutdown_tx.send(());
    };
    let server = axum::serve(listener, app).with_graceful_shutdown(shutdown);
    let watchdog = async move {
        if shutdown_rx.await.is_ok() {
            tokio::time::sleep(Duration::from_secs(20)).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        result = std::future::IntoFuture::into_future(server) => {
            if let Err(error) = result { tracing::error!(%error, "server failed"); }
        }
        _ = watchdog => {
            warn!("20-second drain deadline exceeded; closing remaining streams");
        }
    }
}

async fn shutdown_signal(state: Arc<AppState>) {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .expect("install Ctrl-C handler");
    state.accepting.store(false, Ordering::Release);
    info!("shutdown requested; readiness disabled and draining requests");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_rejects_missing_or_incorrect_tokens() {
        let headers = HeaderMap::new();
        assert!(!authorized(&headers, "secret"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer wrong"),
        );
        assert!(!authorized(&headers, "secret"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic secret"),
        );
        assert!(!authorized(&headers, "secret"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret"),
        );
        assert!(authorized(&headers, "secret"));
    }

    #[test]
    fn token_bucket_enforces_burst_capacity() {
        let mut bucket = TokenBucket::new(0.00001, 2.0);
        assert!(bucket.allow());
        assert!(bucket.allow());
        assert!(!bucket.allow());
    }

    #[tokio::test]
    async fn bounded_queue_permits_are_released() {
        let permits = Arc::new(Semaphore::new(1));
        let first = permits.clone().try_acquire_owned().unwrap();
        assert!(permits.clone().try_acquire_owned().is_err());
        drop(first);
        assert!(permits.try_acquire_owned().is_ok());
    }

    #[test]
    fn metrics_encode_in_prometheus_format() {
        let m = Metrics::new();
        m.record("success");
        m.attempt("A", "timeout");
        let mut buffer = Vec::new();
        TextEncoder::new()
            .encode(&m.registry.gather(), &mut buffer)
            .unwrap();
        let text = String::from_utf8(buffer).unwrap();
        assert!(text.contains("ai_requests_total{result=\"success\"} 1"));
        assert!(text.contains("ai_backend_attempts_total{backend=\"A\",outcome=\"timeout\"} 1"));
    }
}
