use axum::{
    http::StatusCode,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use serde_json::{json, Value};
use std::{convert::Infallible, time::Duration};

async fn completion(Json(payload): Json<Value>) -> Response {
    let name = std::env::var("BACKEND_NAME").unwrap_or_else(|_| "A".into());
    if std::env::var("FAIL_MODE").as_deref() == Ok("always") {
        return (StatusCode::INTERNAL_SERVER_ERROR, "simulated model failure").into_response();
    }

    if payload.get("stream").and_then(Value::as_bool) == Some(true) {
        let events = async_stream::stream! {
            for word in ["Hello", " from", " model", " ", name.as_str()] {
                tokio::time::sleep(Duration::from_millis(120)).await;
                let part = json!({
                    "choices":[{"index":0,"delta":{"content":word},"finish_reason":null}]
                });
                yield Ok::<Event, Infallible>(Event::default().data(part.to_string()));
            }
            yield Ok::<Event, Infallible>(Event::default().data("[DONE]"));
        };
        Sse::new(events).into_response()
    } else {
        tokio::time::sleep(Duration::from_millis(200)).await;
        Json(json!({
            "model":"mock-llm",
            "choices":[{"message":{"role":"assistant","content":format!("Hello from model {name}")}}]
        }))
        .into_response()
    }
}

#[tokio::main]
async fn main() {
    let port = std::env::var("PORT").unwrap_or_else(|_| "9000".into());
    let name = std::env::var("BACKEND_NAME").unwrap_or_else(|_| "A".into());
    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/chat/completions", post(completion));
    let host = std::env::var("BIND_HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}"))
        .await
        .expect("bind mock backend");
    println!("Model {name} listening on {host}:{port}");
    axum::serve(listener, app)
        .await
        .expect("serve mock backend");
}
