# Multi-stage Rust build; non-root runtime and health probe.
FROM rust:1-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY ui ./ui
RUN cargo build --locked --release --bin inferenceforge --bin mock_worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home appuser
COPY --from=builder /app/target/release/inferenceforge /usr/local/bin/inferenceforge
COPY --from=builder /app/target/release/mock_worker /usr/local/bin/mock_worker
USER appuser
EXPOSE 8001 9000 9001
CMD ["inferenceforge"]
