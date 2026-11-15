#!/usr/bin/env bash
# Run the console and two deterministic workers on loopback.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo build --locked --bins
export GATEWAY_API_KEY="${GATEWAY_API_KEY:-local-demo-key}"
export BIND_HOST=127.0.0.1
export LISTEN_ADDR="127.0.0.1:${GATEWAY_PORT:-8007}"
export MAX_INFLIGHT="${MAX_INFLIGHT:-4}" MAX_WAITING="${MAX_WAITING:-2}"
export BACKEND_A=http://127.0.0.1:9000 BACKEND_B=http://127.0.0.1:9001 MODEL_NAME=mock-llm
bin="${CARGO_TARGET_DIR:-target}/debug"
pids=()
cleanup() {
  trap - EXIT INT TERM
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  wait || true
}
trap cleanup EXIT
trap 'exit 0' INT TERM
PORT=9000 BACKEND_NAME=A "$bin/mock_worker" & pids+=("$!")
PORT=9001 BACKEND_NAME=B "$bin/mock_worker" & pids+=("$!")
"$bin/inferenceforge" & pids+=("$!")
echo "Console: http://$LISTEN_ADDR · Ctrl-C to stop"
wait -n
