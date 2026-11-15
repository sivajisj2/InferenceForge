#!/usr/bin/env bash
# Run against the local Compose demo. Restart the worker on any exit.
set -euo pipefail
cd "$(dirname "$0")/.."
base="${BASE:-http://127.0.0.1:8007}"
key="${GATEWAY_API_KEY:-local-demo-key}"
scratch=$(mktemp -d)
cleanup() { docker compose start model-a >/dev/null; rm -rf "$scratch"; }
trap cleanup EXIT
docker compose stop model-a >/dev/null
chat() {
  curl -fsS --max-time 10 -D "$scratch/headers" -o "$scratch/body" \
    -H "Authorization: Bearer $key" -H 'Content-Type: application/json' \
    -d '{"model":"mock-llm","messages":[{"role":"user","content":"test"}]}' "$base/v1/chat/completions"
}
for _ in {1..6}; do chat; grep -qi '^x-backend: B' "$scratch/headers"; done
curl -fsS "$base/backends" | python3 -c 'import sys,json; assert json.load(sys.stdin)["backends"][0]["circuit"] == "open"'
docker compose start model-a >/dev/null
for _ in {1..24}; do
  chat
  if grep -qi '^x-backend: A' "$scratch/headers"; then echo 'PASS: worker B served through the outage and A rejoined'; exit 0; fi
  sleep 1
done
echo 'Worker A failed to recover' >&2
exit 1
