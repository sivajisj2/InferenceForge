#!/usr/bin/env bash
# Live black-box checks against Docker Compose or a port-forward.
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:8007}"
KEY="${GATEWAY_API_KEY:-local-demo-key}"
BODY='{"model":"mock-llm","messages":[{"role":"user","content":"smoke"}]}'
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/health")
[[ "$code" == 200 ]] || { echo "health expected 200, got $code"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H 'Content-Type: application/json' -d "$BODY" "$BASE/v1/chat/completions")
[[ "$code" == 401 ]] || { echo "unauthenticated expected 401, got $code"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d "$BODY" "$BASE/v1/chat/completions")
[[ "$code" == 200 ]] || { echo "authenticated expected 200, got $code"; exit 1; }
code=$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' -d '{"model":"unknown","messages":[{"role":"user","content":"x"}]}' "$BASE/v1/chat/completions")
[[ "$code" == 400 ]] || { echo "invalid model expected 400, got $code"; exit 1; }
curl -fsS "$BASE/metrics" | grep -q 'ai_requests_total' || { echo 'missing metrics'; exit 1; }
echo 'PASS: health, auth, successful inference, input validation, metrics'
