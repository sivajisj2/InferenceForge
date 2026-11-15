#!/usr/bin/env bash
# Fetch one pinned GGUF and verify it before making it available to the worker.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p models
target=models/SmolLM2-135M-Instruct-Q4_K_M.gguf
digest=2e8040ceae7815abe0dcb3540b9995eaa1fa0d2ca9e797d0a635ae4433c68c2d
verify() { printf '%s  %s\n' "$digest" "$1" | sha256sum --check --status; }
if [[ -f "$target" ]] && verify "$target"; then echo "$target"; exit 0; fi
temp=$(mktemp models/.weights.XXXXXX)
trap 'rm -f "$temp"' EXIT
curl -fL --retry 3 --output "$temp" 'https://huggingface.co/bartowski/SmolLM2-135M-Instruct-GGUF/resolve/09816acd5d99df7be770d85ea30822623dab342c68c2d/SmolLM2-135M-Instruct-Q4_K_M.gguf'
verify "$temp"
mv "$temp" "$target"
echo "$target"
