# Run a real CPU model

The gateway was exercised with **llama.cpp b12403** and **SmolLM2-135M-Instruct Q4_K_M**. The model runs on the CPU. JSON and SSE use the same gateway endpoint and admission controls as the two-worker demo.

## Start the worker

Install the [b12403 llama.cpp release](https://github.com/ggml-org/llama.cpp/releases/tag/b12403) for your platform and put its `llama-server` executable on PATH. The verified setup used the Ubuntu x64 release and two CPU threads. Fetch the model from the repository root. The script requires curl and sha256sum.

```bash
bash scripts/fetch_model.sh
llama-server -m models/SmolLM2-135M-Instruct-Q4_K_M.gguf \
  --alias smollm2 --host 127.0.0.1 --port 9123 -t 2 -c 512
```

The download is approximately 101 MiB. The [quantized model repository](https://huggingface.co/bartowski/SmolLM2-135M-Instruct-GGUF/tree/09816acd5d99df7be770d85ea30822623dab342c) and SHA-256 digest are pinned in the script. Weights stay outside git and the container build context.

## Route through the gateway

In another terminal, run:

```bash
GATEWAY_API_KEY=local-demo-key MODEL_NAME=smollm2 \
  BACKEND_A=http://127.0.0.1:9123 BACKEND_B='' \
  LISTEN_ADDR=127.0.0.1:8123 cargo run --locked --bin inferenceforge
```

Open the console at http://127.0.0.1:8123. The empty second URL selects a single worker. To test from the shell:

```bash
curl -sS http://127.0.0.1:8123/v1/chat/completions \
  -H 'Authorization: Bearer local-demo-key' -H 'Content-Type: application/json' \
  -d '{"model":"smollm2","temperature":0,"max_tokens":24,"messages":[{"role":"user","content":"What is the capital of France?"}]}'
```

The observed JSON response contained **“The capital of France is Paris.”** The worker reported 37 prompt tokens and 8 completion tokens. The streamed version emitted content deltas followed by `data: [DONE]`. The [observed outputs and usage](evidence/cpu-inference.json) record both responses. These are single-request functional checks. Use a representative workload to measure serving performance.

## Connect a different engine

Set `MODEL_NAME` to the served model identifier and `BACKEND_A` to the server origin, without a trailing slash or `/v1`. The gateway appends `/v1/chat/completions`. Set `UPSTREAM_API_KEY` if the upstream requires bearer authentication. Two configured workers must serve the same model. A server offering this text-chat contract can be integrated independently of its hardware. GPU and vLLM deployments require separate validation.
