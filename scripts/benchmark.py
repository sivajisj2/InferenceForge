#!/usr/bin/env python3
"""Closed-loop JSON workload. Counts errors separately from completed generations."""

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import math
import os
import time
import urllib.error
import urllib.request

HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def distribution(samples):
    values = sorted(samples)
    if not values:
        return None
    return {
        f"p{p}": round(values[math.ceil(p * len(values) / 100) - 1], 2)
        for p in (50, 95, 99)
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:8007/v1/chat/completions")
    parser.add_argument("--requests", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=10)
    args = parser.parse_args()
    if min(args.requests, args.concurrency, args.timeout) <= 0 or args.warmup < 0:
        parser.error(
            "positive workload sizes and timeout required, warmup must be nonnegative"
        )
    payload = json.dumps(
        {
            "model": os.getenv("MODEL_NAME", "mock-llm"),
            "max_tokens": 32,
            "messages": [{"role": "user", "content": "Say hello."}],
        }
    ).encode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer " + os.getenv("GATEWAY_API_KEY", "local-demo-key"),
    }

    def request(_):
        started = time.perf_counter()
        try:
            with HTTP.open(
                urllib.request.Request(args.url, payload, headers), timeout=args.timeout
            ) as r:
                message = json.load(r)["choices"][0]["message"]["content"]
                status = str(r.status) if isinstance(message, str) else "invalid_body"
        except urllib.error.HTTPError as err:
            status = str(err.code)
            err.close()
        except (ValueError, IndexError, KeyError, TypeError):
            status = "invalid_body"
        except OSError:
            status = "transport_error"
        return status, 1000 * (time.perf_counter() - started)

    for i in range(args.warmup):
        request(i)
    started = time.perf_counter()
    with ThreadPoolExecutor(args.concurrency) as pool:
        results = list(pool.map(request, range(args.requests)))
    elapsed = time.perf_counter() - started
    statuses = Counter(s for s, _ in results)
    print(
        json.dumps(
            {
                "requests": args.requests,
                "concurrency": args.concurrency,
                "warmup": args.warmup,
                "duration_seconds": round(elapsed, 3),
                "statuses": statuses,
                "attempts_per_second": round(len(results) / elapsed, 2),
                "completions_per_second": round(statuses["200"] / elapsed, 2),
                "success_latency_ms": distribution(t for s, t in results if s == "200"),
                "failure_latency_ms": distribution(t for s, t in results if s != "200"),
            },
            indent=2,
        )
    )
    if not statuses["200"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
