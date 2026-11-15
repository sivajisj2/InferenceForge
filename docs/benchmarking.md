# Measured capacity and overload

One release-build run on Linux x86-64, AMD EPYC 9V74 host, Rust 1.100.0. Two Tokio threads per process, two deterministic workers, four gateway slots, two waiting slots, 150 ms queue deadline and a 100 requests/s token bucket with burst 30. The JSON worker sleeps 200 ms before returning. The Python client runs on the same host over loopback, with a fresh HTTP request for each attempt.

| Concurrent clients | HTTP 200 | HTTP 429 | HTTP 503 | Completed/s | Successful p50 | Successful p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 60 | 0 | 0 | 4.93 | 202.66 ms | 203.32 ms |
| 4 | 60 | 0 | 0 | 19.65 | 203.01 ms | 206.85 ms |
| 12 | 4 | 29 | 27 | 19.44 | 203.01 ms | 203.84 ms |

The four-client result is close to the simple capacity estimate of four permits divided by 0.2 seconds per completion, or 20 completions per second. At twelve clients, fast rejections let the closed-loop client consume its remaining attempts quickly. Only four requests completed in that run. Its p95 has four observations and is unsuitable for a tail-latency estimate.

Each row includes 60 measured attempts after five serial warmups. These short runs demonstrate admission behavior. Sustained load, multiple trials and fixed-arrival-rate traffic are needed for capacity planning. The [CPU-model check](real-model.md) is a separate functional test.

## Reproduce

Run the native release binaries with the settings above, or start the release image with `make up`, then run:

```bash
for n in 1 4 12; do
  python3 scripts/benchmark.py --requests 60 --concurrency "$n" --warmup 5
done
```

Raw output includes duration, attempt throughput, completed throughput, HTTP outcomes and separate success/failure latency percentiles. Open the [one-client](evidence/mock-c1.json), [four-client](evidence/mock-c4.json) and [twelve-client](evidence/mock-c12.json) samples. Compose results can vary with CPU allocation and Docker networking.
