#!/usr/bin/env python3
"""Exercise real HTTP sockets and Rust binaries. Only the test's child workers are stopped."""
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import time
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.getenv("CARGO_TARGET_DIR", ROOT / "target")) / "debug"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
KEY = secrets.token_urlsafe(24)


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(check, timeout=4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except OSError:
            pass
        time.sleep(.02)
    raise AssertionError("Condition did not become true")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


class Gateway(unittest.TestCase):
    def launch(self, binary, port, **settings):
        env = {**os.environ, **settings, "TOKIO_WORKER_THREADS": "2"}
        child = subprocess.Popen([BIN / binary], env=env, stdout=subprocess.DEVNULL)
        self.addCleanup(stop, child)
        def ready():
            if child.poll() is not None:
                raise AssertionError(f"{binary} exited during startup")
            with HTTP.open(f"http://127.0.0.1:{port}/health", timeout=.3) as response:
                return response.status == 200
        eventually(ready)
        return child

    def start(self, fail_a=False, **settings):
        self.a_port = free_port()
        self.a = self.worker("A", self.a_port, fail_a)
        b_port = free_port()
        self.worker("B", b_port)
        self.port = free_port()
        self.base = f"http://127.0.0.1:{self.port}"
        config = dict(LISTEN_ADDR=f"127.0.0.1:{self.port}", GATEWAY_API_KEY=KEY,
                      MODEL_NAME="mock-llm", BACKEND_A=f"http://127.0.0.1:{self.a_port}",
                      BACKEND_B=f"http://127.0.0.1:{b_port}", MAX_INFLIGHT="4",
                      MAX_WAITING="2", RATE_LIMIT_BURST="30", RATE_LIMIT_RPS="100")
        self.gateway = self.launch("inferenceforge", self.port, **(config | settings))

    def worker(self, name, port, fail=False):
        return self.launch("mock_worker", port, BACKEND_NAME=name, BIND_HOST="127.0.0.1",
                           PORT=str(port), FAIL_MODE="always" if fail else "")

    def chat(self, raw=None, key=KEY, **options):
        payload = {"model": "mock-llm", "messages": [{"role":"user","content":"test"}], **options}
        req = urllib.request.Request(self.base + "/v1/chat/completions",
              data=json.dumps(payload).encode() if raw is None else raw,
              headers={"Authorization": "Bearer "+key, "Content-Type":"application/json"})
        try:
            return HTTP.open(req, timeout=5)
        except urllib.error.HTTPError as e:
            return e

    def pool(self):
        with HTTP.open(self.base + "/backends") as response:
            return json.load(response)

    def test_auth_validation_limit_and_round_robin(self):
        self.start()
        for kwargs, code in [({"raw":b"{","key":"wrong"},401),
                             ({"raw":b"x"*1100000},413), ({"model":"unknown"},400),
                             ({"messages":[{}]},400), ({"stream":"yes"},400)]:
            with self.chat(**kwargs) as response:
                self.assertEqual(response.status,code)
        for worker in "AB":
            with self.chat() as response:
                self.assertEqual(response.headers["x-backend"], worker)
                self.assertEqual(json.load(response)["choices"][0]["message"]["content"],
                                 "Hello from model "+worker)
        self.assertEqual(self.pool()["admitted"], 2)

    def test_stream_holds_permit_bounds_waiters_and_cancels(self):
        self.start(MAX_INFLIGHT="1", MAX_WAITING="1", QUEUE_WAIT_MS="3000")
        with self.chat(stream=True) as stream, ThreadPoolExecutor(1) as executor:
            self.assertIn(b"Hello", stream.readline())
            queued = executor.submit(self.chat)
            eventually(lambda: self.pool()["queued"] == 1)
            with self.chat() as excess:
                self.assertEqual(excess.status,503)
                self.assertIn(b"queue full",excess.read())
            stream.close()
            with queued.result(timeout=3) as next_request:
                self.assertEqual(next_request.status,200)
        eventually(lambda: self.pool()["active"] == self.pool()["queued"] == 0)

    def test_queue_deadline_and_rate_quota(self):
        self.start(MAX_INFLIGHT="1", QUEUE_WAIT_MS="25", RATE_LIMIT_RPS="1", RATE_LIMIT_BURST="2")
        with self.chat(stream=True) as stream:
            with self.chat() as waiting:
                self.assertEqual(waiting.status,503)
                self.assertIn(b"queue timeout",waiting.read())
            with self.chat() as limited:
                self.assertEqual(limited.status,429)
                self.assertEqual(limited.headers["Retry-After"], "1")
            self.assertIn(b"data: [DONE]", stream.read())

    def test_refused_connection_failover_and_recovery(self):
        self.start()
        stop(self.a)
        for _ in range(4):
            with self.chat() as response:
                self.assertEqual(response.headers["x-backend"], "B")
        self.assertEqual(self.pool()["backends"][0]["circuit"], "open")
        self.worker("A",self.a_port)
        eventually(lambda: self.pool()["backends"][0]["circuit"] == "probe-ready",12)
        with self.chat() as probe:
            self.assertEqual(probe.headers["x-backend"],"A")
        self.assertEqual(self.pool()["backends"][0]["circuit"],"closed")

    def test_no_replay_after_server_error(self):
        self.start(fail_a=True)
        with self.chat() as failed:
            self.assertEqual(failed.status,502)
        self.assertEqual(self.pool()["backends"][1]["accepted_headers"],0)

    def test_header_timeout_is_not_replayed(self):
        self.start(HEADER_TIMEOUT_MS="35")
        with self.chat() as timed_out:
            self.assertEqual(timed_out.status,504)
        self.assertEqual(self.pool()["failovers"],0)

    def test_stream_deadline_aborts_without_sentinel(self):
        self.start(GENERATION_BUDGET_MS="310")
        with self.chat(stream=True) as stream:
            self.assertEqual(stream.status,200)
            with self.assertRaises(http.client.IncompleteRead) as aborted:
                stream.read()
            self.assertNotIn(b"[DONE]",aborted.exception.partial)
        eventually(lambda: self.pool()["active"] == 0)
        with HTTP.open(self.base + "/metrics") as response:
            self.assertIn(b"ai_stream_errors_total 1",response.read())

    def test_sigterm_drains_open_stream(self):
        self.start()
        with self.chat(stream=True) as stream:
            self.assertIn(b"Hello",stream.readline())
            self.gateway.terminate()
            self.assertIn(b"[DONE]",stream.read())
        self.assertEqual(self.gateway.wait(timeout=3),0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
