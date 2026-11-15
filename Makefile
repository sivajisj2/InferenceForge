.PHONY: demo up down check test drill benchmark
demo:
	bash scripts/demo.sh
up:
	docker compose up --build --wait
down:
	docker compose down
check:
	cargo fmt --all -- --check
	cargo clippy --all-targets --locked -- -D warnings
	cargo test --all-targets --locked
test:
	cargo build --locked --bins
	python3 scripts/integration_test.py
drill:
	bash scripts/failover_test.sh
benchmark:
	python3 scripts/benchmark.py --requests 60 --concurrency 4
