-include .env
export

.DEFAULT_GOAL := help

.PHONY: help run build test lint fmt fmt-check bench bench-sweep bench-plot loadgen

help:
	@echo "targets: run build test lint fmt fmt-check bench bench-sweep bench-plot loadgen"

run:
	cargo run -p hydrant-server

build:
	cargo build --workspace --release --locked

test:
	cargo test --workspace --locked

lint:
	cargo clippy --workspace --all-targets --locked -- -D warnings

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

bench:
	cargo bench --workspace

loadgen:
	cargo run --release -p hydrant-loadgen -- $(ARGS)

bench-sweep: build
	python3 bench/run.py $(ARGS)

bench-plot:
	test -x bench/.venv/bin/python || (python3 -m venv bench/.venv && bench/.venv/bin/pip install -q matplotlib==3.11.2)
	bench/.venv/bin/python bench/plot.py
