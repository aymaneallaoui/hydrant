-include .env
export

.DEFAULT_GOAL := help

.PHONY: help run build test lint fmt fmt-check bench

help:
	@echo "targets: run build test lint fmt fmt-check bench"

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
