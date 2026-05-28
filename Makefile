.PHONY: build run clean test bench fmt check

build:
	cargo build --release

run:
	cargo run --release --

clean:
	cargo clean
	rm -rf data/

test:
	cargo test -- --nocapture

bench:
	cargo test --release bench_ -- --nocapture

fmt:
	cargo fmt

check:
	cargo clippy -- -D warnings
