.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

.PHONY: check
check:
	cargo check --all-targets

.PHONY: clippy
clippy:
	RUSTFLAGS=-Dwarnings cargo clippy --all-targets

.PHONY: test
test:
	cargo test

.PHONY: build
build:
	cargo build --examples --benches

.PHONY: clean
clean:
	cargo clean
