MAKEFLAGS=-j3

all: linux-amd64 linux-arm64 darwin-arm64
	cp crates/zomdb-sys/target/zomdb-sys.h include/zomdb.h

darwin-arm64:
	cargo build --release --target aarch64-apple-darwin
	cp target/aarch64-apple-darwin/release/libzomdb_sys.a lib/libzomdb_darwin_arm64.a

linux-arm64:
	cargo build --release --target aarch64-unknown-linux-gnu
	cp target/aarch64-unknown-linux-gnu/release/libzomdb_sys.a lib/libzomdb_linux_arm64.a

linux-amd64:
	cargo build --release --target x86_64-unknown-linux-gnu
	cp target/x86_64-unknown-linux-gnu/release/libzomdb_sys.a lib/libzomdb_linux_amd64.a

clean:
	rm -rf lib/*
	cargo clean
