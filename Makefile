MAKEFLAGS=-j3

PACKAGE_NAME=zomdb

all: linux-amd64 linux-arm64 darwin-arm64
	cp target/${PACKAGE_NAME}.h include/${PACKAGE_NAME}.h

darwin-arm64:
	cargo build --release --target aarch64-apple-darwin
	cp target/aarch64-apple-darwin/release/lib${PACKAGE_NAME}.a lib/lib${PACKAGE_NAME}_darwin_arm64.a

linux-arm64:
	cargo build --release --target aarch64-unknown-linux-gnu
	cp target/aarch64-unknown-linux-gnu/release/lib${PACKAGE_NAME}.a lib/lib${PACKAGE_NAME}_linux_arm64.a

linux-amd64:
	cargo build --release --target x86_64-unknown-linux-gnu
	cp target/x86_64-unknown-linux-gnu/release/lib${PACKAGE_NAME}.a lib/lib${PACKAGE_NAME}_linux_amd64.a

clean:
	rm -rf lib/*
	cargo clean
