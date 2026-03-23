FROM rust:1.88-alpine AS builder
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static perl make
WORKDIR /app

# Layer 1: cache dependency compilation
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

# Layer 2: build actual source (deps already cached)
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM alpine:3.20
COPY --from=builder /app/target/release/dmm /usr/local/bin/dmm
EXPOSE 3000
CMD ["dmm"]
