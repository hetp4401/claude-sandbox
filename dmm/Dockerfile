FROM rust:1.85-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM alpine:3.20
COPY --from=builder /app/target/release/dmm /usr/local/bin/dmm
EXPOSE 3000
CMD ["dmm"]
