# syntax=docker/dockerfile:1
FROM rust:1.96-slim-trixie AS builder
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked -p hydrant-server \
    && cp target/release/hydrant-server /hydrant-server

FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=builder /hydrant-server /usr/local/bin/hydrant-server
ENV HYDRANT_LISTEN_ADDR=0.0.0.0:8080
EXPOSE 8080
USER nonroot
ENTRYPOINT ["/usr/local/bin/hydrant-server"]
