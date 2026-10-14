# syntax=docker/dockerfile:1
FROM rust:1.95-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /mocra
ENV CARGO_BUILD_JOBS=4
COPY . .
# COPY can restore source mtimes older than cached Cargo artifacts; refresh inputs so
# the target cache cannot silently reuse a binary from a different source revision.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/mocra/target \
    touch Cargo.toml && find src examples -type f -name '*.rs' -exec touch {} + \
    && CARGO_PROFILE_DEV_DEBUG=0 cargo build --example cluster_acceptance \
      --features cluster-embedded,queue-nats,dashboard \
    && cp target/debug/examples/cluster_acceptance /cluster_acceptance

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /cluster_acceptance /usr/local/bin/cluster_acceptance
ENTRYPOINT ["/usr/local/bin/cluster_acceptance"]
