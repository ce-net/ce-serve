# ce-serve as a container — "just an app running in a ce-node container that serves whatever we
# point it at." Built so ce-gke can place it on any docker-capable mesh host. It reaches the mesh
# through a ce node (CE_NODE_URL); pass the node's API token via CE_API_TOKEN (a ce-gke secret) since
# a container can't read the host's data dir.
#
# Build context must include the sibling ce-rs crate (ce-serve depends on ../ce-rs), so build from the
# workspace root:   docker build -f ce-serve/Dockerfile -t ce-net/ce-serve:latest .

FROM rust:1-bookworm AS build
WORKDIR /src
COPY ce-rs ./ce-rs
COPY ce-serve ./ce-serve
WORKDIR /src/ce-serve
RUN cargo build --release --bin ce-serve

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/ce-serve/target/release/ce-serve /usr/local/bin/ce-serve
ENV CE_SERVE_PORT=8790
EXPOSE 8790
# CE_NODE_URL + CE_API_TOKEN are provided at deploy time (see deploy/ce-serve.gke.yaml).
ENTRYPOINT ["/usr/local/bin/ce-serve"]
