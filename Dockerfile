# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM rust:1.88-slim-bookworm AS builder
ARG TARGETARCH
WORKDIR /build
RUN case "$TARGETARCH" in \
        amd64) echo x86_64-unknown-linux-gnu x86_64-linux-gnu gcc-x86-64-linux-gnu libc6-dev-amd64-cross ;; \
        arm64) echo aarch64-unknown-linux-gnu aarch64-linux-gnu gcc-aarch64-linux-gnu libc6-dev-arm64-cross ;; \
        *) echo "unsupported TARGETARCH $TARGETARCH" >&2; exit 1 ;; \
    esac > /target && \
    read -r triple gnu gcc libc < /target && \
    if ! command -v "$gnu-gcc" >/dev/null; then \
        apt-get update && \
        apt-get install -y --no-install-recommends "$gcc" "$libc" && \
        rm -rf /var/lib/apt/lists/*; \
    fi && \
    rustup target add "$triple"
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    read -r triple gnu gcc libc < /target && \
    linker_var="CARGO_TARGET_$(echo "$triple" | tr 'a-z-' 'A-Z_')_LINKER" && \
    export "$linker_var=$gnu-gcc" "CC_$(echo "$triple" | tr '-' '_')=$gnu-gcc" && \
    cargo build --release --locked --bins --target "$triple" && \
    mkdir -p /out && \
    for b in weave weave-northbound weave-southbound weave-controller weave-adapter-strom; do \
        cp "target/$triple/release/$b" /out/; \
    done

FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl tini && \
    rm -rf /var/lib/apt/lists/* && \
    useradd --system --create-home --uid 10001 weave
COPY --from=builder /out/ /usr/local/bin/
USER weave
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["weave", "--help"]
