FROM --platform=$TARGETOS/$TARGETARCH docker.io/library/rust:1.97.1-slim-trixie AS build-image
LABEL org.opencontainers.image.description="PostgreSQL migration tool"
LABEL authors="Olegs Korsaks"

ARG TARGETARCH
ARG TARGETOS

WORKDIR /build

RUN apt update && apt install -y --no-install-recommends make musl-tools musl-dev && \
  rm -rf /var/lib/apt/lists/*

COPY ./ /build/

# Map Docker architecture to Rust target
RUN echo "Target architecture is: ${TARGETARCH}" && \
    if [ "${TARGETARCH}" = "amd64" ]; then \
        RUST_TARGETARCH=x86_64 make release; \
    elif [ "${TARGETARCH}" = "arm64" ]; then \
        RUST_TARGETARCH=aarch64 make release; \
    else \
        echo "Unsupported architecture: ${TARGETARCH}"; exit 1; \
    fi

FROM --platform=$TARGETOS/$TARGETARCH docker.io/library/postgres:18.4 AS runtime
LABEL org.opencontainers.image.description="PostgreSQL migration tool"
LABEL authors="Olegs Korsaks"

WORKDIR /
COPY --from=build-image /build/target/pg-migrate /build/LICENSE /

# The postgres image already has psql, pg_dump, pg_restore in /usr/bin/
# We just need to make sure we can run our binary.

ENTRYPOINT ["/pg-migrate"]
