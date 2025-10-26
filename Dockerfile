FROM rust:1.90.0-alpine AS builder

WORKDIR /usr/src/stranger

RUN apk add --no-cache musl-dev

# Build: cache dependencies
RUN mkdir -p crates && \
    cd crates && \
    cargo new stranger_api_server && \
    cargo new stranger_jail --lib && \
    rm stranger_api_server/Cargo.toml && \
    rm stranger_jail/Cargo.toml

COPY Cargo.toml Cargo.lock ./
COPY crates/stranger_api_server/Cargo.toml crates/stranger_api_server/Cargo.toml
COPY crates/stranger_jail/Cargo.toml crates/stranger_jail/Cargo.toml

RUN cargo build --release && rm -rf src

# Build: copy source code and build
COPY crates/stranger_api_server/src crates/stranger_api_server/src
COPY crates/stranger_jail/src crates/stranger_jail/src

# There is a bug with Docker where mtimes are not updated when files are copied.
# This causes cargo to think that files have not changed and skip recompiling.

RUN touch crates/stranger_api_server/src/main.rs
RUN touch crates/stranger_jail/src/lib.rs

RUN cargo build --release

FROM docker:28.3.3-dind

RUN apk add --no-cache libressl-dev ca-certificates-bundle tini bash ncurses wget

COPY scripts/server-entrypoint.sh /server-entrypoint.sh
RUN chmod +x /server-entrypoint.sh

# Install gVisor (runsc)
RUN set -e && \
    wget https://storage.googleapis.com/gvisor/releases/release/latest/x86_64/runsc && \
    chmod a+rx runsc && \
    mv runsc /usr/local/bin

RUN /usr/local/bin/runsc install

ENV DOCKER_HOST=unix:///var/run/docker.sock
EXPOSE 8081
VOLUME ["/var/lib/docker", "/etc/stranger"]

COPY --from=builder /usr/src/stranger/target/release/stranger_api_server /usr/local/bin/stranger_api_server

CMD ["/server-entrypoint.sh"]