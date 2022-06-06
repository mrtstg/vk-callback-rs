FROM lukemathwalker/cargo-chef AS rust-chef
WORKDIR /app

FROM rust-chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM rust-chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release

FROM debian AS runtime
ENV TZ=Europe/Moscow
WORKDIR /usr/local/bin/
COPY deployment/config.toml /usr/local/bin/
COPY --from=builder /app/target/release/callback-rs /usr/local/bin
ENTRYPOINT ["/usr/local/bin/callback-rs"]
