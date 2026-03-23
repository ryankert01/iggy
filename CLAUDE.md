# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Iggy is a persistent message streaming platform written in Rust. It supports QUIC, WebSocket, TCP (custom binary protocol), and HTTP transport protocols. It uses a **thread-per-core shared-nothing architecture** with `io_uring`/`compio` for high throughput and sub-millisecond tail latencies.

## Build & Test Commands

### Rust (core platform)

```bash
cargo build                                          # Build all workspace crates
cargo test                                           # Run all tests
cargo test <TEST_NAME>                               # Run a single test
cargo nextest run                                    # Run tests with nextest (faster)
cargo nextest run --nocapture -- <TEST_NAME>          # Single test with output
cargo run --bin iggy-server                           # Run the server
cargo clippy --all-targets --all-features -- -D warnings  # Lint
cargo fmt --all                                      # Format
cargo machete                                        # Find unused dependencies
cargo sort --workspace                               # Sort Cargo.toml entries
typos                                                # Spell check (install: cargo install typos-cli)
```

A `justfile` provides aliases: `just build`, `just test`, `just nextest`, `just server`.

### Pre-commit hooks

Install with [prek](https://github.com/j178/prek): `cargo install prek && prek install`. Hooks run `cargo fmt`, `cargo sort`, `typos`, `taplo` (TOML format), license headers, trailing whitespace/newline checks on pre-commit. `cargo clippy` and language-specific linters run on pre-push.

### Foreign SDKs

Each SDK lives under `foreign/<lang>/` with its own build system:

- **Go**: `cd foreign/go && go build ./... && go test ./...` (Go 1.25+, `golangci-lint` for linting)
- **Java**: `cd foreign/java && ./gradlew build` (Gradle, Spotless formatting)
- **Python**: `cd foreign/python && maturin develop` (PyO3/Maturin, `ruff` for linting, `pytest` for tests)
- **Node.js**: `cd foreign/node && npm install && npm run build && npm test` (TypeScript, ESLint)
- **C#**: `cd foreign/csharp && dotnet build Iggy_SDK.sln && dotnet test`
- **C++**: `cd foreign/cpp && bazel build //:iggy-cpp`

### BDD tests (cross-SDK)

```bash
./scripts/run-bdd-tests.sh [sdk] [feature_file]  # sdk: all|rust|python|go|node|csharp|java
```

Shared Gherkin scenarios are in `bdd/scenarios/`. Language-specific step implementations are in `bdd/<lang>/`.

## Architecture

### Workspace layout

```
core/
  server/          - iggy-server binary (main entry point)
  cli/             - iggy CLI binary (installed via `cargo install iggy-cli`)
  sdk/             - Rust client SDK (iggy crate on crates.io)
  common/          - Shared types, traits, error types (iggy_common)
  binary_protocol/ - Wire protocol codec: [length:4][code:4][payload:N] (no I/O, no async)
  shard/           - Shard execution engine (one shard per CPU core)
  partitions/      - Partition and segmented log abstractions
  journal/         - Append-only message journal with indexing
  metadata/        - Stream/topic/partition/user metadata storage
  message_bus/     - Inter-shard messaging
  configs/         - Server configuration (compiled into binary via static_toml)
  consensus/       - Viewstamped Replication (future clustering)
  connectors/      - Plugin runtime + SDK for source/sink connectors
  bench/           - Benchmarking tool with dashboard
  integration/     - Integration test library
  ai/mcp/          - Model Context Protocol server
foreign/           - Non-Rust SDKs (go, java, python, node, csharp, cpp)
bdd/               - Cross-SDK BDD tests with shared Gherkin scenarios
examples/          - Example code per language
web/               - Svelte/TypeScript Web UI (embedded in server binary via rust-embed)
```

### Key architectural concepts

- **Shared-nothing, thread-per-core**: Each CPU core runs one shard (`core/shard/`). Shards communicate via `core/message_bus/`. CPU allocation is configurable via `sharding.cpu_set` in server config (supports NUMA-aware pinning).
- **Transport layer**: Four independent transport implementations in `core/server/src/{tcp,quic,http,websocket}/`, all with TLS support. Binary protocol handlers are in `core/server/src/binary/`.
- **Storage hierarchy**: Streams contain topics, topics contain partitions, partitions contain segments. Segments are the physical append-only log files (`core/server/src/streaming/segments/`). Indexes enable offset-based and timestamp-based lookups.
- **Zero-copy serialization**: The binary protocol (`core/binary_protocol/`) uses custom `WireEncode`/`WireDecode` traits. Consensus headers use `#[repr(C)]` with `bytemuck` for zero-copy deserialization.
- **Server config**: `core/server/config.toml` is the default configuration. It's compiled into the binary via `static_toml`. Environment variables override config (e.g., `IGGY_TCP_ADDRESS=0.0.0.0:8090`).

### Server ports (default)

| Port | Protocol  |
|------|-----------|
| 3000 | HTTP/REST |
| 8080 | QUIC      |
| 8090 | TCP       |
| 8092 | WebSocket |

## Commit Message Convention

Format: `type(scope): subject` — keep subject under 72 chars.

Types: `feat`, `fix`, `refactor`, `chore`, `test`, `ci`, `deps`, `docs`
Scopes: `server`, `sdk`, `cli`, `go`, `java`, `python`, `node`, `csharp`, `bench`, `ci`, `docs`, `rust`, `security`

## Contributing Rules

- Every PR must link to an approved issue. New contributors: PRs under 500 lines.
- One PR = one purpose (bug fix, feature, or refactor — not mixed).
- High-risk areas requiring design discussion first: persistence, protocol, concurrency, public API, connectors.
- All Rust files must have Apache 2.0 license headers. Run `just licenses-check` or `./scripts/ci/license-headers.sh --check`.
- Run code locally before submitting — "relying on CI" is not acceptable.

## Rust Toolchain

Pinned to **Rust 1.94.0** via `rust-toolchain.toml`. Workspace edition is **2024**. Release builds use LTO with single codegen unit. Dev builds selectively optimize CPU-intensive deps (argon2, twox-hash, rand_chacha).
