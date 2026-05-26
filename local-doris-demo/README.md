# Local Doris sink demo

A one-command local stack to see the Doris sink connector working end to end:
produce JSON messages to Iggy, watch them land in an Apache Doris table.

**This directory is scratch — it is not part of the PR.** Delete it any time.

## What runs where

| Component         | Where        | Dashboard / endpoint                       |
| ----------------- | ------------ | ------------------------------------------ |
| Apache Doris      | Docker       | http://localhost:8030 (root, no password)  |
| Adminer (DB view) | Docker       | http://localhost:8088 (browse Doris rows)  |
| Apache Iggy       | Docker       | TCP :8090, HTTP :3000                       |
| Iggy web UI       | Docker       | http://localhost:3050/auth/sign-in (iggy / iggy) |
| Connector runtime | **host**     | logs to your terminal; HTTP :8081           |

The runtime runs on the host on purpose: Doris's Stream Load FE replies with a
`307` redirect to the BE at `127.0.0.1:8040`, and running on the host lets that
resolve to the published BE port. (Inside-Docker would need the runtime on the
same network — see Troubleshooting.)

## Prerequisites

- Docker (with `docker compose`)
- Rust toolchain (the runtime and `iggy` CLI build from this repo)
- `nc` and `awk` (preinstalled on macOS)

## Run it

```sh
# 1. Bring up Doris + Iggy + web UI, create the table, stream, and topic
./local-doris-demo/up.sh

# 2. In a second terminal, build the sink and start the runtime
cargo build --release -p iggy_connector_doris_sink
IGGY_CONNECTORS_CONFIG_PATH=local-doris-demo/runtime-config.toml \
  cargo run --release --bin iggy-connectors

# 3. In a third terminal, produce some messages
./local-doris-demo/produce.sh 25
```

Then watch:

- **Iggy UI** (http://localhost:3050/auth/sign-in — the UI has no `/` route,
  so go straight to the sign-in page; log in with iggy / iggy) — the `events`
  stream / `doris_events` topic message count climbs as you produce.
- **Connector terminal** — one log line per Stream Load (label + status).
- **Doris** — row count climbs:

  ```sh
  docker run --rm -it --network iggy-doris-demo mysql:8 \
    mysql -h doris -P 9030 -u root -e "SELECT * FROM iggy_demo.events ORDER BY id;"
  ```

  Or browse the rows in **Adminer** at http://localhost:8088 (System `MySQL`,
  Server `doris:9030`, user `viewer`, password `viewer`, DB `iggy_demo` → table
  `events` → "Select data"). The `viewer` user exists because Adminer refuses
  blank-password logins and Doris `root` has none; the connector still uses
  `root`.

  > The Doris bundled web Playground at http://localhost:8030 has a broken
  > database picker in the 2.1.0 all-in-one image (it sends `dbName=undefined`
  > on select and query). Use Adminer or the MySQL CLI below instead. The Doris
  > FE backend itself is fine — its REST API lists databases correctly. For a
  > working native console, see "Optional: Doris 4.0.3" below.

## Optional: Doris 4.0.3 (working native web console)

The default `2.1.0` image works everywhere but ships a broken web Playground.
Doris **4.0.3** has a fixed native console — but newer Doris enforces OS-level
requirements as **hard** errors (2.1.0 only warned), and Docker Desktop doesn't
meet them by default. This is why "just use the newest version" doesn't work
out of the box; it's the environment, not the architecture (the image is native
`linux/arm64`).

**One-time Docker Desktop setup** (Settings → Resources):

| Requirement | Default | Set to |
| --- | --- | --- |
| Memory | ~3.8 GB | **8 GB+** (FE+BE OOM below this) |
| Swap | on | **0** (BE refuses to start with swap enabled) |

Apply & Restart. Then:

```sh
./local-doris-demo/enable-doris-4.sh
```

The script sets `vm.max_map_count=2000000` (the third requirement; no Docker
Desktop slider for it, and it resets on every Docker restart — so re-run the
script after restarting Docker), stops the 2.1.0 container, launches 4.0.3 with
the `doris` network alias, waits for the BE to go alive, and creates the table.

Then open **http://localhost:8030** → log in `root` / blank → **SQL/Playground**
→ database `iggy_demo` → `SELECT * FROM events`. The database picker works here.

Revert to the default 2.1.0:

```sh
docker rm -f doris4
docker compose -f local-doris-demo/docker-compose.yml up -d doris
```

> Don't run both Doris versions at once — they share ports 8030/9030/8040.
> CI/Linux should stay on the 2.1.0 default; 4.0.3 is a local-dev convenience.

## Tear down

```sh
docker compose -f local-doris-demo/docker-compose.yml down -v
docker rm -f doris4 adminer-demo 2>/dev/null
rm -rf local-doris-demo/.runtime_state
```

## Troubleshooting

- **Loads hang or time out after the redirect.** This is the macOS routing
  case. Confirm BE port 8040 is published (`docker ps`) and that the runtime is
  running on the host, not in a container. If Doris advertises an internal IP
  instead of `127.0.0.1`, run the runtime inside the `iggy-doris-demo` network
  and set `fe_url`/`address` to the container names.
- **`CREATE TABLE` fails with "Failed to find enough backend".** The BE hasn't
  finished reporting its storage paths yet. Re-run `./up.sh`; it retries.
- **Web UI returns 404 at `http://localhost:3050/`.** Expected — this build has
  no root route. Use `/auth/sign-in`. Real routes: `/auth/sign-in`,
  `/dashboard/overview`, `/dashboard/streams`.
- **Web UI shows no data / network errors after sign-in.** `PUBLIC_IGGY_API_URL`
  must be reachable from your browser (it's `http://localhost:3000` here). Make
  sure the iggy container's `:3000` is published.
- **Runtime can't find the plugin.** Check the absolute `path` in
  `connectors/doris_sink.toml` matches your checkout and that
  `cargo build --release -p iggy_connector_doris_sink` produced
  `target/release/libiggy_connector_doris_sink.dylib`.
- **Non-loopback HTTP warning in the runtime logs.** Expected — `fe_url` is
  plain `http://`. Fine for local; use `https://` in production.
