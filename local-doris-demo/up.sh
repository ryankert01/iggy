#!/usr/bin/env bash
# Bring up the whole local demo: Doris + Iggy + web UI, create the Doris
# table, and create the Iggy stream/topic the sink consumes.
#
# Usage:  ./local-doris-demo/up.sh
# Re-runnable: every step is idempotent.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
NETWORK="iggy-doris-demo"

IGGY="cargo run --quiet --manifest-path $REPO/Cargo.toml --bin iggy -- -u iggy -p iggy"

echo "==> Starting containers (Doris, Iggy, web UI)"
docker compose -f "$HERE/docker-compose.yml" up -d

echo "==> Waiting for Doris BE to register (can take 40-90s on a cold start)"
for i in $(seq 1 90); do
  # NB: no `-N` here — in \G vertical output it strips the "Alive:" label the
  # grep depends on, so the BE would never be detected as alive.
  alive="$(docker run --rm -i --network "$NETWORK" mysql:8 \
            mysql -h doris -P 9030 -u root -e "SHOW BACKENDS\G" 2>/dev/null \
          | grep -i 'Alive:' | grep -ic 'true' || true)"
  if [ "${alive:-0}" -ge 1 ]; then
    echo "    Doris BE alive after ${i} checks."
    break
  fi
  sleep 2
  [ "$i" = 90 ] && { echo "    Doris BE never came alive; check 'docker logs doris-demo'"; exit 1; }
done

echo "==> Creating Doris database + table"
docker run --rm -i --network "$NETWORK" mysql:8 \
  mysql -h doris -P 9030 -u root < "$HERE/init-doris.sql"

echo "==> Waiting for Iggy server TCP :8090"
for i in $(seq 1 30); do
  if nc -z localhost 8090 2>/dev/null; then echo "    Iggy up."; break; fi
  sleep 1
  [ "$i" = 30 ] && { echo "    Iggy never opened :8090; check 'docker logs iggy-demo'"; exit 1; }
done

echo "==> Creating Iggy stream 'events' and topic 'doris_events' (1 partition)"
# Tolerate "already exists" on re-runs.
$IGGY stream create events            2>/dev/null || echo "    stream 'events' already exists"
$IGGY topic create events doris_events 1 none 2>/dev/null || echo "    topic 'doris_events' already exists"

cat <<'DONE'

==> Stack is up.

   Doris  UI : http://localhost:8030               (user root, no password)
   Iggy   UI : http://localhost:3050/auth/sign-in   (user iggy / iggy)
               (the UI has no "/" route; go straight to /auth/sign-in)

Next, in a SEPARATE terminal, start the connector runtime from the repo root:

   cargo build --release -p iggy_connector_doris_sink
   IGGY_CONNECTORS_CONFIG_PATH=local-doris-demo/runtime-config.toml \
     cargo run --release --bin iggy-connectors

Then produce some messages and watch the rows appear:

   ./local-doris-demo/produce.sh 25

   # verify in Doris:
   docker run --rm -it --network iggy-doris-demo mysql:8 \
     mysql -h doris -P 9030 -u root -e "SELECT COUNT(*) FROM iggy_demo.events;"
DONE
