#!/usr/bin/env bash
# Optional: run the demo on Apache Doris 4.0.3 instead of the default 2.1.0
# all-in-one. 4.0.3 has a working native web console (2.1.0's bundled Playground
# is broken — see README). This is opt-in because newer Doris enforces OS-level
# requirements that the default 2.1.0 image only warns about, and that Docker
# Desktop does NOT satisfy out of the box.
#
# Prerequisites you must set YOURSELF first (Docker Desktop -> Settings ->
# Resources), because they can't be scripted safely:
#   - Memory: 8 GB or more   (FE + BE need it; 3.8 GB default OOMs)
#   - Swap:   0              (Doris 4.x BE refuses to start with swap enabled)
# Apply & Restart Docker after changing those. This script handles the rest
# (vm.max_map_count) and launches Doris 4.0.3.
#
# NOTE: vm.max_map_count resets on every Docker Desktop restart, so re-run this
# script after restarting Docker.

set -euo pipefail

NETWORK="iggy-doris-demo"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE="apache/doris:4.0.3-all-slim"

# --- preflight: verify Docker Desktop meets Doris 4.x hard requirements ---
mem_bytes="$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)"
mem_gb="$(awk "BEGIN{printf \"%.1f\", ${mem_bytes}/1073741824}")"
swap_mb="$(docker run --rm --privileged alpine sh -c 'free -m | awk "/Swap/{print \$2}"' 2>/dev/null || echo '?')"
echo "Docker memory: ${mem_gb} GB | swap: ${swap_mb} MB"

if [ "${swap_mb}" != "0" ]; then
  echo "ERROR: swap is ${swap_mb} MB — Doris 4.x BE will refuse to start."
  echo "       Set Swap = 0 in Docker Desktop -> Settings -> Resources, Apply & Restart, then re-run."
  exit 1
fi
if [ "${mem_bytes}" -lt 6442450944 ]; then
  echo "WARNING: <6 GB RAM allocated to Docker. FE+BE may OOM. Raise Memory to 8 GB+ in Docker Desktop."
fi

echo "==> Setting vm.max_map_count=2000000 (Doris BE requires >= 2,000,000)"
docker run --rm --privileged alpine sysctl -w vm.max_map_count=2000000 >/dev/null

echo "==> Stopping the default 2.1.0 container if running (frees ports 8030/9030/8040)"
docker stop doris-demo >/dev/null 2>&1 || true

echo "==> Launching Doris 4.0.3 (network alias 'doris', so scripts/Adminer resolve it)"
docker rm -f doris4 >/dev/null 2>&1 || true
docker run -d --name doris4 --network "${NETWORK}" --network-alias doris \
  -p 8030:8030 -p 9030:9030 -p 8040:8040 \
  "${IMAGE}" tail -f /dev/null >/dev/null

echo "==> Waiting for BE to register alive (first boot ~40-90s)"
until docker exec doris4 sh -c 'test -d /opt/apache-doris/be/log' 2>/dev/null; do sleep 2; done
for i in $(seq 1 120); do
  out="$(docker run --rm -i --network "${NETWORK}" mysql:8 mysql -h doris -P 9030 -u root -e "SHOW BACKENDS\G" 2>/dev/null)"
  if echo "$out" | grep -i 'Alive:' | grep -qi 'true'; then echo "    BE alive."; break; fi
  sleep 2
  [ "$i" = 120 ] && { echo "    BE not alive after 240s. Check: docker exec doris4 tail /opt/apache-doris/be/log/be.INFO"; exit 1; }
done

echo "==> Creating table + viewer user"
docker run --rm -i --network "${NETWORK}" mysql:8 mysql -h doris -P 9030 -u root < "${HERE}/init-doris.sql" 2>/dev/null || true

cat <<'DONE'

==> Doris 4.0.3 is up.

   Native console : http://localhost:8030   (root / blank) -> SQL/Playground -> database iggy_demo
   Adminer        : http://localhost:8088   (viewer / viewer, server doris:9030)

If iggy/runtime aren't running yet, start them as in the main README (up.sh /
the runtime), then: ./local-doris-demo/produce.sh 20

Revert to the default 2.1.0:
   docker rm -f doris4
   docker compose -f local-doris-demo/docker-compose.yml up -d doris
DONE
