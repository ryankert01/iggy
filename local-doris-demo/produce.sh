#!/usr/bin/env bash
# Produce N JSON messages to events/doris_events. The shape matches the
# iggy_demo.events table, so the sink loads them straight into Doris.
#
# Usage:  ./local-doris-demo/produce.sh [count]   (default 10)

set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COUNT="${1:-10}"
IGGY="cargo run --quiet --manifest-path $REPO/Cargo.toml --bin iggy -- -u iggy -p iggy"

echo "==> Sending $COUNT messages to events/doris_events"
for i in $(seq 1 "$COUNT"); do
  ts="$(date +%s)"
  json="{\"id\":${i},\"name\":\"event_${i}\",\"count\":$((i * 3)),\"amount\":$(awk "BEGIN{print ${i} * 1.5}"),\"active\":$([ $((i % 2)) -eq 0 ] && echo true || echo false),\"timestamp\":${ts}}"
  $IGGY message send events doris_events "$json"
done

echo "==> Done. The runtime polls every 100ms; rows should appear in Doris within a second or two."
