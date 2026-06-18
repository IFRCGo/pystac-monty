#!/usr/bin/env bash
# Export Charter model fixtures and optionally sync into monty-stac-extension examples/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODEL="$ROOT/monty-stac-extension/docs/model/sources/Charter"
OUT="$(mktemp -d)"
UPSTREAM="${UPSTREAM:-$ROOT/../monty-stac-extension/examples}"
ROLES=(charter-events charter-hazards charter-response)

trap 'rm -rf "$OUT"' EXIT

if [[ ! -d "$MODEL" ]]; then
  echo "Charter model dir missing: $MODEL (init submodule first)" >&2
  exit 1
fi

uv run pystac-monty charter --input "$MODEL" --output "$OUT"

if [[ "${1:-}" != "--sync" ]]; then
  echo "Exported to $OUT (re-run with --sync to update upstream examples)" >&2
  trap - EXIT
  echo "$OUT"
  exit 0
fi

for role in "${ROLES[@]}"; do
  mkdir -p "$UPSTREAM/$role"
  cp -a "$OUT/$role/." "$UPSTREAM/$role/"
  if [[ -d "$ROOT/monty-stac-extension/examples/$role" ]]; then
    cp -a "$OUT/$role/." "$ROOT/monty-stac-extension/examples/$role/"
  fi
done

echo "Synced ${ROLES[*]} to $UPSTREAM"
