#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
examples_dir="${repo_root}/monty-stac-extension/examples"
tmp_dir="$(mktemp -d)"
fgb_default="${repo_root}/tests/data-files/world-administrative-boundaries.fgb"
export MONTY_WORLD_ADMIN_BOUNDARIES_FGB="${MONTY_WORLD_ADMIN_BOUNDARIES_FGB:-${fgb_default}}"

cems_source="${repo_root}/monty-stac-extension/docs/model/sources/CEMS/api-files/EMSR847-storm-detail.json"
charter_source="${repo_root}/monty-stac-extension/docs/model/sources/Charter"

cleanup() {
  rm -rf "${tmp_dir}"
}
trap cleanup EXIT

cd "${repo_root}"

echo "Regenerating CEMS examples..."
uv run python - <<'PY' "${cems_source}" "${tmp_dir}"
from pathlib import Path
import sys

from pystac_monty.sources.cems import default_cems_export_geocoder, regenerate_cems_examples

regenerate_cems_examples(Path(sys.argv[1]), Path(sys.argv[2]), geocoder=default_cems_export_geocoder())
PY

for collection in cems-events cems-hazards cems-response cems-impacts; do
  rm -rf "${examples_dir}/${collection}"
  mkdir -p "${examples_dir}/${collection}"
  cp -R "${tmp_dir}/${collection}/." "${examples_dir}/${collection}/"
done

echo "Regenerating Charter examples..."
uv run python - <<'PY' "${charter_source}" "${tmp_dir}"
from pathlib import Path
import sys

from pystac_monty.sources.charter import regenerate_charter_examples

regenerate_charter_examples(Path(sys.argv[1]), Path(sys.argv[2]))
PY

for collection in charter-events charter-hazards charter-response; do
  rm -rf "${examples_dir}/${collection}"
  mkdir -p "${examples_dir}/${collection}"
  cp -R "${tmp_dir}/${collection}/." "${examples_dir}/${collection}/"
done

cd "${repo_root}/monty-stac-extension"
npm run format-examples
npm test
