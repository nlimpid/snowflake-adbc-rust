#!/usr/bin/env bash
# Decrypt ci/parameters/parameters_<cloud>.json.gpg and print `export` lines.
# Source the output so the variables land in the current shell:
#
#   source ci/scripts/decrypt_parameters.sh aws
#
# Requires PARAMETERS_SECRET (the symmetric passphrase) in the environment.
set -euo pipefail

cloud="${1:-}"
if [[ -z "$cloud" ]]; then
  echo "usage: source ci/scripts/decrypt_parameters.sh <aws|azure|gcp>" >&2
  return 1 2>/dev/null || exit 1
fi

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
enc="$root/ci/parameters/parameters_${cloud}.json.gpg"
plain="$root/ci/parameters/parameters_${cloud}.json"

if [[ ! -f "$enc" ]]; then
  echo "encrypted parameter file not found: $enc" >&2
  return 1 2>/dev/null || exit 1
fi
if [[ -z "${PARAMETERS_SECRET:-}" ]]; then
  echo "PARAMETERS_SECRET is not set" >&2
  return 1 2>/dev/null || exit 1
fi

gpg --batch --yes --decrypt \
  --passphrase "$PARAMETERS_SECRET" \
  --output "$plain" \
  "$enc"

python3 - "$plain" <<'PY'
import json, shlex, sys
data = json.load(open(sys.argv[1]))
block = data["testconnection"]
for key, value in block.items():
    print(f"export {key}={shlex.quote(str(value))}")
PY
