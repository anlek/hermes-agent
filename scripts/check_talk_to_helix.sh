#!/usr/bin/env bash
# No credentials or model invocation: a healthy dedicated route must reject this with 401.
set -euo pipefail
endpoint=${1:-https://talk-to-helix.undafeed.com/v1/platform-turns/matrix}
status=$(curl --silent --show-error --max-time 15 --output /dev/null \
  --write-out '%{http_code}' --header 'Content-Type: application/json' \
  --data '{}' "$endpoint")
if [[ "$status" != "401" ]]; then
  echo "Talk to Helix route check failed: expected unauthenticated HTTP 401, got $status." >&2
  exit 1
fi
echo "Talk to Helix route is present and requires authentication (HTTP 401)."
