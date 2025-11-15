#!/usr/bin/env bash
set -euo pipefail

TIMEOUT="${1:-180}"
END=$((SECONDS + TIMEOUT))
PORTS=("127.0.0.1:4222" "127.0.0.1:9000")

for target in "${PORTS[@]}"; do
  host="${target%%:*}"
  port="${target##*:}"
  printf 'waiting for %s:%s' "$host" "$port"
  while ! nc -z "$host" "$port" >/dev/null 2>&1; do
    if (( SECONDS >= END )); then
      echo " - timeout" >&2
      exit 1
    fi
    printf '.'
    sleep 2
  done
  echo ' ready'
  SECONDS=$((SECONDS))
done
