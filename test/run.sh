#!/usr/bin/env bash
# Ephemeral Postgres, install pgseries.sql, run smoke.sql, tear down.
# Usage: ./test/run.sh  [PG_BIN overrides auto-detect]
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
PG_BIN="${PG_BIN:-$(command -v psql 2>/dev/null | xargs dirname 2>/dev/null || echo /nix/store/vwph78y8l4q6fndzmd9yqgc4rln6583g-postgresql-18.6/bin)}"
export PATH="$PG_BIN:$PATH"

ROOT=/tmp/opencode/pgseries_test
PGDATA=$ROOT/data SOCK=$ROOT/sock PORT=55434
rm -rf "$ROOT"; mkdir -p "$PGDATA" "$SOCK"

initdb -D "$PGDATA" -E UTF8 >/dev/null
pg_ctl -D "$PGDATA" -l "$ROOT.log" -o "-k $SOCK -p $PORT -c listen_addresses=''" start >/dev/null
trap 'pg_ctl -D "$PGDATA" stop >/dev/null 2>&1; rm -rf "$ROOT"' EXIT
sleep 2

echo "== install =="
psql -h "$SOCK" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 -f "$HERE/pgseries.sql"
echo "== reinstall (upgrade-path coverage: same file twice must be a no-op) =="
psql -h "$SOCK" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 -f "$HERE/pgseries.sql"
echo "== smoke =="
psql -h "$SOCK" -p "$PORT" -d postgres -v ON_ERROR_STOP=1 -f "$HERE/test/smoke.sql"
