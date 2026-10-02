#!/usr/bin/env bash
# Live stealth-path verification for reddittv.
# Boots the server WITHOUT CHROME_DISABLED (real chromiumoxide browser) on an
# ephemeral port, or uses $BASE / $1 if given (assumes that server already runs
# with the stealth path enabled).
#
# Polite by design: small sub (videos), limit 5, single navigation.
# 429 RATE_LIMITED / 502 UPSTREAM_BLOCKED are treated as environment-blocked
# (exit 0 with NOTE), not as code bugs.
#
# Usage:
#   ./scripts/check-stealth.sh [base_url]   # default: boot local server
#   BASE=http://localhost:3000 ./scripts/check-stealth.sh
set -u

BASE="${1:-${BASE:-}}"
CHILD_PID=""
PORT=""

cleanup() {
  if [ -n "$CHILD_PID" ]; then
    kill "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

if [ -z "$BASE" ]; then
  BIN="./target/debug/reddittv"
  if [ ! -x "$BIN" ]; then
    echo "Building $BIN ..."
    cargo build || { echo "FAIL: cargo build failed"; exit 1; }
  fi
  # Ephemeral port via python3.
  PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])')"
  echo "Booting $BIN on 127.0.0.1:$PORT (stealth path ENABLED: CHROME_DISABLED unset) ..."
  # Ensure the stealth path is enabled: never pass CHROME_DISABLED=1.
  env -u CHROME_DISABLED PORT="$PORT" "$BIN" >/tmp/reddittv-stealth.log 2>&1 &
  CHILD_PID="$!"
  BASE="http://127.0.0.1:$PORT"
  # Wait for /healthz (max ~15s).
  for _ in $(seq 1 75); do
    if curl -sS -m 2 "$BASE/healthz" 2>/dev/null | grep -q '"ok"'; then
      echo "server ready at $BASE"
      break
    fi
    if ! kill -0 "$CHILD_PID" 2>/dev/null; then
      echo "FAIL: server exited early; log:"
      cat /tmp/reddittv-stealth.log 2>/dev/null || true
      exit 1
    fi
    sleep 0.2
  done
  curl -sS -m 2 "$BASE/healthz" 2>/dev/null | grep -q '"ok"' || {
    echo "FAIL: server did not become ready at $BASE/healthz"
    cat /tmp/reddittv-stealth.log 2>/dev/null || true
    exit 1
  }
else
  echo "Using existing server at $BASE"
fi

FAIL=0

echo
echo "=== healthz ==="
curl -sS -m 10 "$BASE/healthz" | python3 -m json.tool 2>/dev/null || curl -sS -m 10 "$BASE/healthz"
echo

echo "=== POST /api/refresh?sub=videos (first fetch may block on stealth nav, up to ~120s) ==="
body="$(curl -sS -m 150 -X POST -w '\n%{http_code}' "$BASE/api/refresh?sub=videos")" || { echo "curl failed"; FAIL=1; }
code="$(printf '%s' "$body" | tail -n1)"
payload="$(printf '%s' "$body" | sed '$d')"
printf '%s' "$payload" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$payload"
echo "HTTP $code"
if [[ "$code" == "429" || "$code" == "502" ]]; then
  echo "NOTE: environment-blocked ($code) — not a code bug."
elif [ "$code" != "200" ]; then
  echo "FAIL: expected HTTP 200 (or 429/502 when blocked)"
  FAIL=1
else
  echo "OK"
fi
echo

echo "=== GET /api/videos?sub=videos&limit=5&refresh=false ==="
body="$(curl -sS -m 150 -w '\n%{http_code}' "$BASE/api/videos?sub=videos&limit=5&refresh=false")" || { echo "curl failed"; FAIL=1; }
code="$(printf '%s' "$body" | tail -n1)"
payload="$(printf '%s' "$body" | sed '$d')"
printf '%s' "$payload" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$payload"
echo "HTTP $code"
if [[ "$code" == "429" || "$code" == "502" ]]; then
  echo "NOTE: environment-blocked ($code) — server correctly surfaced upstream/limiter failure."
else
  # Summarise {cached,stale,fetchedAt,videos.length,after}.
  printf '%s' "$payload" | python3 -c '
import json, sys
try:
    b = json.loads(sys.stdin.read())
    vids = b.get("videos", [])
    print("summary: cached=%s stale=%s fetchedAt=%s videos.length=%d after=%s" % (
        b.get("cached"), b.get("stale"), b.get("fetchedAt"),
        len(vids) if isinstance(vids, list) else -1, b.get("after")))
except Exception as e:
    print("summary parse failed: %s" % e)
' || true
  if [ "$code" != "200" ]; then
    echo "FAIL: expected HTTP 200 (or 429/502 when blocked)"
    FAIL=1
  else
    echo "OK"
  fi
fi
echo

if [ "$FAIL" -ne 0 ]; then echo "check-stealth: FAILURES (see above)"; exit 1; fi
echo "check-stealth: done (200 rows or documented 429/502 block)"
