#!/usr/bin/env bash
# Manual live verification for reddittv feeds.
# Usage: ./scripts/check-feeds.sh [base_url]   (default http://localhost:3000)
set -u
BASE="${1:-http://localhost:3000}"
FAIL=0

check() {
  local name="$1" url="$2" expect="$3"
  echo "=== $name ==="
  echo "GET $url"
  body="$(curl -sS -m 25 -w '\n%{http_code}' "$url")" || { echo "curl failed"; FAIL=1; echo; return; }
  code="$(printf '%s' "$body" | tail -n1)"
  payload="$(printf '%s' "$body" | sed '$d')"
  printf '%s' "$payload" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$payload"
  echo "HTTP $code"
  if [ "$code" != "$expect" ]; then
    # /api/videos may legitimately return 429/502 when Reddit blocks the sandbox.
    if [[ "$url" == *"/api/videos"* ]] && [[ "$code" == "429" || "$code" == "502" ]]; then
      echo "NOTE: upstream blocked ($code) — server correctly surfaced Reddit failure."
    else
      echo "FAIL: expected HTTP $expect"
      FAIL=1
    fi
  else
    echo "OK"
  fi
  echo
}

check "healthz" "$BASE/healthz" 200
check "subs" "$BASE/api/subs" 200
check "videos (live feed)" "$BASE/api/videos?sub=videos&limit=5" 200
check "videos refresh=false" "$BASE/api/videos?sub=videos&limit=5&refresh=false" 200
check "videos refresh=true" "$BASE/api/videos?sub=videos&limit=5&refresh=true" 200

echo "=== refresh (POST force) ==="
echo "POST $BASE/api/refresh?sub=videos"
body="$(curl -sS -m 60 -X POST -w '\n%{http_code}' "$BASE/api/refresh?sub=videos")" || { echo "curl failed"; FAIL=1; echo; }
code="$(printf '%s' "$body" | tail -n1)"
payload="$(printf '%s' "$body" | sed '$d')"
printf '%s' "$payload" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$payload"
echo "HTTP $code"
if [[ "$code" == "429" || "$code" == "502" ]]; then
  echo "NOTE: upstream/limiter blocked ($code) — server correctly surfaced failure."
elif [ "$code" != "200" ]; then
  echo "FAIL: expected HTTP 200"
  FAIL=1
else
  echo "OK"
fi
echo

if [ "$FAIL" -ne 0 ]; then echo "check-feeds: FAILURES (see above)"; exit 1; fi
echo "check-feeds: all green"
