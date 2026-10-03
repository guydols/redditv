Reddit Video Player

## Logs (diagnosing slow loads)

The server logs every API request (method, route, sub/limit/after/refresh,
status, elapsed ms, served-from, videos count, loading/hasMore) plus the
scrape lifecycle (head-refresh start/end with winner leg + rows, paginate
start/end, nav-gate misses, single-flight coalescing, fills and blocks).
Per-leg HTTP/browser details (public/RSS/OAuth legs, browser nav/selector/
extract) are at `debug` level. No PII beyond subreddit names is logged.
Secrets are never logged — only the auth mode label (`script`/`no-auth`).

```sh
cargo run                  # default: info (requests + scrape start/end)
RUST_LOG=debug cargo run   # verbose: per-leg scrape details
RUST_LOG=reddittv=debug,tower_http=debug cargo run  # same, scoped
```

## Auth (optional Reddit OAuth — no registration needed by default)

By default the server needs **no Reddit account or app registration**. The
HTTP chain is fully no-auth:

`ArcticShift` (primary, epoch pagination, ~2 req/s + `X-RateLimit-Reset`) →
`PullPush` (secondary, 4s gap, never sole source) → `Redlib` round-robin
(`safereddit.com`, `redlib.catsarch.com`, `redlib.r4fo.com`,
`redlib.cow.rip`, rotated on 429/403 with per-host cooldowns) → native
`RSS` → stealth-browser `old.reddit` HTML (last resort).

Direct `api`/`www.reddit.com` + `old.reddit.com` `.json` unauth legs are
**not** part of the default path. A 429 on one mirror cools down only that
host — the chain continues to the next mirror.

Opt-in OAuth (only when you explicitly want it): create a Reddit **script**
app and set `REDDIT_AUTH_MODE=script` **plus** all four creds. The chain
then tries `oauth.reddit.com` (Bearer, cached token, ~1 req/s throttle
honoring `X-Ratelimit-Remaining/Reset`) first, then the same mirror chain
(plus legacy `.json` legs before RSS). A failed token fetch never fails
hard — it logs a warning and falls back to the mirrors plus the existing
429 cooldown (`Retry-After` on 429 from the OAuth host is honored too).

Create the app (logged in as your Reddit user):

1. Go to https://www.reddit.com/prefs/apps and click **create another
   app…**.
2. Choose type **script**, any name (e.g. `redditv`), any redirect URI
   (e.g. `http://localhost:3000`).
3. Note the **client ID** (under the app name) and the **secret**.

Configure (see `.env.example`) — only needed when opting into OAuth:

```sh
REDDIT_CLIENT_ID=<app id>
REDDIT_CLIENT_SECRET=<app secret>
REDDIT_USERNAME=<your reddit username>
REDDIT_PASSWORD=<your reddit password>
# Optional overrides:
REDDIT_USER_AGENT="linux:redditv:0.1.0 by /u/<username>"  # default when unset
REDDIT_AUTH_MODE=script   # default is no-auth (mirrors); `script` opts into OAuth when creds are set
```

Verify:

- At startup the server logs `reddit auth mode` with `auth_mode=script`
  (or `no-auth` when creds are absent / mode is `none`).
- With `RUST_LOG=debug`, a successful authenticated fetch logs
  `chain="fallback" winner="oauth"` (or `old-public`/`rss` fallbacks).
- Live check: `./scripts/check-feeds.sh [base_url]` (defaults to
  `http://localhost:3000`).
