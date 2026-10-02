Reddit Video Player

## Logs (diagnosing slow loads)

The server logs every API request (method, route, sub/limit/after/refresh,
status, elapsed ms, served-from, videos count, loading/hasMore) plus the
scrape lifecycle (head-refresh start/end with winner leg + rows, paginate
start/end, nav-gate misses, single-flight coalescing, fills and blocks).
Per-leg HTTP/browser details (public/RSS/OAuth legs, browser nav/selector/
extract) are at `debug` level. No PII beyond subreddit names is logged.

```sh
cargo run                  # default: info (requests + scrape start/end)
RUST_LOG=debug cargo run   # verbose: per-leg scrape details
RUST_LOG=reddittv=debug,tower_http=debug cargo run  # same, scoped
```
