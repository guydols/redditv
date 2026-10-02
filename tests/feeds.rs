//! Feed smoke tests: spin the compiled server binary in-process (no test-side
//! router import) on a random localhost port and assert HTTP behaviour.
//!
//! Offline-safe: `cargo test` passes without Reddit access. The live upstream
//! test is `#[ignore]`d; run with `cargo test -- --ignored`.

use std::process::{Child, Command};
use std::time::Duration;

fn bin_path() -> String {
    // Set automatically by cargo for integration tests of a binary target.
    option_env!("CARGO_BIN_EXE_reddittv")
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            // Fallback for manual rustc runs: assume debug build exists.
            let p = format!(
                "{}/target/debug/reddittv",
                env!("CARGO_MANIFEST_DIR")
            );
            assert!(
                std::path::Path::new(&p).exists(),
                "server binary not found at {p}; run `cargo build` first"
            );
            p
        })
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
    l.local_addr().unwrap().port()
}

struct TestServer {
    child: Child,
    base: String,
}

impl TestServer {
    fn spawn() -> Self {
        let port = free_port();
        let mut child = Command::new(bin_path())
            .env("PORT", port.to_string())
            // Stealth browser stays off in tests: API-only (serve stale/error).
            .env("CHROME_DISABLED", "1")
            // Avoid picking up developer OAuth creds during offline tests.
            .env_remove("REDDIT_CLIENT_ID")
            .env_remove("REDDIT_CLIENT_SECRET")
            .env_remove("REDDIT_SECRET")
            .env_remove("REDDIT_USERNAME")
            .env_remove("REDDIT_USER")
            .env_remove("REDDIT_USERNAME_OVERRIDE")
            .env_remove("REDDIT_PASSWORD")
            .current_dir(env!("CARGO_MANIFEST_DIR")) // ServeDir("static") is cwd-relative
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to spawn server binary");
        let base = format!("http://127.0.0.1:{port}");
        // Poll /healthz until ready (max ~10s).
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            match client.get(format!("{base}/healthz")).send() {
                Ok(r) if r.status().is_success() => break,
                _ => {
                    if std::time::Instant::now() > deadline {
                        let _ = child.kill();
                        panic!("server did not become ready at {base}/healthz");
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            }
        }
        Self { child, base }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(25))
        .build()
        .unwrap()
}

#[test]
fn healthz_returns_200_ok_true() {
    let srv = TestServer::spawn();
    let r = client().get(format!("{}/healthz", srv.base)).send().unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().unwrap();
    assert_eq!(body, serde_json::json!({"ok": true}));
}

#[test]
fn subs_returns_known_list_containing_videos() {
    let srv = TestServer::spawn();
    let r = client()
        .get(format!("{}/api/subs", srv.base))
        .send()
        .unwrap();
    assert_eq!(r.status(), 200);
    let body: serde_json::Value = r.json().unwrap();
    let subs = body
        .get("subs")
        .and_then(|v| v.as_array())
        .expect("expected {\"subs\": [...]}");
    assert!(!subs.is_empty(), "subs list should not be empty");
    assert!(
        subs.iter().any(|s| s.as_str() == Some("videos")),
        "subs list should contain \"videos\", got {body}"
    );
}

#[test]
fn bad_sub_returns_400() {
    let srv = TestServer::spawn();
    // Invalid chars (and too short) must be rejected without hitting upstream.
    for bad in ["x", "bad sub!", "../etc", "a"] {
        let r = client()
            .get(format!("{}/api/videos?sub={bad}&limit=5", srv.base))
            .send()
            .unwrap();
        assert_eq!(r.status(), 400, "sub={bad:?} should be 400");
        let body: serde_json::Value = r.json().unwrap();
        assert_eq!(
            body.get("error").and_then(|e| e.get("code")).and_then(|c| c.as_str()),
            Some("BAD_SUB"),
            "expected BAD_SUB error code, got {body}"
        );
    }
}

#[test]
fn bad_limit_returns_400() {
    let srv = TestServer::spawn();
    for bad in ["0", "500", "-1", "abc"] {
        let r = client()
            .get(format!(
                "{}/api/videos?sub=videos&limit={bad}",
                srv.base
            ))
            .send()
            .unwrap();
        assert_eq!(r.status(), 400, "limit={bad:?} should be 400");
        let body: serde_json::Value = r.json().unwrap();
        assert_eq!(
            body.get("error").and_then(|e| e.get("code")).and_then(|c| c.as_str()),
            Some("BAD_LIMIT"),
            "expected BAD_LIMIT error code, got {body}"
        );
    }
}

/// Fast-switch contract: GET /api/videos never blocks on the browser.
/// A stale/missing cache returns immediately (200) with the current slice
/// (possibly empty) plus loading:true while the background job runs, and an
/// optional retryAfterMs hint when the 45s nav gate is hot. Offline-safe:
/// allows 200 or 429/502 like the shape test.
#[test]
fn videos_first_request_is_instant_with_loading_flag() {
    let srv = TestServer::spawn();
    let started = std::time::Instant::now();
    let r = client()
        .get(format!("{}/api/videos?sub=videos&limit=5", srv.base))
        .send()
        .unwrap();
    let elapsed = started.elapsed();
    let status = r.status().as_u16();
    let body: serde_json::Value = r.json().unwrap();
    if status == 200 {
        assert!(
            body.get("loading").and_then(|l| l.as_bool()).is_some(),
            "loading must be bool, got {body}"
        );
        assert!(
            body.get("cached").and_then(|c| c.as_bool()).is_some(),
            "cached must be bool, got {body}"
        );
        assert!(
            body.get("stale").and_then(|s| s.as_bool()).is_some(),
            "stale must be bool, got {body}"
        );
        if let Some(ms) = body.get("retryAfterMs") {
            assert!(ms.is_null() || ms.is_number(), "retryAfterMs must be null|number, got {body}");
        }
        // Instant: must not block on a 45s nav gate / browser dwell.
        assert!(
            elapsed < Duration::from_secs(20),
            "switch blocked {elapsed:?}; must serve stale/loading immediately, got {body}"
        );
    } else {
        assert!(
            [429, 502].contains(&status),
            "expected 200 instant slice or 429/502, got {status}: {body}"
        );
    }
}

/// `after` stays an opaque index cursor: numeric, empty, and legacy t3_*
// values all keep the 200 shape. Offline-safe (200 or 429/502).
#[test]
fn videos_after_cursor_paging_keeps_shape() {
    let srv = TestServer::spawn();
    for after in ["", "0", "2", "t3_abc123"] {
        let r = client()
            .get(format!(
                "{}/api/videos?sub=videos&limit=5&after={after}",
                srv.base
            ))
            .send()
            .unwrap();
        let status = r.status().as_u16();
        let body: serde_json::Value = r.json().unwrap();
        if status == 200 {
            assert!(
                body.get("videos").and_then(|v| v.as_array()).is_some(),
                "expected videos[] array, got {body}"
            );
            if let Some(a) = body.get("after") {
                assert!(a.is_null() || a.is_string(), "after must be string|null, got {body}");
            }
            assert!(
                body.get("hasMore").and_then(|h| h.as_bool()).is_some(),
                "hasMore must be bool, got {body}"
            );
            assert!(
                body.get("loading").and_then(|l| l.as_bool()).is_some(),
                "loading must be bool, got {body}"
            );
        } else {
            assert!(
                [429, 502].contains(&status),
                "expected 200 or 429/502, got {status}: {body}"
            );
        }
    }
}

/// No-duplicate merge across pages: page 2 (via the `after` index cursor)
/// must not repeat youtubeIds from page 1. Offline-safe: skips when either
/// page is empty (cold cache returns loading:true with []).
#[test]
fn videos_pages_have_no_duplicate_youtube_ids() {
    let srv = TestServer::spawn();
    let get = |url: String| -> Option<serde_json::Value> {
        let r = client().get(&url).send().unwrap();
        if r.status() != 200 {
            return None;
        }
        Some(r.json().unwrap())
    };
    let p1 = match get(format!("{}/api/videos?sub=videos&limit=10", srv.base)) {
        Some(b) => b,
        None => return,
    };
    let after = p1.get("after").and_then(|a| a.as_str()).unwrap_or("");
    if after.is_empty() {
        return; // tail or cold cache: nothing to compare
    }
    let p2 = match get(format!(
        "{}/api/videos?sub=videos&limit=10&after={after}",
        srv.base
    )) {
        Some(b) => b,
        None => return,
    };
    let ids = |b: &serde_json::Value| -> Vec<String> {
        b.get("videos")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.get("youtubeId").and_then(|x| x.as_str()).map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let a = ids(&p1);
    let b = ids(&p2);
    if a.is_empty() || b.is_empty() {
        return; // cold/loading cache: vacuous pass
    }
    let set: std::collections::HashSet<&str> = a.iter().map(|s| s.as_str()).collect();
    for id in &b {
        assert!(!set.contains(id.as_str()), "duplicate youtubeId {id} across pages");
    }
}

/// Offline-tolerant shape test: accepts either a 200 feed payload or an
/// upstream-failure payload (sandbox blocks Reddit with 403/429 -> 502/429).
/// Asserts JSON types either way so `cargo test` passes offline.
#[test]
fn videos_shape_allows_empty_but_asserts_types() {
    let srv = TestServer::spawn();
    let r = client()
        .get(format!("{}/api/videos?sub=videos&limit=5", srv.base))
        .send()
        .unwrap();
    let status = r.status().as_u16();
    let body: serde_json::Value = r.json().unwrap();
    if status == 200 {
        assert_eq!(
            body.get("sub").and_then(|s| s.as_str()),
            Some("videos"),
            "expected sub echo, got {body}"
        );
        let videos = body
            .get("videos")
            .and_then(|v| v.as_array())
            .unwrap_or_else(|| panic!("expected videos[] array, got {body}"));
        for v in videos {
            assert!(v.get("youtubeId").and_then(|x| x.as_str()).is_some(), "missing youtubeId: {v}");
            assert!(v.get("youtubeUrl").and_then(|x| x.as_str()).is_some(), "missing youtubeUrl: {v}");
            assert!(v.get("title").and_then(|x| x.as_str()).is_some(), "missing title: {v}");
            assert!(v.get("redditUrl").and_then(|x| x.as_str()).is_some(), "missing redditUrl: {v}");
            assert!(v.get("thumbnail").and_then(|x| x.as_str()).is_some(), "missing thumbnail: {v}");
            // createdUtc is Option<i64>: null or number only.
            if let Some(c) = v.get("createdUtc") {
                assert!(c.is_null() || c.is_number(), "createdUtc must be null|number: {v}");
            }
        }
        // `after` is string|null; `hasMore` is bool.
        if let Some(a) = body.get("after") {
            assert!(a.is_null() || a.is_string(), "after must be string|null, got {body}");
        }
        assert!(
            body.get("hasMore").and_then(|h| h.as_bool()).is_some(),
            "hasMore must be bool, got {body}"
        );
        // Timeline cache fields.
        assert!(
            body.get("cached").and_then(|c| c.as_bool()).is_some(),
            "cached must be bool, got {body}"
        );
        assert!(
            body.get("stale").and_then(|s| s.as_bool()).is_some(),
            "stale must be bool, got {body}"
        );
        if let Some(f) = body.get("fetchedAt") {
            assert!(f.is_null() || f.is_number(), "fetchedAt must be null|number, got {body}");
        }
    } else {
        // Offline/sandbox path: upstream blocked -> JSON error envelope.
        assert!(
            [429, 502].contains(&status),
            "expected 200 feed or 429/502 upstream failure, got {status}: {body}"
        );
        let code = body
            .get("error")
            .and_then(|e| e.get("code"))
            .and_then(|c| c.as_str())
            .unwrap_or("");
        assert!(
            ["UPSTREAM_403", "UPSTREAM_429", "UPSTREAM_5xx", "UPSTREAM_BLOCKED", "RATE_LIMITED"].contains(&code),
            "expected upstream error code, got {body}"
        );
        eprintln!("offline-tolerant: upstream unreachable (status {status}, code {code})");
    }
}

/// The `refresh` query param is accepted and keeps the same response shape.
/// Offline-safe: allows 200 or 429/502 like the shape test above.
#[test]
fn videos_refresh_param_keeps_shape() {
    let srv = TestServer::spawn();
    for refresh in ["false", "true"] {
        let r = client()
            .get(format!(
                "{}/api/videos?sub=videos&limit=5&refresh={refresh}",
                srv.base
            ))
            .send()
            .unwrap();
        let status = r.status().as_u16();
        let body: serde_json::Value = r.json().unwrap();
        if status == 200 {
            assert!(
                body.get("cached").and_then(|c| c.as_bool()).is_some(),
                "cached must be bool, got {body}"
            );
            assert!(
                body.get("stale").and_then(|s| s.as_bool()).is_some(),
                "stale must be bool, got {body}"
            );
            if let Some(f) = body.get("fetchedAt") {
                assert!(f.is_null() || f.is_number(), "fetchedAt must be null|number, got {body}");
            }
        } else {
            assert!(
                [429, 502].contains(&status),
                "expected 200 or 429/502, got {status}: {body}"
            );
            assert!(
                body.get("error").and_then(|e| e.get("code")).is_some(),
                "expected error envelope, got {body}"
            );
        }
    }
}

/// POST /api/refresh triggers a force refresh under the same limiter.
/// Offline-safe: 200 snapshot or 429/502 error envelope.
#[test]
fn refresh_endpoint_offline_safe() {
    let srv = TestServer::spawn();
    let r = client()
        .post(format!("{}/api/refresh?sub=videos", srv.base))
        .send()
        .unwrap();
    let status = r.status().as_u16();
    let body: serde_json::Value = r.json().unwrap();
    if status == 200 {
        assert_eq!(
            body.get("sub").and_then(|s| s.as_str()),
            Some("videos"),
            "expected sub echo, got {body}"
        );
        // First fetch on an empty timeline is non-blocking: queued snapshot
        // with loading:true (background job fills the timeline).
        if body.get("queued").is_some() {
            assert!(
                body.get("cached").and_then(|c| c.as_bool()).is_some(),
                "cached must be bool, got {body}"
            );
            assert!(
                body.get("stale").and_then(|s| s.as_bool()).is_some(),
                "stale must be bool, got {body}"
            );
            assert!(
                body.get("loading").and_then(|l| l.as_bool()).is_some(),
                "loading must be bool, got {body}"
            );
            if let Some(f) = body.get("fetchedAt") {
                assert!(f.is_null() || f.is_number(), "fetchedAt must be null|number, got {body}");
            }
        } else {
            // Blocking first-fetch path: same shape as GET /api/videos.
            assert!(
                body.get("videos").and_then(|v| v.as_array()).is_some(),
                "expected videos[] array, got {body}"
            );
        }
    } else {
        assert!(
            [400, 429, 502].contains(&status),
            "expected 200/400/429/502, got {status}: {body}"
        );
        assert!(
            body.get("error").and_then(|e| e.get("code")).is_some(),
            "expected error envelope, got {body}"
        );
    }
    // Bad sub still 400.
    let r = client()
        .post(format!("{}/api/refresh?sub=bad%20sub!", srv.base))
        .send()
        .unwrap();
    assert_eq!(r.status(), 400);
}

/// Live check: hits real Reddit through the local server. Requires network.
/// Non-blocking API: the first GET queues a background fetch and returns
/// loading:true, so this polls until videos arrive (up to ~90s).
/// Run with: `cargo test -- --ignored`
#[test]
#[ignore]
fn live_reddit_videos_returns_rows() {
    let srv = TestServer::spawn();
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let r = client()
            .get(format!("{}/api/videos?sub=videos&limit=5", srv.base))
            .send()
            .unwrap();
        let status = r.status();
        let body: serde_json::Value = r.json().unwrap();
        let count = body
            .get("videos")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        if status == 200 && count > 0 {
            println!("live feed ok: {} rows, after={}", count, body.get("after").unwrap_or(&serde_json::Value::Null));
            return;
        }
        if std::time::Instant::now() > deadline {
            panic!(
                "live upstream request failed (sandbox may block Reddit 403/429): status={status} {body}"
            );
        }
        eprintln!("live poll: status={status} rows={count} loading={} — retrying…", body.get("loading").unwrap_or(&serde_json::Value::Null));
        std::thread::sleep(Duration::from_secs(3));
    }
}
