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
            ["UPSTREAM_403", "UPSTREAM_429", "UPSTREAM_5xx"].contains(&code),
            "expected upstream error code, got {body}"
        );
        eprintln!("offline-tolerant: upstream unreachable (status {status}, code {code})");
    }
}

/// Live check: hits real Reddit through the local server. Requires network.
/// Run with: `cargo test -- --ignored`
#[test]
#[ignore]
fn live_reddit_videos_returns_rows() {
    let srv = TestServer::spawn();
    let r = client()
        .get(format!("{}/api/videos?sub=videos&limit=5", srv.base))
        .send()
        .unwrap();
    let status = r.status();
    let body: serde_json::Value = r.json().unwrap();
    assert_eq!(
        status,
        200,
        "live upstream request failed (sandbox may block Reddit 403/429): {body}"
    );
    let videos = body
        .get("videos")
        .and_then(|v| v.as_array())
        .expect("expected videos[] array");
    assert!(
        !videos.is_empty(),
        "live feed returned zero rows: {body}"
    );
    println!("live feed ok: {} rows, after={}", videos.len(), body.get("after").unwrap_or(&serde_json::Value::Null));
}
