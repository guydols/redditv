//! Live stealth-path check: boots the server binary WITHOUT CHROME_DISABLED
//! so the chromiumoxide stealth browser path is exercised.
//!
//! Polite by design: single navigation, small sub (`videos`), limit 5.
//! Always `#[ignore]` so `cargo test` stays offline-green.
//! Run with: `cargo test --test stealth_live -- --ignored --nocapture`
//!
//! Outcome contract (documents reality, doesn't fail on environment blocks):
//! - 200 with videos[] non-empty           -> PASS (stealth path served rows)
//! - 429 RATE_LIMITED / 502 UPSTREAM_BLOCKED -> PASS as "environment-blocked"
//!   (prints body, asserts the error code is present, does not fail as a code bug)
//! - Chrome binary missing                  -> PASS as "skipped" (prints why, returns early)

use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;

fn bin_path() -> String {
    option_env!("CARGO_BIN_EXE_reddittv")
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            let p = format!("{}/target/debug/reddittv", env!("CARGO_MANIFEST_DIR"));
            assert!(
                Path::new(&p).exists(),
                "server binary not found at {p}; run `cargo build` first"
            );
            p
        })
}

/// True when a Chrome/Chromium binary looks available for chromiumoxide to launch.
fn chrome_present() -> bool {
    // Explicit override paths first.
    for key in ["CHROME", "CHROME_BIN", "CHROME_PATH"] {
        if let Ok(p) = std::env::var(key) {
            if !p.is_empty() && Path::new(&p).exists() {
                return true;
            }
        }
    }
    // Well-known absolute locations.
    for abs in [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/opt/google/chrome/chrome",
        "/snap/bin/chromium",
    ] {
        if Path::new(abs).exists() {
            return true;
        }
    }
    // PATH search for common binary names.
    let candidates = [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "chrome",
        "headless_shell",
    ];
    let path_var = std::env::var_os("PATH").unwrap_or_default();
    for dir in std::env::split_paths(&path_var) {
        for c in &candidates {
            if dir.join(c).exists() {
                return true;
            }
        }
    }
    false
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
    /// Spawn WITHOUT CHROME_DISABLED so the real stealth browser path is live.
    /// Also strips the test-only CHROME_DISABLED and developer OAuth creds are
    /// left intact (live upstream may benefit); OAuth absence is fine too.
    fn spawn_stealth() -> Self {
        let port = free_port();
        let mut cmd = Command::new(bin_path());
        cmd.env("PORT", port.to_string())
            .env_remove("CHROME_DISABLED")
            .current_dir(env!("CARGO_MANIFEST_DIR")) // ServeDir("static") is cwd-relative
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null());
        let mut child = cmd.spawn().expect("failed to spawn server binary");
        let base = format!("http://127.0.0.1:{port}");
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
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
    // Non-blocking API: first stealth navigation runs in the background
    // (fast ~7s caps + dwell, raced with HTTP); poll with a short per-request timeout.
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(25))
        .build()
        .unwrap()
}

/// Live stealth check: single polite navigation (r/videos, limit 5).
/// See module docs for the pass/blocked/skip contract.
/// Non-blocking API: the first GET returns loading:true while the browser
/// job runs, so poll until videos arrive (up to ~120s).
#[test]
#[ignore]
fn stealth_live_videos_single_navigation() {
    if !chrome_present() {
        eprintln!("SKIP stealth_live: no Chrome/Chromium binary found in PATH or well-known locations; stealth browser cannot launch here.");
        return;
    }
    let srv = TestServer::spawn_stealth();
    let url = format!("{}/api/videos?sub=videos&limit=5&refresh=true", srv.base);
    eprintln!("GET {url} (single stealth navigation, polling)");
    let deadline = std::time::Instant::now() + Duration::from_secs(120);
    loop {
        let r = client().get(&url).send().expect("request to test server failed");
        let status = r.status().as_u16();
        let body: serde_json::Value = r.json().expect("expected JSON body");
        let count = body
            .get("videos")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        println!("stealth_live poll status={status} rows={count} body={body}");
        if status == 200 && count > 0 {
            println!(
                "stealth_live PASS: {} rows, after={}",
                count,
                body.get("after").unwrap_or(&serde_json::Value::Null)
            );
            return;
        }
        if [429, 502].contains(&status) {
            let code = body
                .get("error")
                .and_then(|e| e.get("code"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            // Environment-blocked outcomes: document, don't fail as a code bug.
            // Only stop polling early on explicit rate-limit/blocked codes
            // after at least one attempt; loading:true + 200/empty keeps polling.
            if ["RATE_LIMITED", "UPSTREAM_429", "UPSTREAM_BLOCKED"].contains(&code)
                && body.get("videos").is_none()
            {
                println!("stealth_live ENVIRONMENT-BLOCKED (not a code bug): status={status} code={code}");
                return;
            }
        }
        if std::time::Instant::now() > deadline {
            assert!(
                [429, 502].contains(&status),
                "expected 200 rows or 429/502 environment-blocked, got {status}: {body}"
            );
            let code = body
                .get("error")
                .and_then(|e| e.get("code"))
                .and_then(|c| c.as_str())
                .unwrap_or("");
            assert!(
                ["RATE_LIMITED", "UPSTREAM_429", "UPSTREAM_BLOCKED"].contains(&code),
                "expected RATE_LIMITED / UPSTREAM_429 / UPSTREAM_BLOCKED error code, got {body}"
            );
            println!("stealth_live ENVIRONMENT-BLOCKED (not a code bug): status={status} code={code}");
            return;
        }
        std::thread::sleep(Duration::from_secs(4));
    }
}
