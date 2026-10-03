use axum::{
    Json, Router,
    extract::{MatchedPath, Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chromiumoxide::{Browser, BrowserConfig};
use dashmap::DashMap;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tower_http::trace::TraceLayer;
use tracing::{debug, info, warn};
use std::{
    collections::{HashSet, VecDeque},
    sync::{Arc, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, RwLock};
use tower_http::services::{ServeDir, ServeFile};

const DEFAULT_UA: &str = "linux:redditv:0.1.0 (by /u/redditv-dev)";
/// Pinned desktop Chrome UA used for the headless browser.
const CHROME_UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";
const DEFAULT_SUBS: &[&str] = &[
    "videos",
    "ArtisanVideos",
    "DeepIntoYouTube",
    "educationalvideos",
    "youtubehaiku",
    "curiousvideos",
    "documentaries",
    "mealtimevideos",
    "CookingVideos",
    "shortfilms",
    "ted",
    "StandUpComedy",
    "ObscureMedia",
    "listentothis",
    "livemusic",
    "fullmoviesonyoutube",
    "lectures",
];
const PLACEHOLDER_THUMB: &str =
    "https://via.placeholder.com/200x112/333/666?text=No+Thumbnail";

/// Timeline TTL: slices are served from memory for 1h.
const TTL_MS: i64 = 3_600_000;
/// Max posts kept per sub timeline, newest-first.
const MAX_POSTS: usize = 500;
/// Minimum gap between two browser navigations (+ jitter 0-15s).
const MIN_NAV_GAP: Duration = Duration::from_secs(45);
/// Priority lane for never-fetched subs: only this short gap applies so a
/// sub switch fetches then and there instead of showing a countdown.
const PRIORITY_NAV_GAP: Duration = Duration::from_secs(6);
/// Fast-path caps so a sub switch never blocks on the browser: first paint
/// uses short navigation/selector timeouts and a ~2s dwell. Longer dwells
/// are reserved for background pagination jobs.
const FAST_NAV_TIMEOUT: Duration = Duration::from_secs(7);
const FAST_SELECTOR_TIMEOUT: Duration = Duration::from_secs(7);
const FAST_DWELL: Duration = Duration::from_secs(2);
/// Fast path never queues behind a busy browser: if the shared browser cell
/// is contended longer than this, skip straight to HTTP (browser stays
/// reserved for background enrichment only).
const FAST_BROWSER_LOCK_TIMEOUT: Duration = Duration::from_secs(1);
/// Fast first-paint budgets: each HTTP leg gets this long on a never-fetched
/// sub so the background fill lands in ~4s instead of stacking sequential
/// 10s reqwest timeouts (public -> OAuth token POST -> OAuth fetch -> RSS).
/// Longer budgets stay reserved for background enrichment/pagination.
const FAST_HTTP_LEG_TIMEOUT: Duration = Duration::from_secs(4);
/// When a slice ends within this many items of the tail, serve it now and
/// prefetch the next reddit page in the background.
const PREFETCH_TAIL_THRESHOLD: usize = 5;
/// Sliding-window cap on browser navigations per hour.
const MAX_NAVS_PER_HOUR: usize = 35;
/// Backoff for a sub after a block/challenge was detected (~3h, within 2-4h).
const BLOCKED_BACKOFF_MS: i64 = 3 * 3_600_000;
/// Short backoff for transient/single-leg blocks on an empty timeline: the
/// feed stays retryable instead of a 3h blackout (~5min).
const SHORT_BLOCKED_BACKOFF_MS: i64 = 5 * 60_000;
/// Negative-cache cooldown after an upstream 429 (rate-limit) for a sub.
/// While active, GET /api/videos returns immediately with
/// {videos:[], loading:true, retryAfterMs} without spawning new upstream
/// jobs and without touching nav gates, so frontend repolls cannot extend
/// the ban. Honors the upstream Retry-After header when present.
const UPSTREAM_429_COOLDOWN_MS: i64 = 5 * 60_000;
/// Clamp for a Retry-After-derived 429 cooldown (5s..30min).
const MIN_429_COOLDOWN_MS: i64 = 5_000;
const MAX_429_COOLDOWN_MS: i64 = 30 * 60_000;
/// OAuth politeness: average 1 req/s on oauth.reddit.com (60/min honest use;
/// X-Ratelimit-Remaining/Reset headers are honored when present).
const OAUTH_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// Token expiry skew: treat the bearer token as expired 60s before `expires_in`.
const OAUTH_EXPIRY_SKEW_SECS: u64 = 60;
/// Minimum cached TTL floor so a tiny `expires_in` never hot-loops token POSTs.
const OAUTH_MIN_CACHED_TTL_SECS: u64 = 10;
const OAUTH_TOKEN_URL: &str = "https://www.reddit.com/api/v1/access_token";

/// No-auth mirror chain (default path; no Reddit registration needed).
/// Order: ArcticShift (primary) -> PullPush (secondary) -> Redlib
/// round-robin -> RSS -> stealth-browser old.reddit HTML. Direct
/// api/www.reddit.com + old.reddit.com `.json` unauth legs are NOT part of
/// the default path; they only run when REDDIT_AUTH_MODE=script with explicit
/// script creds (opt-in OAuth).
const ARCTIC_BASE: &str = "https://arctic-shift.photon-reddit.com";
/// Couple req/s max on ArcticShift: 500ms global gap + X-RateLimit-Reset.
const ARCTIC_MIN_INTERVAL: Duration = Duration::from_millis(500);
const PULLPUSH_BASE: &str = "https://api.pullpush.io";
/// PullPush soft limit (~15/min): 4s minimum gap, backoff, never sole source.
const PULLPUSH_MIN_INTERVAL: Duration = Duration::from_secs(4);
/// Redlib round-robin mirrors (same listing schema as Reddit, with
/// thumbnail+permalink). Rotated on 429/403; a 429 on one host never blocks
/// the others (per-host cooldowns).
const REDLIB_HOSTS: &[&str] = &[
    "safereddit.com",
    "redlib.catsarch.com",
    "redlib.r4fo.com",
    "redlib.cow.rip",
];
/// Global politeness per Redlib host.
const REDLIB_MIN_INTERVAL: Duration = Duration::from_secs(1);

// ---------- state ----------

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    timelines: Arc<DashMap<String, Arc<RwLock<SubTimeline>>>>,
    inflight: Arc<DashMap<String, Arc<Mutex<()>>>>,
    paginate_inflight: Arc<DashMap<String, Arc<Mutex<()>>>>,
    nav_gate: Arc<Mutex<NavGate>>,
    oauth: Arc<OAuthState>,
    mirrors: Arc<MirrorState>,
}

#[derive(Default)]
struct SubTimeline {
    posts: Vec<VideoItem>,
    /// Epoch ms of last successful fetch; None = never fetched.
    fetched_at_ms: Option<i64>,
    last_error: Option<String>,
    /// Epoch ms until which this sub is backed off (after BLOCKED).
    blocked_until_ms: Option<i64>,
    /// Epoch ms until which this sub is 429-cooled-down (negative cache).
    /// While in the future, handlers return loading+retryAfterMs without
    /// spawning upstream jobs.
    rate_limited_until_ms: Option<i64>,
    /// old.reddit pagination cursor (t3_*) for the tail of `posts`.
    /// None = no further pages known yet.
    reddit_after: Option<String>,
    /// True while a background fetch/paginate job is running for this sub.
    loading: bool,
}

#[derive(Default)]
struct NavGate {
    last_nav: Option<Instant>,
    /// Timestamps of recent navigations (sliding 1h window).
    nav_times: VecDeque<Instant>,
}

/// Shared OAuth state: cached bearer token (with skew expiry), a
/// single-flight refresh lock, and a global 1 req/s throttle for the
/// oauth.reddit.com host (plus X-Ratelimit-Remaining/Reset tracking).
#[derive(Default)]
struct OAuthState {
    cached: Mutex<Option<CachedToken>>,
    /// Held across the whole token POST so concurrent fetches coalesce
    /// onto one refresh (single-flight).
    refresh: Mutex<()>,
    /// Last oauth.reddit.com request start; enforces OAUTH_MIN_INTERVAL.
    last_req: Mutex<Option<Instant>>,
    /// Last seen X-Ratelimit-Remaining / X-Ratelimit-Reset (seconds).
    ratelimit_remaining: Mutex<Option<f64>>,
    ratelimit_reset_at: Mutex<Option<Instant>>,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
}

impl CachedToken {
    fn valid(&self) -> bool {
        Instant::now() < self.expires_at
    }
}

/// Per-mirror politeness + per-host negative cache for the no-auth chain.
/// A 429/cooldown on one mirror host never blocks the others: cooldown keys
/// are per-host (`arctic`, `pullpush`, `redlib:<host>`, `rss`), while the
/// sub-level `rate_limited_until_ms` negative cache still applies only when
/// every mirror leg reports 429.
#[derive(Default)]
struct MirrorState {
    arctic_last: Mutex<Option<Instant>>,
    pullpush_last: Mutex<Option<Instant>>,
    redlib_last: Mutex<Option<Instant>>,
    /// Round-robin start index into REDLIB_HOSTS.
    redlib_idx: Mutex<usize>,
    /// Per-host cooldown until-epoch-ms (negative cache). Keyed by
    /// `arctic` / `pullpush` / `redlib:<host>` / `rss`.
    cooldowns: DashMap<String, i64>,
}

/// Effective TTL for a token: `expires_in` minus the 60s skew, floored so a
/// tiny `expires_in` never hot-loops token POSTs. Pure for offline tests.
fn token_ttl_secs(expires_in: u64) -> u64 {
    expires_in
        .saturating_sub(OAUTH_EXPIRY_SKEW_SECS)
        .max(OAUTH_MIN_CACHED_TTL_SECS)
}

/// Absolute expiry for a token fetched now. Pure wrapper for tests.
fn token_expiry_for(expires_in: u64) -> Instant {
    Instant::now() + Duration::from_secs(token_ttl_secs(expires_in))
}

/// True when the cached expiry is still in the future (i.e. usable).
/// Pure helper over epoch millis so unit tests stay offline-safe.
#[cfg(test)]
fn token_cache_valid(expires_at_ms: i64, now_ms: i64) -> bool {
    now_ms < expires_at_ms
}

#[derive(Debug, Deserialize)]
struct VideosQuery {
    sub: Option<String>,
    limit: Option<String>,
    after: Option<String>,
    refresh: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RefreshQuery {
    sub: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VideoItem {
    #[serde(rename = "youtubeId")]
    youtube_id: String,
    #[serde(rename = "youtubeUrl")]
    youtube_url: String,
    title: String,
    #[serde(rename = "redditUrl")]
    reddit_url: String,
    thumbnail: String,
    #[serde(rename = "createdUtc")]
    created_utc: Option<i64>,
    /// old.reddit post id (t3_*) used for pagination dedupe. Optional so
    /// older cached rows and RSS fallbacks without an id still decode.
    #[serde(rename = "redditId", default)]
    reddit_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScrapedRow {
    #[serde(rename = "youtubeId", default)]
    youtube_id: String,
    #[serde(rename = "youtubeUrl", default)]
    youtube_url: String,
    #[serde(default)]
    title: String,
    #[serde(rename = "redditUrl", default)]
    reddit_url: String,
    #[serde(default)]
    thumbnail: String,
    #[serde(rename = "createdUtc", default)]
    created_utc: Option<i64>,
    #[serde(rename = "redditId", default)]
    reddit_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScrapePage {
    #[serde(default)]
    rows: Vec<ScrapedRow>,
    #[serde(rename = "nextHref", default)]
    next_href: Option<String>,
}

// ---------- helpers ----------

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 0-15s jitter in ms derived from current time (no extra deps).
fn jitter_15s_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| (d.subsec_nanos() % 15_000) as u64)
        .unwrap_or(0)
}

/// Script-app User-Agent: `<platform>:<appID>:<version> by /u/<username>`.
/// Prefers REDDIT_USER_AGENT, then the legacy REDDIT_UA, else builds the
/// default from the configured script username when known.
fn script_default_ua(username: Option<&str>) -> String {
    match username {
        Some(u) if !u.is_empty() => format!("linux:redditv:0.1.0 by /u/{}", u),
        _ => DEFAULT_UA.to_string(),
    }
}

fn script_username() -> Option<String> {
    std::env::var("REDDIT_USERNAME")
        .or_else(|_| std::env::var("REDDIT_USER"))
        .or_else(|_| std::env::var("REDDIT_USERNAME_OVERRIDE"))
        .ok()
        .filter(|s| !s.is_empty())
}

fn reddit_ua() -> String {
    if let Ok(ua) = std::env::var("REDDIT_USER_AGENT") {
        if !ua.is_empty() {
            return ua;
        }
    }
    if let Ok(ua) = std::env::var("REDDIT_UA") {
        if !ua.is_empty() {
            return ua;
        }
    }
    script_default_ua(script_username().as_deref())
}

/// When set (e.g. `CHROME_DISABLED=1` in CI/tests), never launch the browser;
/// serve from memory + reqwest fallback only.
fn chrome_disabled() -> bool {
    match std::env::var("CHROME_DISABLED") {
        Ok(v) => v == "1" || v.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

fn is_valid_sub(sub: &str) -> bool {
    let len = sub.len();
    if len < 2 || len > 21 {
        return false;
    }
    sub.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn error_json(status: StatusCode, code: &str, message: String, retry_after_ms: Option<u64>) -> Response {
    let mut err = serde_json::Map::new();
    err.insert("code".to_string(), serde_json::Value::String(code.to_string()));
    err.insert(
        "message".to_string(),
        serde_json::Value::String(message),
    );
    if let Some(ms) = retry_after_ms {
        err.insert(
            "retryAfterMs".to_string(),
            serde_json::Value::Number(ms.into()),
        );
    }
    let mut root = serde_json::Map::new();
    root.insert("error".to_string(), serde_json::Value::Object(err));
    (status, Json(serde_json::Value::Object(root))).into_response()
}

fn retry_after_ms(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(|secs| secs.saturating_mul(1000))
}

/// Negative-cache cooldown duration for an upstream 429. Honors the
/// upstream Retry-After hint when present, else the 5min default.
/// Clamped to 5s..30min so a stray header cannot pin the sub forever.
fn upstream_429_cooldown_ms(retry_after: Option<u64>) -> i64 {
    match retry_after {
        Some(ms) if ms > 0 => (ms as i64).clamp(MIN_429_COOLDOWN_MS, MAX_429_COOLDOWN_MS),
        _ => UPSTREAM_429_COOLDOWN_MS,
    }
}

/// Remaining 429 cooldown for a timeline, if active.
fn rate_limit_remaining_ms(tl: &SubTimeline, now: i64) -> Option<u64> {
    tl.rate_limited_until_ms.and_then(|until| {
        if now < until {
            Some((until - now).max(0) as u64)
        } else {
            None
        }
    })
}

/// Opaque index cursor into the in-memory timeline. Legacy `t3_*` cursors are
/// accepted but restart at index 0 (no reddit-page mapping server-side).
fn parse_after(after: &Option<String>) -> usize {
    match after {
        None => 0,
        Some(s) if s.is_empty() => 0,
        Some(s) => s.parse::<usize>().unwrap_or(0),
    }
}

/// Extract a YouTube video id from a URL. Supports watch?v=, youtu.be/, /shorts/, /embed/.
fn extract_youtube_id(url: &str) -> Option<String> {
    let url = url.replace("&amp;", "&");
    // order matters: check each marker
    for marker in ["youtu.be/", "youtube.com/shorts/", "youtube.com/embed/"] {
        if let Some(pos) = url.find(marker) {
            let rest = &url[pos + marker.len()..];
            let id: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            if id.len() >= 5 {
                let short: String = id.chars().take(11).collect();
                return Some(short);
            }
        }
    }
    // watch?v= (may appear as ?v= or &v=)
    if let Some(pos) = url.find("v=") {
        // ensure it looks like a youtube watch url
        if url[..pos].contains("youtube.com") || url.contains("watch") {
            let rest = &url[pos + 2..];
            let id: String = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            if id.len() >= 5 {
                let short: String = id.chars().take(11).collect();
                return Some(short);
            }
        }
    }
    None
}

/// Given a blob of HTML/text, find the first YouTube URL and its id.
fn extract_youtube_from_text(haystack: &str) -> Option<(String, String)> {
    let lower_markers = ["youtube.com/watch", "youtu.be/", "youtube.com/shorts/", "youtube.com/embed/"];
    let mut best: Option<(usize, &str)> = None;
    for m in lower_markers {
        if let Some(pos) = haystack.find(m) {
            match best {
                Some((bp, _)) if bp <= pos => {}
                _ => best = Some((pos, m)),
            }
        }
    }
    let (pos, _) = best?;
    // walk backwards to find http start
    let start = haystack[..pos].rfind("https://").or_else(|| haystack[..pos].rfind("http://"))?;
    let rest = &haystack[start..];
    let end = rest
        .find(|c: char| c == '"' || c == '\'' || c == '<' || c == '>' || c.is_whitespace())
        .unwrap_or(rest.len());
    let mut url = rest[..end].to_string();
    url = url.replace("&amp;", "&");
    // trim trailing punctuation
    while url.ends_with(|c: char| c == '.' || c == ',' || c == ';' || c == ')' || c == '!' || c == '?') {
        url.pop();
    }
    let id = extract_youtube_id(&url)?;
    Some((url, id))
}

fn clean_url(u: &str) -> String {
    u.replace("&amp;", "&")
}

/// Derive a t3_* reddit post id from a permalink or comments URL.
/// `/r/sub/comments/abc123/slug/` -> `t3_abc123`. Passes through values
/// that already look like `t3_*`. Returns None when no id is found.
fn reddit_id_from_permalink(url: &str) -> Option<String> {
    let url = url.replace("&amp;", "&");
    if let Some(pos) = url.find("t3_") {
        let rest = &url[pos..];
        let id: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        if id.len() > 3 {
            return Some(id);
        }
    }
    if let Some(pos) = url.find("/comments/") {
        let rest = &url[pos + "/comments/".len()..];
        let id: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric())
            .collect();
        if !id.is_empty() {
            return Some(format!("t3_{}", id));
        }
    }
    None
}

/// Extract the `after=t3_*` cursor from an old.reddit next-button href
/// (`?count=25&after=t3_xxx`). Returns None when absent.
fn after_from_next_href(href: Option<&str>) -> Option<String> {
    let href = href?;
    let href = href.replace("&amp;", "&");
    for chunk in href.split(['?', '&']) {
        if let Some(v) = chunk.strip_prefix("after=") {
            let v = v.trim();
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// Filter `incoming` down to items not already present in `existing`.
/// Dedupe keys: youtubeId plus redditId/redditUrl. Order preserved; the
/// caller decides head-prepend (newest refresh) vs tail-append (older page).
fn filter_unseen_older(existing: &[VideoItem], incoming: Vec<VideoItem>) -> Vec<VideoItem> {
    let mut seen_yt: HashSet<String> =
        existing.iter().map(|v| v.youtube_id.clone()).collect();
    let mut seen_reddit: HashSet<String> = existing
        .iter()
        .filter_map(|v| {
            v.reddit_id
                .clone()
                .or_else(|| (!v.reddit_url.is_empty()).then(|| v.reddit_url.clone()))
        })
        .collect();
    let mut out = Vec::new();
    for v in incoming {
        if seen_yt.contains(&v.youtube_id) {
            continue;
        }
        let rkey = v
            .reddit_id
            .clone()
            .or_else(|| (!v.reddit_url.is_empty()).then(|| v.reddit_url.clone()));
        if let Some(k) = &rkey {
            if seen_reddit.contains(k) {
                continue;
            }
        }
        seen_yt.insert(v.youtube_id.clone());
        if let Some(k) = rkey {
            seen_reddit.insert(k);
        }
        out.push(v);
    }
    out
}

/// Merge a freshly fetched newest page at the head: unseen items first
/// (in fetch order), then existing tail. Caps to MAX_POSTS.
fn merge_newest_head(existing: Vec<VideoItem>, fresh_page: Vec<VideoItem>) -> Vec<VideoItem> {
    let unseen = filter_unseen_older(&existing, fresh_page);
    let mut merged = Vec::with_capacity((unseen.len() + existing.len()).min(MAX_POSTS));
    merged.extend(unseen);
    merged.extend(existing);
    dedupe_cap(merged)
}

/// Merge an older next-page at the tail: only unseen items appended,
/// timeline order (newest-first) preserved. Caps to MAX_POSTS.
fn merge_older_tail(existing: Vec<VideoItem>, next_page: Vec<VideoItem>) -> Vec<VideoItem> {
    let unseen = filter_unseen_older(&existing, next_page);
    let mut merged = Vec::with_capacity((existing.len() + unseen.len()).min(MAX_POSTS));
    merged.extend(existing);
    merged.extend(unseen);
    dedupe_cap(merged)
}

fn dedupe_cap(mut items: Vec<VideoItem>) -> Vec<VideoItem> {
    let mut seen: HashSet<String> = HashSet::new();
    items.retain(|v| seen.insert(v.youtube_id.clone()));
    items.truncate(MAX_POSTS);
    items
}

// ---------- reddit JSON types ----------

#[derive(Debug, Deserialize)]
struct RedditListing {
    data: RedditListingData,
}

#[derive(Debug, Deserialize)]
struct RedditListingData {
    after: Option<String>,
    children: Vec<RedditChild>,
}

#[derive(Debug, Deserialize)]
struct RedditChild {
    data: RedditPost,
}

#[derive(Debug, Deserialize, Default)]
struct RedditPost {
    name: Option<String>,
    title: Option<String>,
    url: Option<String>,
    url_overridden_by_dest: Option<String>,
    permalink: Option<String>,
    thumbnail: Option<String>,
    created_utc: Option<f64>,
    preview: Option<RedditPreview>,
}

#[derive(Debug, Deserialize)]
struct RedditPreview {
    images: Option<Vec<RedditPreviewImage>>,
}

#[derive(Debug, Deserialize)]
struct RedditPreviewImage {
    source: Option<RedditPreviewSource>,
}

#[derive(Debug, Deserialize)]
struct RedditPreviewSource {
    url: Option<String>,
}

fn post_thumbnail(post: &RedditPost) -> String {
    if let Some(t) = post.thumbnail.as_deref() {
        if t.starts_with("http") {
            return clean_url(t);
        }
    }
    if let Some(prev) = &post.preview {
        if let Some(images) = &prev.images {
            for img in images {
                if let Some(src) = img.source.as_ref().and_then(|s| s.url.as_deref()) {
                    if src.starts_with("http") {
                        return clean_url(src);
                    }
                }
            }
        }
    }
    PLACEHOLDER_THUMB.to_string()
}

fn videos_from_listing(listing: &RedditListing) -> (Vec<VideoItem>, Option<String>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<VideoItem> = Vec::new();
    for child in &listing.data.children {
        let post = &child.data;
        let candidates = [
            post.url_overridden_by_dest.as_deref(),
            post.url.as_deref(),
        ];
        let mut found: Option<(String, String)> = None;
        for cand in candidates.into_iter().flatten() {
            if let Some(id) = extract_youtube_id(cand) {
                found = Some((clean_url(cand), id));
                break;
            }
        }
        let Some((yt_url, yt_id)) = found else {
            continue;
        };
        if !seen.insert(yt_id.clone()) {
            continue;
        }
        let reddit_url = post
            .permalink
            .as_deref()
            .map(|p| format!("https://www.reddit.com{p}"))
            .or_else(|| post.url.clone())
            .unwrap_or_default();
        let reddit_id = post
            .name
            .clone()
            .or_else(|| reddit_id_from_permalink(&reddit_url));
        out.push(VideoItem {
            youtube_id: yt_id,
            youtube_url: yt_url,
            title: post.title.clone().unwrap_or_else(|| "Untitled".to_string()),
            reddit_url,
            thumbnail: post_thumbnail(post),
            created_utc: post.created_utc.map(|f| f as i64),
            reddit_id,
        });
    }
    (out, listing.data.after.clone())
}

// ---------- fetch paths (reqwest fallback; kept when browser disabled/fails) ----------

async fn fetch_public_json_with_host(
    client: &reqwest::Client,
    ua: &str,
    host: &str,
    leg: &'static str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let leg_start = Instant::now();
    debug!(sub = %sub, leg = leg, "http leg start");
    let url = public_json_url(host, sub, limit, after);
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| {
            debug!(sub = %sub, leg = leg, elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "http leg transport error");
            (
                StatusCode::BAD_GATEWAY,
                format!("upstream request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        debug!(sub = %sub, leg = leg, status = 429, elapsed_ms = leg_start.elapsed().as_millis() as u64, retry_after_ms = ?ms, "http leg rate-limited");
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        debug!(sub = %sub, leg = leg, status = 403, elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg forbidden");
        return Err((
            StatusCode::BAD_GATEWAY,
            "Reddit denied the request (403)".to_string(),
            None,
        ));
    }
    if status.is_server_error() || status == StatusCode::UNAUTHORIZED {
        debug!(sub = %sub, leg = leg, status = status.as_u16(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg upstream error");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream returned {}", status.as_u16()),
            None,
        ));
    }
    if !status.is_success() {
        debug!(sub = %sub, leg = leg, status = status.as_u16(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg unexpected status");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream returned {}", status.as_u16()),
            None,
        ));
    }
    let listing: RedditListing = resp.json().await.map_err(|e| {
        debug!(sub = %sub, leg = leg, elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "http leg parse error");
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to parse Reddit response: {}", e),
            None,
        )
    })?;
    let (videos, after) = videos_from_listing(&listing);
    debug!(sub = %sub, leg = leg, status = 200, rows = videos.len(), has_after = after.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
    Ok((videos, after))
}

/// Unauth public JSON leg (www host) — the existing unauth path, unchanged.
async fn fetch_public_json(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    fetch_public_json_with_host(client, ua, "www.reddit.com", "public", sub, limit, after).await
}

/// Old.reddit public JSON leg — middle leg of the authed chain
/// (oauth -> old.reddit JSON -> RSS), gentler than the www host.
async fn fetch_old_reddit_json(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    fetch_public_json_with_host(client, ua, "old.reddit.com", "old-public", sub, limit, after).await
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OauthCreds {
    client_id: String,
    secret: String,
    username: String,
    password: String,
}

/// Pure constructor for tests (no env access): None when any field is missing.
fn oauth_creds_from(
    client_id: Option<String>,
    secret: Option<String>,
    username: Option<String>,
    password: Option<String>,
) -> Option<OauthCreds> {
    let (client_id, secret, username, password) =
        (client_id?, secret?, username?, password?);
    if client_id.is_empty() || secret.is_empty() || username.is_empty() || password.is_empty() {
        return None;
    }
    Some(OauthCreds {
        client_id,
        secret,
        username,
        password,
    })
}

fn oauth_creds() -> Option<OauthCreds> {
    oauth_creds_from(
        std::env::var("REDDIT_CLIENT_ID").ok(),
        std::env::var("REDDIT_CLIENT_SECRET")
            .or_else(|_| std::env::var("REDDIT_SECRET"))
            .ok(),
        std::env::var("REDDIT_USERNAME")
            .or_else(|_| std::env::var("REDDIT_USER"))
            .or_else(|_| std::env::var("REDDIT_USERNAME_OVERRIDE"))
            .ok(),
        // NOTE: REDDIT_USER doubles as the script username when REDDIT_USERNAME is unset.
        std::env::var("REDDIT_PASSWORD").ok(),
    )
}

/// Auth mode: `REDDIT_AUTH_MODE=script` WITH complete script creds opts into
/// OAuth; every other value (including unset — the default) is no-auth.
/// No Reddit registration is needed by default: the mirror chain
/// (ArcticShift -> PullPush -> Redlib -> RSS -> browser) serves feeds
/// without credentials. Pure helper takes the raw env value + creds presence
/// so unit tests never touch the process environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthMode {
    Script,
    NoAuth,
}

fn auth_mode_from(raw: Option<&str>, has_creds: bool) -> AuthMode {
    if matches!(raw, Some(v) if v.eq_ignore_ascii_case("script")) && has_creds {
        return AuthMode::Script;
    }
    AuthMode::NoAuth
}

fn auth_mode() -> AuthMode {
    let raw = std::env::var("REDDIT_AUTH_MODE").ok();
    auth_mode_from(raw.as_deref(), oauth_creds().is_some())
}

fn auth_mode_label(mode: AuthMode) -> &'static str {
    match mode {
        AuthMode::Script => "script",
        AuthMode::NoAuth => "no-auth",
    }
}

/// OAuth listing URL builder (pure, offline-testable).
/// `sort` is `new` or `hot`; cursor is the reddit `after` (t3_*) value.
fn oauth_listings_url(sub: &str, sort: &str, limit: u32, after: Option<&str>) -> String {
    let mut url = format!(
        "https://oauth.reddit.com/r/{}/{}.json?raw_json=1&limit={}",
        sub, sort, limit
    );
    if let Some(a) = after {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    url
}

/// Public (unauth) JSON URL builder for a given host (pure, offline-testable).
fn public_json_url(host: &str, sub: &str, limit: u32, after: Option<&str>) -> String {
    let mut url = format!(
        "https://{}/r/{}/new.json?limit={}&raw_json=1",
        host, sub, limit
    );
    if let Some(a) = after {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    url
}

/// Script-UA header value check helper: must match
/// `<platform>:<appID>:<version> by /u/<username>` (contains " by /u/").
/// Pure for offline tests.
fn is_script_ua(ua: &str) -> bool {
    ua.contains(" by /u/") && !ua.contains("(by /u/")
}

// ---------- no-auth mirrors (ArcticShift / PullPush / Redlib) ----------

/// YouTube thumbnail fallback when a mirror ships no thumbnail.
fn youtube_thumb(yt_id: &str) -> String {
    format!("https://i.ytimg.com/vi/{}/hqdefault.jpg", yt_id)
}

/// ArcticShift search URL builder (pure, offline-testable).
/// Pagination is epoch-based (`after` = oldest `created_utc` seen); t3
/// cursors are not understood by ArcticShift and are dropped.
fn arctic_search_url(sub: &str, limit: u32, after_epoch: Option<&str>) -> String {
    let mut url = format!(
        "{}/api/posts/search?subreddit={}&sort=desc&limit={}&fields=title,url,id,subreddit,created_utc,author,score,num_comments",
        ARCTIC_BASE, sub, limit
    );
    if let Some(a) = after_epoch {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    url
}

/// PullPush search URL builder (pure, offline-testable).
/// Newest-first by `created_utc`; older pages use `before` (epoch secs).
fn pullpush_search_url(sub: &str, size: u32, before_epoch: Option<&str>) -> String {
    let mut url = format!(
        "{}/reddit/search/submission/?subreddit={}&sort=desc&sort_type=created_utc&size={}",
        PULLPUSH_BASE, sub, size
    );
    if let Some(b) = before_epoch {
        if !b.is_empty() {
            url.push_str(&format!("&before={}", b));
        }
    }
    url
}

/// Redlib listing URL builder for one mirror host (pure, offline-testable).
/// Same listing schema as Reddit (thumbnail+permalink), t3 cursors apply.
fn redlib_url(host: &str, sub: &str, limit: u32, after: Option<&str>) -> String {
    let mut url = format!("https://{}/r/{}/new.json?limit={}&raw_json=1", host, sub, limit);
    if let Some(a) = after {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    url
}

/// Redlib hosts in round-robin order starting at `start` (pure).
fn redlib_order(start: usize) -> Vec<&'static str> {
    let n = REDLIB_HOSTS.len();
    (0..n).map(|i| REDLIB_HOSTS[(start + i) % n]).collect()
}

/// True when `after` looks like epoch seconds (ArcticShift/PullPush cursor).
fn is_epoch_cursor(s: &str) -> bool {
    !s.is_empty() && s.len() >= 9 && s.bytes().all(|b| b.is_ascii_digit())
}

/// Split an incoming `after` cursor: epoch cursors go to ArcticShift/PullPush
/// (`before`/`after` epoch params), t3 cursors go to Redlib/OAuth legs.
/// Pure helper so pagination mapping stays offline-testable.
fn split_after_cursor(after: Option<&str>) -> (Option<String>, Option<String>) {
    match after {
        Some(a) if !a.is_empty() && is_epoch_cursor(a) => (Some(a.to_string()), None),
        Some(a) if !a.is_empty() => (None, Some(a.to_string())),
        _ => (None, None),
    }
}

/// Oldest `created_utc` in `posts` as an epoch-seconds cursor for the next
/// ArcticShift/PullPush page. None when no timestamps are known.
fn epoch_cursor_from_posts(posts: &[VideoItem]) -> Option<String> {
    posts.iter().filter_map(|v| v.created_utc).min().map(|m| m.to_string())
}

/// Best `reddit_after` cursor to store: prefer a t3 cursor when the winning
/// leg supplied one, else fall back to the epoch of the oldest post seen.
/// Pure for offline tests.
fn best_cursor(t3: Option<String>, posts: &[VideoItem]) -> Option<String> {
    if let Some(c) = t3 {
        if !c.is_empty() {
            return Some(c);
        }
    }
    epoch_cursor_from_posts(posts)
}

/// Newest-first ordering by `created_utc` (unknown timestamps sink last).
fn sort_newest_first(items: &mut Vec<VideoItem>) {
    items.sort_by(|a, b| match (b.created_utc, a.created_utc) {
        (Some(x), Some(y)) => x.cmp(&y),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    });
}

/// Merge several mirror pages: concat, newest-first, dedupe+cap.
fn merge_mirror_pages(mut pages: Vec<Vec<VideoItem>>) -> Vec<VideoItem> {
    let mut all: Vec<VideoItem> = pages.drain(..).flatten().collect();
    sort_newest_first(&mut all);
    dedupe_cap(all)
}

#[derive(Debug, Deserialize, Default)]
#[allow(dead_code)] // subreddit/author retained for upstream schema compat + debugging.
struct ArcticPost {
    id: Option<String>,
    title: Option<String>,
    url: Option<String>,
    subreddit: Option<String>,
    created_utc: Option<f64>,
    author: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct ArcticResponse {
    #[serde(default)]
    data: Vec<ArcticPost>,
}

#[derive(Debug, Deserialize, Default)]
struct PullPushPost {
    id: Option<serde_json::Value>,
    title: Option<String>,
    url: Option<String>,
    permalink: Option<String>,
    thumbnail: Option<String>,
    created_utc: Option<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct PullPushResponse {
    #[serde(default)]
    data: Vec<PullPushPost>,
}

/// Map ArcticShift rows: `title`->title, `url`->url (YouTube only),
/// `id`->redditId with permalink synthesized as `/r/{sub}/comments/{id}/`.
/// No thumbnail from ArcticShift: fall back to YouTube hqdefault.
fn videos_from_arctic(resp: &ArcticResponse, sub: &str) -> (Vec<VideoItem>, Option<String>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<VideoItem> = Vec::new();
    for post in &resp.data {
        let url = match post.url.as_deref() {
            Some(u) if !u.is_empty() => u,
            _ => continue,
        };
        let Some(yt_id) = extract_youtube_id(url) else {
            continue;
        };
        if !seen.insert(yt_id.clone()) {
            continue;
        }
        let raw_id = post.id.as_deref().unwrap_or("").trim().to_string();
        if raw_id.is_empty() {
            continue;
        }
        let short_id = raw_id.strip_prefix("t3_").unwrap_or(&raw_id).to_string();
        let permalink = format!("/r/{}/comments/{}/", sub, short_id);
        out.push(VideoItem {
            youtube_id: yt_id.clone(),
            youtube_url: clean_url(url),
            title: post.title.clone().filter(|t| !t.is_empty()).unwrap_or_else(|| "Untitled".to_string()),
            reddit_url: format!("https://www.reddit.com{}", permalink),
            thumbnail: youtube_thumb(&yt_id),
            created_utc: post.created_utc.map(|f| f as i64),
            reddit_id: Some(format!("t3_{}", short_id)),
        });
    }
    sort_newest_first(&mut out);
    let cursor = epoch_cursor_from_posts(&out);
    (out, cursor)
}

fn pullpush_id_string(v: &Option<serde_json::Value>) -> String {
    match v {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Number(n)) => n.to_string(),
        _ => String::new(),
    }
}

/// Map PullPush rows: direct `title`/`url`/`permalink`/`thumbnail`/
/// `created_utc` fields. Missing thumbnails fall back to YouTube hqdefault;
/// relative permalinks are rooted at www.reddit.com.
fn videos_from_pullpush(resp: &PullPushResponse) -> (Vec<VideoItem>, Option<String>) {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<VideoItem> = Vec::new();
    for post in &resp.data {
        let url = match post.url.as_deref() {
            Some(u) if !u.is_empty() => u,
            _ => continue,
        };
        let Some(yt_id) = extract_youtube_id(url) else {
            continue;
        };
        if !seen.insert(yt_id.clone()) {
            continue;
        }
        let reddit_url = match post.permalink.as_deref() {
            Some(p) if p.starts_with("http") => p.to_string(),
            Some(p) if !p.is_empty() => format!("https://www.reddit.com{}", if p.starts_with('/') { p.to_string() } else { format!("/{}", p) }),
            _ => {
                let raw = pullpush_id_string(&post.id);
                if raw.is_empty() {
                    String::new()
                } else {
                    format!("https://www.reddit.com/comments/{}/", raw.trim_start_matches("t3_"))
                }
            }
        };
        let thumbnail = match post.thumbnail.as_deref() {
            Some(t) if t.starts_with("http") => clean_url(t),
            _ => youtube_thumb(&yt_id),
        };
        let reddit_id = if !reddit_url.is_empty() {
            reddit_id_from_permalink(&reddit_url).or_else(|| {
                let raw = pullpush_id_string(&post.id);
                if raw.is_empty() {
                    None
                } else {
                    Some(format!("t3_{}", raw.trim_start_matches("t3_")))
                }
            })
        } else {
            None
        };
        out.push(VideoItem {
            youtube_id: yt_id.clone(),
            youtube_url: clean_url(url),
            title: post.title.clone().filter(|t| !t.is_empty()).unwrap_or_else(|| "Untitled".to_string()),
            reddit_url,
            thumbnail,
            created_utc: post.created_utc.map(|f| f as i64),
            reddit_id,
        });
    }
    sort_newest_first(&mut out);
    let cursor = epoch_cursor_from_posts(&out);
    (out, cursor)
}

/// Per-host cooldown key helpers (pure).
fn redlib_cooldown_key(host: &str) -> String {
    format!("redlib:{}", host)
}

/// Remaining per-host cooldown, if active.
fn mirror_cooldown_remaining(mirrors: &MirrorState, key: &str, now: i64) -> Option<u64> {
    mirrors.cooldowns.get(key).and_then(|until| {
        let until = *until;
        if now < until {
            Some((until - now).max(0) as u64)
        } else {
            None
        }
    })
}

fn mirror_set_cooldown(mirrors: &MirrorState, key: &str, retry_after: Option<u64>) {
    let backoff = upstream_429_cooldown_ms(retry_after);
    mirrors.cooldowns.insert(key.to_string(), now_ms() + backoff);
}

/// Parse `X-RateLimit-Reset` (seconds, possibly fractional) into ms.
/// Pure helper; Retry-After is handled separately by `retry_after_ms`.
fn ratelimit_reset_ms(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<f64>().ok())
        .map(|secs| (secs.max(0.0) * 1000.0) as u64)
        .filter(|ms| *ms > 0)
}

/// Global per-mirror politeness throttle.
async fn throttle_last(last: &Mutex<Option<Instant>>, min: Duration) {
    let wait: Option<Duration> = {
        let guard = last.lock().await;
        match *guard {
            Some(t) => {
                let elapsed = t.elapsed();
                if elapsed < min {
                    Some(min - elapsed)
                } else {
                    None
                }
            }
            None => None,
        }
    };
    if let Some(d) = wait {
        tokio::time::sleep(d).await;
    }
    *last.lock().await = Some(Instant::now());
}

async fn fetch_arctic(
    state: &AppState,
    sub: &str,
    limit: u32,
    after_epoch: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    if let Some(remain) = mirror_cooldown_remaining(&state.mirrors, "arctic", now_ms()) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "ArcticShift mirror cooling down".to_string(),
            Some(remain),
        ));
    }
    throttle_last(&state.mirrors.arctic_last, ARCTIC_MIN_INTERVAL).await;
    let leg_start = Instant::now();
    debug!(sub = %sub, leg = "arctic", "http leg start");
    let url = arctic_search_url(sub, limit, after_epoch);
    let resp = state
        .client
        .get(&url)
        .header(header::USER_AGENT, reddit_ua())
        .header(header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| {
            debug!(sub = %sub, leg = "arctic", elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "http leg transport error");
            (
                StatusCode::BAD_GATEWAY,
                format!("ArcticShift request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    let headers = resp.headers().clone();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(&headers).or_else(|| ratelimit_reset_ms(&headers));
        mirror_set_cooldown(&state.mirrors, "arctic", ms);
        debug!(sub = %sub, leg = "arctic", status = 429, retry_after_ms = ?ms, "http leg rate-limited (per-host cooldown, others unaffected)");
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "ArcticShift rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        debug!(sub = %sub, leg = "arctic", status = 403, "http leg forbidden");
        return Err((
            StatusCode::BAD_GATEWAY,
            "ArcticShift denied the request (403)".to_string(),
            None,
        ));
    }
    if !status.is_success() {
        debug!(sub = %sub, leg = "arctic", status = status.as_u16(), "http leg unexpected status");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("ArcticShift upstream returned {}", status.as_u16()),
            None,
        ));
    }
    let parsed: ArcticResponse = resp.json().await.map_err(|e| {
        debug!(sub = %sub, leg = "arctic", error = %e, "http leg parse error");
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to parse ArcticShift response: {}", e),
            None,
        )
    })?;
    let (videos, cursor) = videos_from_arctic(&parsed, sub);
    debug!(sub = %sub, leg = "arctic", rows = videos.len(), has_after = cursor.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
    Ok((videos, cursor))
}

async fn fetch_pullpush(
    state: &AppState,
    sub: &str,
    size: u32,
    before_epoch: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    if let Some(remain) = mirror_cooldown_remaining(&state.mirrors, "pullpush", now_ms()) {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "PullPush mirror cooling down".to_string(),
            Some(remain),
        ));
    }
    throttle_last(&state.mirrors.pullpush_last, PULLPUSH_MIN_INTERVAL).await;
    let leg_start = Instant::now();
    debug!(sub = %sub, leg = "pullpush", "http leg start");
    let url = pullpush_search_url(sub, size, before_epoch);
    let resp = state
        .client
        .get(&url)
        .header(header::USER_AGENT, reddit_ua())
        .header(header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| {
            debug!(sub = %sub, leg = "pullpush", elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "http leg transport error");
            (
                StatusCode::BAD_GATEWAY,
                format!("PullPush request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    let headers = resp.headers().clone();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(&headers).or_else(|| ratelimit_reset_ms(&headers));
        // Exponential backoff flavor: per-host negative cache (never sole
        // source, so the chain continues to Redlib/RSS regardless).
        mirror_set_cooldown(&state.mirrors, "pullpush", ms);
        debug!(sub = %sub, leg = "pullpush", status = 429, retry_after_ms = ?ms, "http leg rate-limited (per-host cooldown, others unaffected)");
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "PullPush rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        debug!(sub = %sub, leg = "pullpush", status = 403, "http leg forbidden");
        return Err((
            StatusCode::BAD_GATEWAY,
            "PullPush denied the request (403)".to_string(),
            None,
        ));
    }
    if !status.is_success() {
        debug!(sub = %sub, leg = "pullpush", status = status.as_u16(), "http leg unexpected status");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("PullPush upstream returned {}", status.as_u16()),
            None,
        ));
    }
    let parsed: PullPushResponse = resp.json().await.map_err(|e| {
        debug!(sub = %sub, leg = "pullpush", error = %e, "http leg parse error");
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to parse PullPush response: {}", e),
            None,
        )
    })?;
    let (videos, cursor) = videos_from_pullpush(&parsed);
    debug!(sub = %sub, leg = "pullpush", rows = videos.len(), has_after = cursor.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
    Ok((videos, cursor))
}

/// Redlib fetch across the round-robin hosts. Tries each host in rotation
/// order; 429/403 on one host sets a per-host cooldown and rotates to the
/// next host instead of failing the chain. Returns the first success, or a
/// 429 only when every tried host was rate-limited.
async fn fetch_redlib(
    state: &AppState,
    sub: &str,
    limit: u32,
    after_t3: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let start = *state.mirrors.redlib_idx.lock().await;
    let order = redlib_order(start);
    let mut saw_429: Option<u64> = None;
    let mut last_err: Option<(StatusCode, String, Option<u64>)> = None;
    let mut attempted = 0usize;
    for host in order {
        let key = redlib_cooldown_key(host);
        if let Some(remain) = mirror_cooldown_remaining(&state.mirrors, &key, now_ms()) {
            saw_429 = Some(saw_429.map(|m: u64| m.min(remain)).unwrap_or(remain));
            continue;
        }
        throttle_last(&state.mirrors.redlib_last, REDLIB_MIN_INTERVAL).await;
        let leg_start = Instant::now();
        let url = redlib_url(host, sub, limit, after_t3);
        debug!(sub = %sub, leg = "redlib", host = %host, "http leg start");
        let resp = match state
            .client
            .get(&url)
            .header(header::USER_AGENT, reddit_ua())
            .header(header::ACCEPT, "application/json")
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                debug!(sub = %sub, leg = "redlib", host = %host, error = %e, "http leg transport error");
                last_err = Some((
                    StatusCode::BAD_GATEWAY,
                    format!("Redlib {} request failed: {}", host, e),
                    None,
                ));
                continue;
            }
        };
        let status = resp.status();
        let headers = resp.headers().clone();
        if status == StatusCode::TOO_MANY_REQUESTS {
            let ms = retry_after_ms(&headers).or_else(|| ratelimit_reset_ms(&headers));
            mirror_set_cooldown(&state.mirrors, &key, ms);
            saw_429 = Some(saw_429.map(|m: u64| m.min(ms.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))).unwrap_or(ms.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64)));
            debug!(sub = %sub, leg = "redlib", host = %host, status = 429, retry_after_ms = ?ms, "http leg rate-limited, rotating");
            // Advance the round-robin start so the next call starts elsewhere.
            *state.mirrors.redlib_idx.lock().await = (start + attempted + 1) % REDLIB_HOSTS.len();
            attempted += 1;
            last_err = Some((
                StatusCode::TOO_MANY_REQUESTS,
                format!("Redlib {} rate-limited the request", host),
                ms,
            ));
            continue;
        }
        if status == StatusCode::FORBIDDEN {
            mirror_set_cooldown(&state.mirrors, &key, None);
            debug!(sub = %sub, leg = "redlib", host = %host, status = 403, "http leg forbidden, rotating");
            *state.mirrors.redlib_idx.lock().await = (start + attempted + 1) % REDLIB_HOSTS.len();
            attempted += 1;
            last_err = Some((
                StatusCode::BAD_GATEWAY,
                format!("Redlib {} denied the request (403)", host),
                None,
            ));
            continue;
        }
        if !status.is_success() {
            debug!(sub = %sub, leg = "redlib", host = %host, status = status.as_u16(), "http leg unexpected status");
            last_err = Some((
                StatusCode::BAD_GATEWAY,
                format!("Redlib {} upstream returned {}", host, status.as_u16()),
                None,
            ));
            continue;
        }
        let listing: RedditListing = match resp.json().await {
            Ok(l) => l,
            Err(e) => {
                debug!(sub = %sub, leg = "redlib", host = %host, error = %e, "http leg parse error");
                last_err = Some((
                    StatusCode::BAD_GATEWAY,
                    format!("failed to parse Redlib {} response: {}", host, e),
                    None,
                ));
                continue;
            }
        };
        let (mut videos, after) = videos_from_listing(&listing);
        sort_newest_first(&mut videos);
        debug!(sub = %sub, leg = "redlib", host = %host, rows = videos.len(), has_after = after.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
        return Ok((dedupe_cap(videos), after));
    }
    if let Some(err) = last_err {
        if err.0 == StatusCode::TOO_MANY_REQUESTS || saw_429.is_some() {
            // Every host cooled down or rate-limited: surface 429 so the
            // sub-level negative cache still applies.
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Redlib mirrors rate-limited the request".to_string(),
                saw_429.or(err.2),
            ));
        }
        return Err(err);
    }
    Err((
        StatusCode::BAD_GATEWAY,
        "Redlib mirrors unreachable".to_string(),
        None,
    ))
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)] // token_type is required for deserialization compat; only access_token/expires_in are used.
struct TokenResponse {
    access_token: Option<String>,
    token_type: Option<String>,
    expires_in: Option<u64>,
    error: Option<serde_json::Value>,
}

/// Enforce the global ~1 req/s OAuth throttle before hitting
/// oauth.reddit.com. Also honors a previously observed
/// X-Ratelimit-Remaining==0 + Reset window by sleeping until reset.
async fn oauth_throttle(oauth: &OAuthState) {
    // Honor an exhausted ratelimit window first.
    let wait_reset: Option<Duration> = {
        let rem = *oauth.ratelimit_remaining.lock().await;
        let reset = *oauth.ratelimit_reset_at.lock().await;
        match (rem, reset) {
            (Some(r), Some(at)) if r < 1.0 => {
                let now = Instant::now();
                if at > now {
                    Some(at - now)
                } else {
                    None
                }
            }
            _ => None,
        }
    };
    if let Some(d) = wait_reset {
        debug!(wait_ms = d.as_millis() as u64, "oauth throttle: ratelimit exhausted, waiting for reset window");
        tokio::time::sleep(d).await;
    }
    // Steady-state 1 req/s average.
    let wait_gap: Option<Duration> = {
        let last = *oauth.last_req.lock().await;
        match last {
            Some(t) => {
                let elapsed = t.elapsed();
                if elapsed < OAUTH_MIN_INTERVAL {
                    Some(OAUTH_MIN_INTERVAL - elapsed)
                } else {
                    None
                }
            }
            None => None,
        }
    };
    if let Some(d) = wait_gap {
        tokio::time::sleep(d).await;
    }
    *oauth.last_req.lock().await = Some(Instant::now());
}

/// Record X-Ratelimit-Remaining / X-Ratelimit-Reset from an oauth response
/// (honest 60/min use). Missing/unparseable headers leave prior state alone.
async fn oauth_record_ratelimit(oauth: &OAuthState, headers: &HeaderMap) {
    let remaining: Option<f64> = headers
        .get("x-ratelimit-remaining")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok());
    let reset_secs: Option<f64> = headers
        .get("x-ratelimit-reset")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse().ok());
    if let Some(r) = remaining {
        *oauth.ratelimit_remaining.lock().await = Some(r);
    }
    if let Some(s) = reset_secs {
        let reset_at = Instant::now() + Duration::from_secs_f64(s.max(0.0));
        *oauth.ratelimit_reset_at.lock().await = Some(reset_at);
    }
    if remaining.is_some() || reset_secs.is_some() {
        debug!(remaining = ?remaining, reset_secs = ?reset_secs, "oauth ratelimit headers observed");
    }
}

/// Raw script password-grant POST. Returns the bearer token + expires_in.
/// Never logs secrets; callers log only status codes on failure.
async fn request_oauth_token(
    client: &reqwest::Client,
    ua: &str,
    creds: &OauthCreds,
) -> Option<(String, u64)> {
    let leg_start = Instant::now();
    let resp = client
        .post(OAUTH_TOKEN_URL)
        .header(header::USER_AGENT, ua)
        .basic_auth(&creds.client_id, Some(&creds.secret))
        .form(&[
            ("grant_type", "password"),
            ("username", creds.username.as_str()),
            ("password", creds.password.as_str()),
            ("duration", "permanent"),
        ])
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        debug!(status = resp.status().as_u16(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "oauth token rejected (status)");
        return None;
    }
    let token: TokenResponse = resp.json().await.ok()?;
    let access = token.access_token.filter(|t| !t.is_empty())?;
    if token.error.is_some() {
        debug!(elapsed_ms = leg_start.elapsed().as_millis() as u64, "oauth token body carried error field");
        // Reddit sometimes includes a benign error field alongside a token;
        // only fail when there is no usable access token (handled above).
    }
    let expires_in = token.expires_in.unwrap_or(3600);
    Some((access, expires_in))
}

/// Cached bearer token with single-flight refresh. Returns None when creds
/// are absent/disabled or the token POST fails (caller must fall back to
/// unauth legs, never fail hard). Logs warn (no secrets) on refresh failure.
async fn oauth_bearer_token(
    oauth: &OAuthState,
    client: &reqwest::Client,
    ua: &str,
) -> Option<String> {
    // Fast path: valid cached token, no lock contention beyond the guard.
    {
        let guard = oauth.cached.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.valid() {
                return Some(cached.token.clone());
            }
        }
    }
    // Single-flight: only one task performs the token POST at a time.
    let _refresh_guard = oauth.refresh.lock().await;
    // Re-check after acquiring the refresh lock (another task may have filled it).
    {
        let guard = oauth.cached.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.valid() {
                return Some(cached.token.clone());
            }
        }
    }
    let creds = oauth_creds()?;
    if auth_mode() == AuthMode::NoAuth {
        return None;
    }
    match request_oauth_token(client, ua, &creds).await {
        Some((token, expires_in)) => {
            let expires_at = token_expiry_for(expires_in);
            *oauth.cached.lock().await = Some(CachedToken {
                token: token.clone(),
                expires_at,
            });
            debug!(expires_in = expires_in, ttl_secs = token_ttl_secs(expires_in), "oauth token refreshed (cached)");
            Some(token)
        }
        None => {
            warn!("oauth token refresh failed; falling back to mirror legs (arctic + pullpush + redlib + RSS)");
            None
        }
    }
}

async fn fetch_oauth_json(
    oauth: &OAuthState,
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Option<Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)>> {
    if auth_mode() == AuthMode::NoAuth {
        return None;
    }
    if oauth_creds().is_none() {
        return None;
    }
    let leg_start = Instant::now();
    debug!(sub = %sub, leg = "oauth", "http leg start");
    let access = oauth_bearer_token(oauth, client, ua).await?;
    oauth_throttle(oauth).await;
    let url = oauth_listings_url(sub, "new", limit, after);
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/json")
        .bearer_auth(access)
        .send()
        .await
        .ok()?;
    oauth_record_ratelimit(oauth, resp.headers()).await;
    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        debug!(sub = %sub, leg = "oauth", status = 429, elapsed_ms = leg_start.elapsed().as_millis() as u64, retry_after_ms = ?ms, "http leg rate-limited");
        return Some(Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit (oauth) rate-limited the request".to_string(),
            ms,
        )));
    }
    if resp.status() == StatusCode::UNAUTHORIZED {
        // Bearer rejected (revoked/expired race): drop the cache so the next
        // call refreshes, then fall through to unauth legs this round.
        *oauth.cached.lock().await = None;
        debug!(sub = %sub, leg = "oauth", status = 401, elapsed_ms = leg_start.elapsed().as_millis() as u64, "oauth bearer rejected; cache cleared, falling through to unauth");
        return None;
    }
    if !resp.status().is_success() {
        debug!(sub = %sub, leg = "oauth", status = resp.status().as_u16(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "oauth fetch non-success, falling through to unauth");
        return None; // let caller fall through to public JSON / RSS
    }
    let listing: RedditListing = resp.json().await.ok()?;
    let (videos, after) = videos_from_listing(&listing);
    debug!(sub = %sub, leg = "oauth", status = 200, rows = videos.len(), has_after = after.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
    Some(Ok((videos, after)))
}

async fn fetch_rss_fallback(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
) -> Result<Vec<VideoItem>, (StatusCode, String, Option<u64>)> {
    let leg_start = Instant::now();
    debug!(sub = %sub, leg = "rss", "http leg start");
    let url = format!("https://www.reddit.com/r/{}/new.rss", sub);
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/rss+xml, application/xml, text/xml")
        .send()
        .await
        .map_err(|e| {
            debug!(sub = %sub, leg = "rss", elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "http leg transport error");
            (
                StatusCode::BAD_GATEWAY,
                format!("RSS fallback request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        debug!(sub = %sub, leg = "rss", status = 429, elapsed_ms = leg_start.elapsed().as_millis() as u64, retry_after_ms = ?ms, "http leg rate-limited");
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        debug!(sub = %sub, leg = "rss", status = 403, elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg forbidden");
        return Err((
            StatusCode::BAD_GATEWAY,
            "Reddit denied the request (403)".to_string(),
            None,
        ));
    }
    if !status.is_success() {
        debug!(sub = %sub, leg = "rss", status = status.as_u16(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg unexpected status");
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit RSS upstream returned {}", status.as_u16()),
            None,
        ));
    }
    let bytes = resp.bytes().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to read RSS body: {}", e),
            None,
        )
    })?;
    let feed = feed_rs::parser::parse(&bytes[..]).map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to parse RSS feed: {}", e),
            None,
        )
    })?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<VideoItem> = Vec::new();
    for entry in feed.entries {
        let title = entry
            .title
            .map(|t| t.content.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "Untitled".to_string());
        let reddit_url = entry
            .links
            .first()
            .map(|l| l.href.clone())
            .unwrap_or_default();
        let html = entry
            .content
            .and_then(|c| c.body)
            .or_else(|| entry.summary.map(|s| s.content))
            .unwrap_or_default();
        let Some((yt_url, yt_id)) = extract_youtube_from_text(&html) else {
            continue;
        };
        if !seen.insert(yt_id.clone()) {
            continue;
        }
        let mut thumbnail = PLACEHOLDER_THUMB.to_string();
        for media in &entry.media {
            for thumb in &media.thumbnails {
                let uri = thumb.image.uri.clone();
                if uri.starts_with("http") {
                    thumbnail = uri;
                    break;
                }
            }
            if thumbnail != PLACEHOLDER_THUMB {
                break;
            }
        }
        let created_utc = entry
            .published
            .or(entry.updated)
            .map(|dt| dt.timestamp());
        let reddit_id = reddit_id_from_permalink(&reddit_url);
        out.push(VideoItem {
            youtube_id: yt_id,
            youtube_url: clean_url(&yt_url),
            title,
            reddit_url,
            thumbnail,
            created_utc,
            reddit_id,
        });
    }
    debug!(sub = %sub, leg = "rss", status = 200, rows = out.len(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "http leg end");
    Ok(out)
}

/// Reqwest fallback chain. Returns timeline posts (newest-first) plus the
/// reddit `after` cursor when known.
///
/// Default (no-auth, no registration needed) order:
/// ArcticShift (primary, epoch pagination) -> PullPush (secondary, 4s gap,
/// never sole source) -> Redlib round-robin (t3 cursors, per-host rotate) ->
/// RSS (head fill only). A 429 on one mirror sets a per-host cooldown and the
/// chain continues to the next mirror — it never blocks the others; the
/// sub-level 429 negative cache applies only when every leg reports 429.
///
/// Opt-in OAuth (`REDDIT_AUTH_MODE=script` with complete script creds) tries
/// `oauth.reddit.com` first, then the same mirror chain, then the legacy
/// old.reddit/`.json` legs before RSS. Direct `www.reddit.com/.json` unauth
/// is NOT part of the default path. A failed token fetch never fails hard.
async fn fetch_http_fallback(
    state: &AppState,
    sub: &str,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let client = &state.client;
    let ua = reddit_ua();
    // Timeline fill always grabs the newest page; pagination is served
    // from memory via the index cursor.
    const FILL_LIMIT: u32 = 100;
    let chain_start = Instant::now();
    let authed = auth_mode() == AuthMode::Script && oauth_creds().is_some();
    let (epoch_after, t3_after) = split_after_cursor(after);
    debug!(sub = %sub, chain = "fallback", authed = authed, has_after = after.is_some(), "http chain start");
    let mut saw_429: Option<u64> = None;
    let mut last_err: Option<(StatusCode, String, Option<u64>)> = None;
    let mut empty_ok: Option<(Vec<VideoItem>, Option<String>)> = None;
    let note_429 = |saw: &mut Option<u64>, r: Option<u64>| {
        if let Some(ms) = r {
            *saw = Some(saw.map(|m| m.min(ms)).unwrap_or(ms));
        } else {
            *saw = Some(saw.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64));
        }
    };
    if authed {
        // OAuth-first (cached token; token failure falls through silently).
        // A 429 here no longer short-circuits: mirrors still get a chance.
        if let Some(oauth_result) =
            fetch_oauth_json(&state.oauth, client, &ua, sub, FILL_LIMIT, t3_after.as_deref().or(after)).await
        {
            match oauth_result {
                Ok((videos, after)) => {
                    if !videos.is_empty() {
                        debug!(sub = %sub, chain = "fallback", winner = "oauth", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                        let mut v = videos;
                        sort_newest_first(&mut v);
                        return Ok((dedupe_cap(v), after));
                    }
                    empty_ok = empty_ok.or(Some((videos, after)));
                }
                Err((c2, m2, r2)) => {
                    if c2 == StatusCode::TOO_MANY_REQUESTS {
                        note_429(&mut saw_429, r2);
                        debug!(sub = %sub, chain = "fallback", leg = "oauth", error = %m2, "oauth leg rate-limited, continuing to mirrors");
                    } else {
                        debug!(sub = %sub, chain = "fallback", leg = "oauth", error = %m2, "oauth leg missed, trying mirrors");
                    }
                }
            }
        }
    }
    // 1) ArcticShift primary.
    match fetch_arctic(state, sub, FILL_LIMIT, epoch_after.as_deref()).await {
        Ok((videos, cursor)) => {
            if !videos.is_empty() {
                debug!(sub = %sub, chain = "fallback", winner = "arctic", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                return Ok((dedupe_cap(videos), cursor));
            }
            empty_ok = empty_ok.or(Some((videos, cursor)));
        }
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                note_429(&mut saw_429, retry_ms);
            } else {
                last_err = Some((code, msg, retry_ms));
            }
            if code == StatusCode::TOO_MANY_REQUESTS {
                debug!(sub = %sub, chain = "fallback", leg = "arctic", "mirror leg rate-limited, continuing");
            }
        }
    }
    // 2) PullPush secondary (never sole source: failure just continues).
    match fetch_pullpush(state, sub, FILL_LIMIT, epoch_after.as_deref()).await {
        Ok((videos, cursor)) => {
            if !videos.is_empty() {
                debug!(sub = %sub, chain = "fallback", winner = "pullpush", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                return Ok((dedupe_cap(videos), cursor));
            }
            empty_ok = empty_ok.or(Some((videos, cursor)));
        }
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                note_429(&mut saw_429, retry_ms);
            } else if last_err.is_none() {
                last_err = Some((code, msg, retry_ms));
            }
        }
    }
    // 3) Redlib round-robin (t3 cursors).
    match fetch_redlib(state, sub, FILL_LIMIT, t3_after.as_deref()).await {
        Ok((videos, after)) => {
            if !videos.is_empty() {
                debug!(sub = %sub, chain = "fallback", winner = "redlib", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                return Ok((dedupe_cap(videos), after));
            }
            // Empty redlib page still carries a cursor; prefer it over
            // epoch cursors when no rows exist anywhere.
            if empty_ok.is_none() {
                empty_ok = Some((videos, after));
            }
        }
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                note_429(&mut saw_429, retry_ms);
            } else if last_err.is_none() {
                last_err = Some((code, msg, retry_ms));
            }
        }
    }
    if authed {
        // Opt-in legacy legs (script mode only): old.reddit JSON, then www
        // JSON. Never part of the default no-auth path.
        match fetch_old_reddit_json(client, &ua, sub, FILL_LIMIT, t3_after.as_deref().or(after)).await {
            Ok((videos, after)) => {
                if !videos.is_empty() {
                    debug!(sub = %sub, chain = "fallback", winner = "old-public", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                    return Ok((dedupe_cap(videos), after));
                }
                if empty_ok.is_none() {
                    empty_ok = Some((videos, after));
                }
            }
            Err((code, msg, retry_ms)) => {
                if code == StatusCode::TOO_MANY_REQUESTS {
                    note_429(&mut saw_429, retry_ms);
                } else if last_err.is_none() {
                    last_err = Some((code, msg, retry_ms));
                }
            }
        }
        match fetch_public_json(client, &ua, sub, FILL_LIMIT, t3_after.as_deref().or(after)).await {
            Ok((videos, after)) => {
                if !videos.is_empty() {
                    debug!(sub = %sub, chain = "fallback", winner = "public(opt-in)", rows = videos.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
                    return Ok((dedupe_cap(videos), after));
                }
                if empty_ok.is_none() {
                    empty_ok = Some((videos, after));
                }
            }
            Err((code, msg, retry_ms)) => {
                if code == StatusCode::TOO_MANY_REQUESTS {
                    note_429(&mut saw_429, retry_ms);
                } else if last_err.is_none() {
                    last_err = Some((code, msg, retry_ms));
                }
            }
        }
    }
    // An empty-but-successful mirror page still fills the timeline (cursor
    // preserved); prefer a t3 cursor when one exists, else epoch.
    if let Some((videos, cursor)) = empty_ok {
        if videos.is_empty() {
            // Try RSS before settling for an empty page (head fill only).
            if after.is_none() {
                let prior = last_err.as_ref().map(|(_, m, _)| m.clone()).unwrap_or_default();
                match rss_tail(client, &ua, sub, &prior, chain_start).await {
                    Ok((rvideos, rcursor)) => {
                        if !rvideos.is_empty() {
                            return Ok((rvideos, rcursor));
                        }
                    }
                    Err((c3, m3, r3)) => {
                        if c3 == StatusCode::TOO_MANY_REQUESTS {
                            note_429(&mut saw_429, r3);
                        } else if last_err.is_none() {
                            last_err = Some((c3, m3, r3));
                        }
                    }
                }
            }
            // No rows anywhere: surface 429 only when every leg was
            // rate-limited; otherwise return the (empty) page or the error.
            if saw_429.is_some() && last_err.is_none() {
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    "Mirrors rate-limited the request".to_string(),
                    saw_429,
                ));
            }
            if saw_429.is_none() {
                return Ok((videos, cursor));
            }
        }
    }
    // RSS has no `after` cursor; only usable for the head fill.
    if after.is_some() {
        if let Some((code, msg, retry)) = last_err {
            return Err((
                code,
                format!("Mirror upstream failed for r/{}: {}", sub, msg),
                retry,
            ));
        }
        if let Some(ms) = saw_429 {
            return Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Mirrors rate-limited the request".to_string(),
                Some(ms),
            ));
        }
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Mirror upstream failed for r/{}", sub),
            None,
        ));
    }
    let prior = last_err.as_ref().map(|(_, m, _)| m.clone()).unwrap_or_default();
    match rss_tail(client, &ua, sub, &prior, chain_start).await {
        Ok(ok) => Ok(ok),
        Err((c3, m3, r3)) => {
            if c3 == StatusCode::TOO_MANY_REQUESTS || saw_429.is_some() {
                return Err((
                    StatusCode::TOO_MANY_REQUESTS,
                    "Mirrors rate-limited the request".to_string(),
                    r3.or(saw_429),
                ));
            }
            if prior.contains("403") || m3.contains("403") {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!("Mirror upstream denied the request. {}", m3),
                    r3,
                ));
            }
            Err((
                StatusCode::BAD_GATEWAY,
                format!("Mirror upstream failed ({}; fallback: {})", prior, m3),
                r3,
            ))
        }
    }
}

/// Shared RSS tail of the fallback chain (head fill only; pagination has no
/// RSS cursor). Extracted so the authed and unauth paths share one body.
async fn rss_tail(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    prior_msg: &str,
    chain_start: Instant,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    match fetch_rss_fallback(client, ua, sub).await {
        Ok(all) => {
            debug!(sub = %sub, chain = "fallback", winner = "rss", rows = all.len(), elapsed_ms = chain_start.elapsed().as_millis() as u64, "http chain end");
            Ok((dedupe_cap(all), None))
        }
        Err((c3, m3, r3)) => {
            if c3 == StatusCode::TOO_MANY_REQUESTS {
                return Err((c3, "Reddit rate-limited the request".to_string(), r3));
            }
            if prior_msg.contains("403") || m3.contains("403") {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!("Reddit upstream denied the request. {}", m3),
                    r3,
                ));
            }
            Err((
                StatusCode::BAD_GATEWAY,
                format!("Reddit upstream failed ({}; fallback: {})", prior_msg, m3),
                r3,
            ))
        }
    }
}

/// Fast first-paint HTTP fill for never-fetched subs: ArcticShift, PullPush,
/// Redlib (first host) and RSS run concurrently, each capped at
/// FAST_HTTP_LEG_TIMEOUT (~4s). OAuth is deliberately skipped here — the
/// token POST would block first paint on a second sequential timeout chain;
/// it stays deferred to background enrichment (fetch_http_fallback) after
/// first paint. Returns merged, deduped rows newest-first by `created_utc`
/// with the best cursor (t3 from Redlib when present, else oldest epoch).
/// A 429 from every leg surfaces as 429 when no rows are available; timeouts
/// are treated as a missed leg, not fatal.
async fn fetch_http_first_paint(
    state: &AppState,
    sub: &str,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    const FILL_LIMIT: u32 = 100;
    let fp_start = Instant::now();
    debug!(sub = %sub, chain = "first_paint", "http first-paint start (arctic+pullpush+redlib+rss parallel)");
    let ua = reddit_ua();
    let (arctic_res, pullpush_res, redlib_res, rss_res) = tokio::join!(
        tokio::time::timeout(
            FAST_HTTP_LEG_TIMEOUT,
            fetch_arctic(state, sub, FILL_LIMIT, None)
        ),
        tokio::time::timeout(
            FAST_HTTP_LEG_TIMEOUT,
            fetch_pullpush(state, sub, FILL_LIMIT, None)
        ),
        tokio::time::timeout(
            FAST_HTTP_LEG_TIMEOUT,
            fetch_redlib(state, sub, FILL_LIMIT, None)
        ),
        tokio::time::timeout(FAST_HTTP_LEG_TIMEOUT, fetch_rss_fallback(&state.client, &ua, sub)),
    );
    // Normalize: timeout -> None (leg missed its budget).
    enum Leg<T> {
        Hit(T),
        Miss429(Option<u64>),
        Miss(String),
        Timeout,
    }
    impl<T> Leg<T> {
        fn label(&self) -> &'static str {
            match self {
                Leg::Hit(_) => "hit",
                Leg::Miss429(_) => "miss-429",
                Leg::Miss(_) => "miss",
                Leg::Timeout => "timeout",
            }
        }
    }
    fn normalize<T>(res: Result<Result<T, (StatusCode, String, Option<u64>)>, tokio::time::error::Elapsed>) -> Leg<T> {
        match res {
            Err(_) => Leg::Timeout,
            Ok(Ok(ok)) => Leg::Hit(ok),
            Ok(Err((code, msg, retry))) => {
                if code == StatusCode::TOO_MANY_REQUESTS {
                    Leg::Miss429(retry)
                } else {
                    Leg::Miss(msg)
                }
            }
        }
    }
    let arctic_leg: Leg<(Vec<VideoItem>, Option<String>)> = normalize(arctic_res);
    let pullpush_leg: Leg<(Vec<VideoItem>, Option<String>)> = normalize(pullpush_res);
    let redlib_leg: Leg<(Vec<VideoItem>, Option<String>)> = normalize(redlib_res);
    let rss_leg: Leg<Vec<VideoItem>> = normalize(rss_res);
    // Collect hits; per-host 429s never block the other legs.
    let mut pages: Vec<Vec<VideoItem>> = Vec::new();
    let mut t3_cursor: Option<String> = None;
    let mut epoch_fallback: Option<String> = None;
    let mut misses: Vec<String> = Vec::new();
    let mut saw_429: Option<u64> = None;
    let mut timeouts = 0u32;
    for (label, leg) in [
        ("arctic", arctic_leg.label()),
        ("pullpush", pullpush_leg.label()),
        ("redlib", redlib_leg.label()),
        ("rss", rss_leg.label()),
    ] {
        debug!(sub = %sub, chain = "first_paint", leg = label, outcome = leg, "first-paint leg settled");
    }
    match arctic_leg {
        Leg::Hit((v, c)) => {
            if c.is_some() && epoch_fallback.is_none() {
                epoch_fallback = c.clone();
            }
            if !v.is_empty() {
                pages.push(v);
            }
        }
        Leg::Miss429(r) => saw_429 = Some(saw_429.map(|m: u64| m.min(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))).unwrap_or(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))),
        Leg::Miss(m) => misses.push(format!("arctic: {}", m)),
        Leg::Timeout => timeouts += 1,
    }
    match pullpush_leg {
        // Never sole source: only merged with other legs, never returned
        // alone when everything else missed (falls through to error below
        // unless another leg also hit).
        Leg::Hit((v, c)) => {
            if c.is_some() && epoch_fallback.is_none() {
                epoch_fallback = c.clone();
            }
            if !v.is_empty() {
                pages.push(v);
            }
        }
        Leg::Miss429(r) => saw_429 = Some(saw_429.map(|m: u64| m.min(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))).unwrap_or(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))),
        Leg::Miss(m) => misses.push(format!("pullpush: {}", m)),
        Leg::Timeout => timeouts += 1,
    }
    match redlib_leg {
        Leg::Hit((v, c)) => {
            if c.is_some() {
                t3_cursor = c.clone();
            }
            if !v.is_empty() {
                pages.push(v);
            }
        }
        Leg::Miss429(r) => saw_429 = Some(saw_429.map(|m: u64| m.min(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))).unwrap_or(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))),
        Leg::Miss(m) => misses.push(format!("redlib: {}", m)),
        Leg::Timeout => timeouts += 1,
    }
    match rss_leg {
        Leg::Hit(v) => {
            if !v.is_empty() {
                pages.push(v);
            }
        }
        Leg::Miss429(r) => saw_429 = Some(saw_429.map(|m: u64| m.min(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))).unwrap_or(r.unwrap_or(UPSTREAM_429_COOLDOWN_MS as u64))),
        Leg::Miss(m) => misses.push(format!("rss: {}", m)),
        Leg::Timeout => timeouts += 1,
    }
    if !pages.is_empty() {
        let merged = merge_mirror_pages(pages);
        let cursor = best_cursor(t3_cursor, &merged).or(epoch_fallback);
        debug!(sub = %sub, chain = "first_paint", rows = merged.len(), has_after = cursor.is_some(), elapsed_ms = fp_start.elapsed().as_millis() as u64, "http first-paint end (mirrors merged)");
        return Ok((merged, cursor));
    }
    // No rows: 429 only when at least one leg was rate-limited and none hit.
    if saw_429.is_some() && misses.is_empty() {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Mirrors rate-limited the request".to_string(),
            saw_429,
        ));
    }
    if saw_429.is_some() && misses.len() + timeouts as usize >= 3 {
        // Mixed 429 + failures with no rows: still surface 429 when at
        // least one mirror explicitly asked for backoff.
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Mirrors rate-limited the request".to_string(),
            saw_429,
        ));
    }
    if !misses.is_empty() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Mirror upstream failed ({})", misses.join("; ")),
            None,
        ));
    }
    Err((
        StatusCode::BAD_GATEWAY,
        "Mirror upstream timed out (fast first paint)".to_string(),
        None,
    ))
}

// ---------- stealth headless browser (chromiumoxide) ----------

/// Single long-lived browser, lazily started on the first cache miss.
static BROWSER_CELL: OnceLock<Arc<Mutex<Option<Browser>>>> = OnceLock::new();

fn browser_cell() -> Arc<Mutex<Option<Browser>>> {
    BROWSER_CELL
        .get_or_init(|| Arc::new(Mutex::new(None)))
        .clone()
}

async fn launch_browser() -> Result<Browser, String> {
    let launch_start = Instant::now();
    debug!("browser launch start");
    let config = BrowserConfig::builder()
        .user_data_dir("./data/chrome-profile")
        .window_size(1920, 1080)
        .new_headless_mode()
        .arg("--disable-blink-features=AutomationControlled")
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("--disable-dev-shm-usage")
        .arg("--window-size=1920,1080")
        // NOTE: request interception is intentionally NOT enabled: without a
        // handler continuing every paused request, navigation would stall.
        .build()
        .map_err(|e| format!("browser config failed: {}", e))?;
    let (browser, mut handler) = Browser::launch(config)
        .await
        .map_err(|e| {
            debug!(elapsed_ms = launch_start.elapsed().as_millis() as u64, error = %e, "browser launch failed");
            format!("browser launch failed: {}", e)
        })?;
    // Drive the connection in the background.
    tokio::spawn(async move {
        while handler.next().await.is_some() {}
    });
    debug!(elapsed_ms = launch_start.elapsed().as_millis() as u64, "browser launch end");
    Ok(browser)
}

const EXTRACT_JS: &str = r#"(() => {
  const out = [];
  const seen = new Set();
  const idOf = (u) => {
    if (!u) return null;
    let m = u.match(/youtu\.be\/([A-Za-z0-9_-]{5,11})/);
    if (m) return m[1];
    m = u.match(/youtube\.com\/shorts\/([A-Za-z0-9_-]{5,11})/);
    if (m) return m[1];
    m = u.match(/youtube\.com\/embed\/([A-Za-z0-9_-]{5,11})/);
    if (m) return m[1];
    m = u.match(/[?&]v=([A-Za-z0-9_-]{5,11})/);
    if (m) return m[1];
    return null;
  };
  const redditIdOf = (permalink) => {
    if (!permalink) return null;
    let m = permalink.match(/\/comments\/([A-Za-z0-9]+)/);
    if (m) return 't3_' + m[1];
    m = permalink.match(/(t3_[A-Za-z0-9]+)/);
    if (m) return m[1];
    return null;
  };
  const anchors = document.querySelectorAll('a[href]');
  for (const a of anchors) {
    const href = a.getAttribute('href') || '';
    if (href.indexOf('youtube.com') === -1 && href.indexOf('youtu.be') === -1) continue;
    const id = idOf(href);
    if (!id || seen.has(id)) continue;
    seen.add(id);
    let title = (a.getAttribute('title') || (a.textContent || '')).trim();
    const root = a.closest('[data-permalink], .thing, shreddit-post, [data-testid="post-container"]');
    let permalink = null;
    if (root) {
      permalink = root.getAttribute('data-permalink') || root.getAttribute('permalink');
      if (!permalink) {
        const c = root.querySelector('a[href*="/comments/"]');
        if (c) permalink = c.getAttribute('href');
      }
      if (!title) {
        const t = root.querySelector('a.title, [data-testid="post-title"], h3, h2');
        if (t) title = ((t.textContent || '')).trim();
      }
    }
    if (permalink && permalink.charAt(0) === '/') permalink = 'https://www.reddit.com' + permalink;
    let thumbnail = null;
    if (root) {
      const img = root.querySelector('img[src^="http"]');
      if (img) thumbnail = img.getAttribute('src');
    }
    out.push({ youtubeId: id, youtubeUrl: href, title: title || 'Untitled',
               redditUrl: permalink || '', thumbnail: thumbnail || '', createdUtc: null,
               redditId: redditIdOf(permalink) });
    if (out.length >= 300) break;
  }
  let nextHref = null;
  const nextA = document.querySelector('span.next-button a[href], a[rel="nofollow next"]');
  if (nextA) nextHref = nextA.getAttribute('href');
  return { rows: out, nextHref: nextHref };
})()"#;

/// Scrape a single old.reddit URL with capped timeouts. `fast` uses the
/// ~2s first-paint dwell for sub switches; background pagination passes
/// `fast=false` for the longer human-ish dwell + scroll.
async fn scrape_old_reddit_url(
    page: &chromiumoxide::Page,
    url: &str,
    fast: bool,
) -> Result<(Vec<VideoItem>, Option<String>), String> {
    let scrape_start = Instant::now();
    debug!(url = %url, fast = fast, "browser nav start (goto)");
    let nav_timeout = if fast { FAST_NAV_TIMEOUT } else { Duration::from_secs(45) };
    match tokio::time::timeout(nav_timeout, page.goto(url)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            debug!(url = %url, elapsed_ms = scrape_start.elapsed().as_millis() as u64, error = %e, "browser nav failed");
            return Err(format!("navigation failed: {}", e));
        }
        Err(_) => {
            debug!(url = %url, elapsed_ms = scrape_start.elapsed().as_millis() as u64, "browser nav timeout");
            return Err("navigation timeout".to_string());
        }
    }
    // Wait for post content; a timeout here usually means a challenge/block.
    let sel_timeout = if fast { FAST_SELECTOR_TIMEOUT } else { Duration::from_secs(20) };
    match tokio::time::timeout(sel_timeout, page.find_element("a.title, shreddit-post")).await {
        Ok(Ok(_)) => {
            debug!(url = %url, elapsed_ms = scrape_start.elapsed().as_millis() as u64, "browser selector found");
        }
        Ok(Err(e)) => {
            debug!(url = %url, elapsed_ms = scrape_start.elapsed().as_millis() as u64, error = %e, "browser selector missing (BLOCKED?)");
            return Err(format!("BLOCKED: selector not found: {}", e));
        }
        Err(_) => {
            debug!(url = %url, elapsed_ms = scrape_start.elapsed().as_millis() as u64, "browser selector timeout (BLOCKED?)");
            return Err("BLOCKED: selector timeout (possible challenge)".to_string());
        }
    }
    if fast {
        tokio::time::sleep(FAST_DWELL).await;
    } else {
        // Human-ish dwell: 2-6s + jitter.
        let dwell = 2000 + (now_ms() as u64 % 4000) + jitter_15s_ms() % 1000;
        tokio::time::sleep(Duration::from_millis(dwell)).await;
        // One scroll to the bottom to trigger lazy content.
        let _ = page
            .evaluate("window.scrollTo(0, document.body.scrollHeight)")
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let scraped: ScrapePage = page
        .evaluate(EXTRACT_JS)
        .await
        .map_err(|e| format!("extract failed: {}", e))?
        .into_value()
        .map_err(|e| format!("extract decode failed: {}", e))?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<VideoItem> = Vec::new();
    for r in scraped.rows {
        if r.youtube_id.is_empty() || !seen.insert(r.youtube_id.clone()) {
            continue;
        }
        let thumbnail = if r.thumbnail.starts_with("http") {
            clean_url(&r.thumbnail)
        } else {
            PLACEHOLDER_THUMB.to_string()
        };
        let reddit_id = r
            .reddit_id
            .clone()
            .or_else(|| reddit_id_from_permalink(&r.reddit_url));
        out.push(VideoItem {
            youtube_id: r.youtube_id,
            youtube_url: clean_url(&r.youtube_url),
            title: if r.title.is_empty() {
                "Untitled".to_string()
            } else {
                r.title
            },
            reddit_url: r.reddit_url,
            thumbnail,
            created_utc: r.created_utc,
            reddit_id,
        });
        if out.len() >= MAX_POSTS {
            break;
        }
    }
    let after = after_from_next_href(scraped.next_href.as_deref());
    debug!(url = %url, rows = out.len(), has_after = after.is_some(), elapsed_ms = scrape_start.elapsed().as_millis() as u64, "browser extract end");
    Ok((out, after))
}

/// Scrape one sub's newest posts via the shared headless browser.
/// Holds the browser lock for the whole scrape (global serialization).
/// Returns Err with "BLOCKED: ..." prefix when a selector timeout or
/// challenge page suggests bot detection.
///
/// Fast path (`fast=true`) caps navigation/selector waits (~7s) and the
/// dwell (~2s) so sub switches stay instant; it also never queues behind a
/// busy browser — if the cell is contended past FAST_BROWSER_LOCK_TIMEOUT
/// it fails fast with BROWSER_BUSY so the caller falls through to HTTP.
/// Background pagination never takes this lock (HTTP-only).
async fn scrape_sub_via_browser(
    sub: &str,
    fast: bool,
) -> Result<(Vec<VideoItem>, Option<String>), String> {
    if chrome_disabled() {
        debug!(sub = %sub, "browser leg skipped (CHROME_DISABLED)");
        return Err("CHROME_DISABLED".to_string());
    }
    let leg_start = Instant::now();
    debug!(sub = %sub, fast = fast, "browser leg start");
    let cell = browser_cell();
    let mut guard = if fast {
        match tokio::time::timeout(FAST_BROWSER_LOCK_TIMEOUT, cell.lock()).await {
            Ok(g) => g,
            Err(_) => {
                debug!(sub = %sub, elapsed_ms = leg_start.elapsed().as_millis() as u64, "browser leg skipped (BROWSER_BUSY)");
                return Err(
                    "BROWSER_BUSY: browser contended, use HTTP fallback".to_string()
                );
            }
        }
    } else {
        cell.lock().await
    };
    if guard.is_none() {
        match launch_browser().await {
            Ok(b) => *guard = Some(b),
            Err(e) => {
                debug!(sub = %sub, elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "browser leg launch failed");
                return Err(e);
            }
        }
    }
    let browser = guard.as_ref().expect("browser just launched");
    let page = browser
        .new_page("about:blank")
        .await
        .map_err(|e| {
            debug!(sub = %sub, elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "browser leg new_page failed");
            format!("new page failed: {}", e)
        })?;
    let result: Result<(Vec<VideoItem>, Option<String>), String> = async {
        page.enable_stealth_mode_with_agent(CHROME_UA)
            .await
            .map_err(|e| format!("stealth mode failed: {}", e))?;
        // Belt-and-braces webdriver hiding (stealth mode already covers this).
        let _ = page
            .evaluate_on_new_document(
                "Object.defineProperty(Object.getPrototypeOf(navigator), 'webdriver', { get: () => undefined });",
            )
            .await;
        // old.reddit first (lighter); www fallback has no next-page href
        // parsing so it contributes rows with after=None.
        match scrape_old_reddit_url(
            &page,
            &format!("https://old.reddit.com/r/{}/new/", sub),
            fast,
        )
        .await
        {
            Ok(ok) => Ok(ok),
            Err(e) => {
                if fast {
                    // Fast switch: don't burn the nav budget on a slow www
                    // fallback; surface immediately so the API can serve
                    // stale/loading and retry in the background.
                    return Err(e);
                }
                // Background job: try the www fallback once.
                scrape_old_reddit_url(
                    &page,
                    &format!("https://www.reddit.com/r/{}/new/", sub),
                    false,
                )
                .await
            }
        }
    }
    .await;
    let _ = page.close().await;
    match &result {
        Ok((rows, after)) => debug!(sub = %sub, fast = fast, rows = rows.len(), has_after = after.is_some(), elapsed_ms = leg_start.elapsed().as_millis() as u64, "browser leg end (ok)"),
        Err(e) => debug!(sub = %sub, fast = fast, elapsed_ms = leg_start.elapsed().as_millis() as u64, error = %e, "browser leg end (err)"),
    }
    result
}

// ---------- rate limiting ----------

/// Check + record a navigation slot. Err(retry_after_ms) when the
/// per-sub... global limits are hit (MIN_NAV_GAP or hourly cap).
fn nav_gate_reserve(gate: &mut NavGate) -> Result<(), u64> {
    let now = Instant::now();
    while gate
        .nav_times
        .front()
        .map(|t| now.duration_since(*t) > Duration::from_secs(3600))
        .unwrap_or(false)
    {
        gate.nav_times.pop_front();
    }
    if gate.nav_times.len() >= MAX_NAVS_PER_HOUR {
        let oldest = gate.nav_times.front().cloned().unwrap_or(now);
        let retry = Duration::from_secs(3600)
            .checked_sub(now.duration_since(oldest))
            .unwrap_or(Duration::from_secs(60));
        return Err(retry.as_millis() as u64);
    }
    if let Some(last) = gate.last_nav {
        let elapsed = now.duration_since(last);
        if elapsed < MIN_NAV_GAP {
            let wait = (MIN_NAV_GAP - elapsed).as_millis() as u64 + jitter_15s_ms();
            return Err(wait);
        }
    }
    gate.last_nav = Some(now);
    gate.nav_times.push_back(now);
    Ok(())
}

/// Peek without recording (used by POST /api/refresh; the spawned task
/// records when it actually navigates).
fn nav_gate_peek(gate: &mut NavGate) -> Result<(), u64> {
    let now = Instant::now();
    while gate
        .nav_times
        .front()
        .map(|t| now.duration_since(*t) > Duration::from_secs(3600))
        .unwrap_or(false)
    {
        gate.nav_times.pop_front();
    }
    if gate.nav_times.len() >= MAX_NAVS_PER_HOUR {
        let oldest = gate.nav_times.front().cloned().unwrap_or(now);
        let retry = Duration::from_secs(3600)
            .checked_sub(now.duration_since(oldest))
            .unwrap_or(Duration::from_secs(60));
        return Err(retry.as_millis() as u64);
    }
    if let Some(last) = gate.last_nav {
        let elapsed = now.duration_since(last);
        if elapsed < MIN_NAV_GAP {
            let wait = (MIN_NAV_GAP - elapsed).as_millis() as u64 + jitter_15s_ms();
            return Err(wait);
        }
    }
    Ok(())
}

/// Priority lane for never-fetched subs: bypass the 45s MIN_NAV_GAP +
/// hourly sliding-window count; only enforce a short in-progress guard
/// (~6s since the last nav start) to avoid overlapping navigations.
/// Records the slot like the normal gate so accounting stays accurate.
fn nav_gate_reserve_priority(gate: &mut NavGate) -> Result<(), u64> {
    let now = Instant::now();
    if let Some(last) = gate.last_nav {
        let elapsed = now.duration_since(last);
        if elapsed < PRIORITY_NAV_GAP {
            let wait = (PRIORITY_NAV_GAP - elapsed).as_millis() as u64;
            return Err(wait);
        }
    }
    gate.last_nav = Some(now);
    gate.nav_times.push_back(now);
    Ok(())
}

/// Peek for the priority lane without recording.
fn nav_gate_peek_priority(gate: &mut NavGate) -> Result<(), u64> {
    let now = Instant::now();
    if let Some(last) = gate.last_nav {
        let elapsed = now.duration_since(last);
        if elapsed < PRIORITY_NAV_GAP {
            let wait = (PRIORITY_NAV_GAP - elapsed).as_millis() as u64;
            return Err(wait);
        }
    }
    Ok(())
}

// ---------- timeline helpers ----------

fn timeline_arc(state: &AppState, sub: &str) -> Arc<RwLock<SubTimeline>> {
    state
        .timelines
        .entry(sub.to_string())
        .or_insert_with(|| Arc::new(RwLock::new(SubTimeline::default())))
        .clone()
}

fn inflight_lock(state: &AppState, sub: &str) -> Arc<Mutex<()>> {
    state
        .inflight
        .entry(sub.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn paginate_lock(state: &AppState, sub: &str) -> Arc<Mutex<()>> {
    state
        .paginate_inflight
        .entry(sub.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

fn slice_body(
    sub: &str,
    tl: &SubTimeline,
    start: usize,
    limit: usize,
    cached: bool,
    retry_after_ms: Option<u64>,
) -> serde_json::Value {
    let now = now_ms();
    let stale = tl
        .fetched_at_ms
        .map(|f| now - f > TTL_MS)
        .unwrap_or(true);
    let start = start.min(tl.posts.len());
    let end = (start + limit).min(tl.posts.len());
    let videos = &tl.posts[start..end];
    // Endless feed: more may exist on reddit even when the in-memory slice
    // is exhausted (reddit_after cursor) or a background job is running.
    // The tail cursor is echoed (even for an empty timeline) while more is
    // pending so the client can retry the same position instead of seeing
    // after=null as terminal and stalling the endless feed.
    let has_more =
        end < tl.posts.len() || tl.reddit_after.is_some() || tl.loading;
    let next_after = if end < tl.posts.len() {
        Some(end.to_string())
    } else if has_more {
        // Tail slice but more may arrive (reddit_after cursor or background
        // job pending): echo the tail index so the client can retry the same
        // cursor instead of losing its position with after=null.
        Some(end.to_string())
    } else {
        None
    };
    let mut body = serde_json::json!({
        "sub": sub,
        "videos": videos,
        "after": next_after,
        "hasMore": has_more,
        "cached": cached,
        "fetchedAt": tl.fetched_at_ms,
        "stale": stale,
        "loading": tl.loading,
    });
    if let Some(ms) = retry_after_ms {
        body["retryAfterMs"] = serde_json::Value::Number(ms.into());
    }
    body
}

/// Store a head fill in the timeline (first fetch / fresh newest page).
/// Never clobbers a known t3_* cursor with a cursor-less (RSS-only) leg:
// `reddit_after` is only assigned when Some.
async fn store_head_fill(
    tl: &Arc<RwLock<SubTimeline>>,
    posts: Vec<VideoItem>,
    reddit_after: Option<String>,
) {
    let rows = posts.len();
    let incoming_after = reddit_after.clone();
    let mut w = tl.write().await;
    let prev_after = w.reddit_after.clone();
    w.posts = dedupe_cap(posts);
    if reddit_after.is_some() {
        w.reddit_after = reddit_after;
    }
    w.fetched_at_ms = Some(now_ms());
    w.last_error = None;
    w.blocked_until_ms = None;
    w.rate_limited_until_ms = None;
    w.loading = false;
    let kept = incoming_after.is_none() && prev_after.is_some();
    info!(
        rows = rows,
        cursor_updated = incoming_after.is_some(),
        cursor_kept = kept,
        has_after = w.reddit_after.is_some(),
        "store_head_fill"
    );
}

/// Merge a refreshed newest page at the head (unseen newer first).
async fn apply_head_merge(
    tl: &Arc<RwLock<SubTimeline>>,
    fresh_page: Vec<VideoItem>,
    reddit_after: Option<String>,
) {
    let incoming_rows = fresh_page.len();
    let incoming_after = reddit_after.clone();
    let mut w = tl.write().await;
    let before = w.posts.len();
    let prev_after = w.reddit_after.clone();
    let merged = merge_newest_head(std::mem::take(&mut w.posts), fresh_page);
    let added = merged.len().saturating_sub(before.min(merged.len()));
    w.posts = merged;
    if reddit_after.is_some() {
        w.reddit_after = reddit_after;
    }
    w.fetched_at_ms = Some(now_ms());
    w.last_error = None;
    w.blocked_until_ms = None;
    w.rate_limited_until_ms = None;
    w.loading = false;
    let kept = incoming_after.is_none() && prev_after.is_some();
    info!(
        incoming_rows = incoming_rows,
        unseen_added = added,
        total = w.posts.len(),
        cursor_updated = incoming_after.is_some(),
        cursor_kept = kept,
        "apply_head_merge"
    );
}

/// Merge an older next-page at the tail: only unseen older items appended.
/// Keeps head freshness (`fetched_at_ms`) untouched so TTL still fires.
async fn apply_tail_merge(
    tl: &Arc<RwLock<SubTimeline>>,
    next_page: Vec<VideoItem>,
    next_after: Option<String>,
) {
    let incoming_rows = next_page.len();
    let mut w = tl.write().await;
    let before = w.posts.len();
    let merged = merge_older_tail(std::mem::take(&mut w.posts), next_page);
    let appended = merged.len().saturating_sub(before);
    w.posts = merged;
    w.reddit_after = next_after.clone();
    w.last_error = None;
    w.rate_limited_until_ms = None;
    w.loading = false;
    info!(
        incoming_rows = incoming_rows,
        appended = appended,
        total = w.posts.len(),
        has_next_after = next_after.is_some(),
        "apply_tail_merge"
    );
}

async fn set_loading(tl: &Arc<RwLock<SubTimeline>>, loading: bool) {
    let mut w = tl.write().await;
    w.loading = loading;
}

async fn mark_blocked(tl: &Arc<RwLock<SubTimeline>>, reason: String) {
    let mut w = tl.write().await;
    w.last_error = Some("BLOCKED".to_string());
    // Empty timelines get a short retryable backoff so a single transient
    // 403 does not 3h-blackout the sub; populated timelines keep the 3h
    // politeness backoff.
    let backoff = if w.posts.is_empty() {
        SHORT_BLOCKED_BACKOFF_MS
    } else {
        BLOCKED_BACKOFF_MS
    };
    w.blocked_until_ms = Some(now_ms() + backoff);
    w.loading = false;
    warn!(
        backoff_ms = backoff,
        empty_timeline = w.posts.is_empty(),
        reason = %reason,
        "mark_blocked: sub backed off after block/challenge"
    );
}

async fn finish_with_error(
    tl: &Arc<RwLock<SubTimeline>>,
    code: StatusCode,
    msg: String,
    retry_after: Option<u64>,
) {
    let mut w = tl.write().await;
    if code == StatusCode::TOO_MANY_REQUESTS {
        w.last_error = Some("UPSTREAM_429".to_string());
        // Negative cache: hold the sub in cooldown so frontend repolls
        // return loading+retryAfterMs without retrying upstream and
        // extending the ban. Honors upstream Retry-After when present.
        let backoff = upstream_429_cooldown_ms(retry_after);
        w.rate_limited_until_ms = Some(now_ms() + backoff);
        warn!(status = 429, backoff_ms = backoff, retry_after_ms = ?retry_after, "fetch ended rate-limited (UPSTREAM_429, cooldown set)");
    } else if msg.contains("403") || msg.contains("BLOCKED") {
        w.last_error = Some("BLOCKED".to_string());
        // Single-leg transient 403 on an empty timeline: short backoff so
        // the feed stays retryable; non-empty timelines keep the 3h block.
        let backoff = if w.posts.is_empty() {
            SHORT_BLOCKED_BACKOFF_MS
        } else {
            BLOCKED_BACKOFF_MS
        };
        w.blocked_until_ms = Some(now_ms() + backoff);
        warn!(backoff_ms = backoff, empty_timeline = w.posts.is_empty(), error = %msg, "fetch ended blocked");
    } else {
        debug!(status = code.as_u16(), error = %msg, "fetch ended with error");
        w.last_error = Some(msg);
    }
    w.loading = false;
}

/// Peek the nav gate for a retry hint without consuming a slot.
/// Returns Some(ms) when a browser navigation right now would be gated.
async fn gate_retry_hint(state: &AppState) -> Option<u64> {
    let mut gate = state.nav_gate.lock().await;
    nav_gate_peek(&mut gate).err()
}

/// Background head refresh: re-fetch the newest page and head-merge it.
/// Never blocks a response; updates the timeline in place.
///
/// Race, don't sequence: the browser leg (fast ~7s caps, nav slot
/// reserved, 1s lock timeout so it never queues behind a busy browser)
/// runs concurrently with the reqwest legs. First paint uses fast parallel
/// mirror legs (arctic+pullpush+redlib+rss ~4s each via tokio::join, OAuth
/// token POST skipped/deferred to enrichment) so a never-fetched sub fills
/// in ~4s instead of stacking sequential 10s timeouts. Enrichment keeps the
/// full fallback chain (OAuth-first only when opted in via
/// REDDIT_AUTH_MODE=script with creds, else mirrors+RSS) with longer
/// budgets.
/// The first leg with rows>0 does the head fill; the loser head-merges
/// unseen rows if it arrives later. BROWSER_BUSY / gate exhaustion just
/// means the HTTP legs decide alone.
async fn background_head_refresh(state: AppState, sub: String) {
    let job_start = Instant::now();
    let tl = timeline_arc(&state, &sub);
    {
        let r = tl.read().await;
        // 429 negative cache first: no upstream retry while cooling down.
        if let Some(remain) = rate_limit_remaining_ms(&r, now_ms()) {
            info!(sub = %sub, retry_after_ms = remain, "head_refresh cooldown active skip (UPSTREAM_429 negative cache)");
            return;
        }
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                let retry_ms = (until - now_ms()).max(0) as u64;
                debug!(sub = %sub, retry_after_ms = retry_ms, "head_refresh skip (backoff active)");
                set_loading(&tl, false).await;
                return;
            }
        }
    }
    let lock = inflight_lock(&state, &sub);
    let Ok(_guard) = lock.try_lock() else {
        info!(sub = %sub, "head_refresh coalesced (single-flight already running)");
        return;
    };
    set_loading(&tl, true).await;
    let first_paint = tl.read().await.posts.is_empty();
    info!(sub = %sub, first_paint = first_paint, "head_refresh start");
    // Chrome-disabled (CI/tests): HTTP-only head fill, no race needed.
    // First paint uses the fast parallel mirror legs (arctic+pullpush+
    // redlib+rss ~4s each, OAuth skipped); enrichment keeps the full
    // fallback chain (OAuth-first only when opted in, else mirrors+RSS).
    if chrome_disabled() {
        let res = if first_paint {
            fetch_http_first_paint(&state, &sub).await
        } else {
            fetch_http_fallback(&state, &sub, None).await
        };
        match res {
            Ok((posts, after)) => {
                let rows = posts.len();
                let has_after = after.is_some();
                if first_paint {
                    store_head_fill(&tl, posts, after).await;
                } else {
                    apply_head_merge(&tl, posts, after).await;
                }
                info!(sub = %sub, first_paint = first_paint, winner = "http(chrome-disabled)", rows = rows, has_after = has_after, elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (filled)");
            }
            Err((code, msg, retry)) => {
                info!(sub = %sub, first_paint = first_paint, status = code.as_u16(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (error)");
                finish_with_error(&tl, code, msg, retry).await;
            }
        }
        return;
    }
    let gate_reserve: Result<(), u64> = {
        let mut gate = state.nav_gate.lock().await;
        // Priority lane: a never-fetched sub bypasses the 45s+jitter +
        // hourly cap and only respects the short overlap guard so the
        // first paint fetches then and there.
        if first_paint {
            nav_gate_reserve_priority(&mut gate)
        } else {
            nav_gate_reserve(&mut gate)
        }
    };
    let gate_ok = gate_reserve.is_ok();
    if !gate_ok {
        // Gate exhausted: HTTP needs no nav slot, so it still makes progress.
        // Fast legs on first paint; full chain for enrichment.
        if let Err(retry_ms) = gate_reserve {
            info!(sub = %sub, first_paint = first_paint, retry_after_ms = retry_ms, "nav_gate reserve miss (http-only fallback)");
        }
        let res = if first_paint {
            fetch_http_first_paint(&state, &sub).await
        } else {
            fetch_http_fallback(&state, &sub, None).await
        };
        match res {
            Ok((posts, after)) => {
                let rows = posts.len();
                let has_after = after.is_some();
                if first_paint {
                    store_head_fill(&tl, posts, after).await;
                } else {
                    apply_head_merge(&tl, posts, after).await;
                }
                info!(sub = %sub, first_paint = first_paint, winner = "http(gate-miss)", rows = rows, has_after = has_after, elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (filled)");
            }
            Err((code, msg, retry)) => {
                info!(sub = %sub, first_paint = first_paint, status = code.as_u16(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (error)");
                finish_with_error(&tl, code, msg, retry).await;
            }
        }
        return;
    }
    debug!(sub = %sub, first_paint = first_paint, lane = if first_paint { "priority" } else { "normal" }, "nav_gate reserve hit");
    // Politeness: the nav slot above covers the browser leg. Race both legs
    // concurrently so HTTP first paint never waits out the ~7s+7s+2s
    // browser budget. First paint: fast parallel mirror legs (arctic+
    // pullpush+redlib+rss ~4s each, OAuth skipped); enrichment: full chain
    // with longer budgets.
    let http_state = state.clone();
    let sub_http = sub.clone();
    let sub_browser = sub.clone();
    let mut browser_handle =
        tokio::spawn(async move { scrape_sub_via_browser(&sub_browser, true).await });
    let mut http_handle = if first_paint {
        tokio::spawn(
            async move { fetch_http_first_paint(&http_state, &sub_http).await },
        )
    } else {
        tokio::spawn(
            async move { fetch_http_fallback(&http_state, &sub_http, None).await },
        )
    };

    type BrowserOut = Result<(Vec<VideoItem>, Option<String>), String>;
    type HttpOut = Result<
        (Vec<VideoItem>, Option<String>),
        (StatusCode, String, Option<u64>),
    >;
    enum First {
        Browser(Result<BrowserOut, tokio::task::JoinError>),
        Http(Result<HttpOut, tokio::task::JoinError>),
    }
    let first = tokio::select! {
        b = &mut browser_handle => First::Browser(b),
        h = &mut http_handle => First::Http(h),
    };
    // Store the first leg with rows>0; remember the other leg to merge.
    // (first_paint only applies to the very first store; the loser always
    // head-merges so playback order is preserved.)
    let mut stored = false;
    let mut winner_leg: Option<&str> = None;
    let mut browser_err: Option<String> = None;
    let mut http_err: Option<(StatusCode, String, Option<u64>)> = None;
    // Deferred loser results, awaited after the first store.
    let mut pending_browser: Option<
        tokio::task::JoinHandle<BrowserOut>,
    > = None;
    let mut pending_http: Option<tokio::task::JoinHandle<HttpOut>> = None;

    async fn store_posts(
        tl: &Arc<RwLock<SubTimeline>>,
        first_paint: bool,
        posts: Vec<VideoItem>,
        after: Option<String>,
        stored: &mut bool,
    ) {
        if *stored {
            apply_head_merge(tl, dedupe_cap(posts), after).await;
        } else if first_paint {
            store_head_fill(tl, posts, after).await;
            *stored = true;
        } else {
            apply_head_merge(tl, dedupe_cap(posts), after).await;
            *stored = true;
        }
    }

    match first {
        First::Browser(Ok(Ok((posts, after)))) => {
            pending_http = Some(http_handle);
            debug!(sub = %sub, leg = "browser", rows = posts.len(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh first leg arrived (browser)");
            if !posts.is_empty() {
                winner_leg = Some("browser");
                store_posts(&tl, first_paint, posts, after, &mut stored).await;
            } else {
                // Empty fast leg: don't store yet; the HTTP leg decides.
                // (If HTTP also comes back empty/failed we fall through to
                // the error/clear-loading handling below.)
            }
        }
        First::Browser(Ok(Err(e))) => {
            pending_http = Some(http_handle);
            debug!(sub = %sub, leg = "browser", elapsed_ms = job_start.elapsed().as_millis() as u64, error = %e, "head_refresh first leg failed (browser)");
            if e.contains("BLOCKED") {
                browser_err = Some(e);
            } else if e != "CHROME_DISABLED" && !e.contains("BROWSER_BUSY") {
                let mut w = tl.write().await;
                w.last_error = Some(e);
            }
            // BROWSER_BUSY / fast miss: HTTP decides alone.
        }
        First::Browser(Err(join_err)) => {
            pending_http = Some(http_handle);
            debug!(sub = %sub, leg = "browser", elapsed_ms = job_start.elapsed().as_millis() as u64, error = %join_err, "head_refresh browser task panicked");
            let mut w = tl.write().await;
            w.last_error = Some(format!("browser task failed: {}", join_err));
        }
        First::Http(Ok(Ok((posts, after)))) => {
            pending_browser = Some(browser_handle);
            debug!(sub = %sub, leg = "http", rows = posts.len(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh first leg arrived (http)");
            if !posts.is_empty() {
                winner_leg = Some("http");
                store_posts(&tl, first_paint, posts, after, &mut stored).await;
            } else if first_paint {
                // HTTP head is empty: still store it so loading clears and
                // the tail cursor echoes; a later non-empty browser leg
                // head-merges on top.
                store_head_fill(&tl, posts, after).await;
                stored = true;
            } else {
                apply_head_merge(&tl, posts, after).await;
                stored = true;
            }
        }
        First::Http(Ok(Err(e))) => {
            pending_browser = Some(browser_handle);
            debug!(sub = %sub, leg = "http", elapsed_ms = job_start.elapsed().as_millis() as u64, error = %e.1, "head_refresh first leg failed (http)");
            http_err = Some(e);
        }
        First::Http(Err(join_err)) => {
            pending_browser = Some(browser_handle);
            debug!(sub = %sub, leg = "http", elapsed_ms = job_start.elapsed().as_millis() as u64, error = %join_err, "head_refresh http task panicked");
            http_err = Some((
                StatusCode::BAD_GATEWAY,
                format!("http task failed: {}", join_err),
                None,
            ));
        }
    }

    // Head-merge the loser if it arrives later with unseen rows. The loser
    // is already in flight and bounded (fast ~7s caps / reqwest timeout),
    // so awaiting it never stalls a response — this task is background-only.
    if let Some(handle) = pending_http {
        match handle.await {
            Ok(Ok((posts, after))) => {
                debug!(sub = %sub, leg = "http", rows = posts.len(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh loser arrived (http)");
                if !posts.is_empty() {
                    if winner_leg.is_none() {
                        winner_leg = Some("http(loser)");
                    }
                    store_posts(&tl, first_paint && !stored, posts, after, &mut stored)
                        .await;
                } else if !stored {
                    if first_paint {
                        store_head_fill(&tl, posts, after).await;
                    }
                    stored = true;
                }
            }
            Ok(Err(e)) => {
                if http_err.is_none() {
                    http_err = Some(e);
                }
            }
            Err(join_err) => {
                if http_err.is_none() {
                    http_err = Some((
                        StatusCode::BAD_GATEWAY,
                        format!("http task failed: {}", join_err),
                        None,
                    ));
                }
            }
        }
    } else if let Some(handle) = pending_browser {
        match handle.await {
            Ok(Ok((posts, after))) => {
                debug!(sub = %sub, leg = "browser", rows = posts.len(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh loser arrived (browser)");
                if !posts.is_empty() {
                    if winner_leg.is_none() {
                        winner_leg = Some("browser(loser)");
                    }
                    store_posts(&tl, first_paint && !stored, posts, after, &mut stored)
                        .await;
                }
                // Empty browser leg after an HTTP decision: nothing to do.
                if !stored {
                    stored = true;
                    set_loading(&tl, false).await;
                }
            }
            Ok(Err(e)) => {
                if browser_err.is_none()
                    && e.contains("BLOCKED")
                {
                    browser_err = Some(e);
                } else if browser_err.is_none()
                    && e != "CHROME_DISABLED"
                    && !e.contains("BROWSER_BUSY")
                {
                    let mut w = tl.write().await;
                    w.last_error = Some(e);
                }
                if !stored {
                    stored = true;
                    set_loading(&tl, false).await;
                }
            }
            Err(join_err) => {
                if !stored {
                    let mut w = tl.write().await;
                    w.last_error = Some(format!("browser task failed: {}", join_err));
                    w.loading = false;
                    stored = true;
                }
            }
        }
    }

    if stored {
        let r = tl.read().await;
        info!(sub = %sub, first_paint = first_paint, winner = winner_leg.unwrap_or("empty"), rows = r.posts.len(), has_after = r.reddit_after.is_some(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (filled)");
        return;
    }
    // Neither leg produced rows: surface block/rate-limit state. A single
    // BLOCKED leg alone must not 3h-blackout the sub when the other leg
    // failed differently (timeout/transient) — only a confirmed block on
    // both legs (or an HTTP 403/BLOCKED of its own) escalates to mark_blocked.
    let http_blocked = http_err
        .as_ref()
        .map(|(_, msg, _)| msg.contains("403") || msg.contains("BLOCKED"))
        .unwrap_or(false);
    let browser_blocked = browser_err
        .as_ref()
        .map(|e| e.contains("BLOCKED"))
        .unwrap_or(false);
    if browser_blocked && http_blocked {
        // Confirmed block on both legs: escalate to backoff.
        if let Some(e) = browser_err {
            info!(sub = %sub, first_paint = first_paint, elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (blocked, both legs)");
            mark_blocked(&tl, e).await;
            return;
        }
    } else if browser_blocked {
        // Single-leg browser BLOCKED with a non-block HTTP failure:
        // record it without a 3h blackout so the next poll retries soon.
        info!(sub = %sub, first_paint = first_paint, elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (browser blocked, http failed differently)");
        if let Some((code, msg, retry)) = http_err {
            finish_with_error(&tl, code, msg, retry).await;
        } else {
            let mut w = tl.write().await;
            w.last_error = browser_err.clone();
            w.loading = false;
        }
        return;
    }
    if let Some((code, msg, retry)) = http_err {
        info!(sub = %sub, first_paint = first_paint, status = code.as_u16(), elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (error)");
        finish_with_error(&tl, code, msg, retry).await;
    } else {
        info!(sub = %sub, first_paint = first_paint, elapsed_ms = job_start.elapsed().as_millis() as u64, "head_refresh end (empty, no rows)");
        set_loading(&tl, false).await;
    }
}

/// Background pagination: fetch the next older reddit page (via the stored
/// `reddit_after` cursor) and tail-merge only unseen older items. Serve-now,
/// prefetch-in-background: the request that triggered this already returned.
///
/// HTTP-only by design: no browser nav slot, no dwell, no browser lock.
/// Pagination therefore never contends the browser cell and can never delay
/// a priority head fill. Same-sub overlap is still single-flight via
/// try_lock (fail-fast, never queues); a sub switch targets a different
/// per-sub lock, so focus changes need no abort — the old sub's paginate
/// just tail-merges whenever its HTTP fetch lands.
async fn background_paginate(state: AppState, sub: String) {
    let job_start = Instant::now();
    let tl = timeline_arc(&state, &sub);
    {
        let r = tl.read().await;
        if let Some(remain) = rate_limit_remaining_ms(&r, now_ms()) {
            debug!(sub = %sub, retry_after_ms = remain, "paginate cooldown active skip (UPSTREAM_429 negative cache)");
            return;
        }
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                debug!(sub = %sub, "paginate skip (backoff active)");
                return;
            }
        }
    }
    let cursor = {
        let r = tl.read().await;
        match r.reddit_after.clone() {
            Some(c) => c,
            None => {
                debug!(sub = %sub, "paginate skip (no cursor)");
                return; // nothing older known; head refresh covers it
            }
        }
    };
    let lock = paginate_lock(&state, &sub);
    let Ok(_guard) = lock.try_lock() else {
        info!(sub = %sub, after = %cursor, "paginate coalesced (single-flight already running)");
        return;
    };
    set_loading(&tl, true).await;
    info!(sub = %sub, after = %cursor, "paginate start");
    match fetch_http_fallback(&state, &sub, Some(&cursor)).await {
        Ok((posts, next_after)) => {
            let rows = posts.len();
            let has_next = next_after.is_some();
            apply_tail_merge(&tl, posts, next_after).await;
            info!(sub = %sub, after = %cursor, rows = rows, has_next_after = has_next, elapsed_ms = job_start.elapsed().as_millis() as u64, "paginate end (appended)");
        }
        Err((code, msg, retry)) => {
            info!(sub = %sub, after = %cursor, status = code.as_u16(), elapsed_ms = job_start.elapsed().as_millis() as u64, "paginate end (error)");
            finish_with_error(&tl, code, msg, retry).await;
        }
    }
}

// ---------- handlers ----------

async fn healthz() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

async fn subs() -> impl IntoResponse {
    Json(serde_json::json!({ "subs": DEFAULT_SUBS }))
}

async fn videos_handler(
    State(state): State<AppState>,
    Query(q): Query<VideosQuery>,
) -> Response {
    let req_start = Instant::now();
    let sub = q.sub.unwrap_or_else(|| "videos".to_string());
    if !is_valid_sub(&sub) {
        info!(method = "GET", route = "/api/videos", sub = %sub, status = 400, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (BAD_SUB)");
        return error_json(
            StatusCode::BAD_REQUEST,
            "BAD_SUB",
            format!("invalid subreddit name: {:?}", sub),
            None,
        );
    }
    let limit: u32 = match &q.limit {
        None => 25,
        Some(s) => match s.parse::<i64>() {
            Ok(n) if (1..=100).contains(&n) => n as u32,
            _ => {
                info!(method = "GET", route = "/api/videos", sub = %sub, status = 400, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (BAD_LIMIT)");
                return error_json(
                    StatusCode::BAD_REQUEST,
                    "BAD_LIMIT",
                    "limit must be an integer 1..100".to_string(),
                    None,
                );
            }
        },
    };
    let after: Option<String> = q.after.filter(|s| !s.is_empty());
    let start = parse_after(&after);
    let refresh_req = q.refresh.unwrap_or(false);

    let tl = timeline_arc(&state, &sub);

    // Blocked backoff with nothing cached: fail fast without spawning.
    // Includes retryAfterMs so the client can show a countdown instead of
    // a dead error; loading semantics stay server-side via blocked_until.
    {
        let r = tl.read().await;
        if r.posts.is_empty() {
            if let Some(until) = r.blocked_until_ms {
                let now = now_ms();
                if now < until {
                    let retry_ms = (until - now).max(0) as u64;
                    info!(method = "GET", route = "/api/videos", sub = %sub, limit = limit, refresh = refresh_req, status = 502, served_from = "empty-loading", retry_after_ms = retry_ms, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (UPSTREAM_BLOCKED backoff)");
                    return error_json(
                        StatusCode::BAD_GATEWAY,
                        "UPSTREAM_BLOCKED",
                        format!(
                            "Reddit blocked automated access to r/{} (backoff); try again later",
                            sub
                        ),
                        Some(retry_ms),
                    );
                }
            }
        }
    }

    // 429 negative-cache cooldown: while active, return immediately with
    // {videos:[], loading:true, retryAfterMs} WITHOUT spawning new upstream
    // jobs and WITHOUT touching nav gates. Single-flight is implicit: no
    // job is spawned here, so concurrent polls coalesce on the cooldown.
    let cooldown_remain: Option<u64> = {
        let r = tl.read().await;
        rate_limit_remaining_ms(&r, now_ms())
    };
    if let Some(remain) = cooldown_remain {
        let empty = tl.read().await.posts.is_empty();
        if empty {
            let r = tl.read().await;
            let mut body = slice_body(&sub, &r, start, limit as usize, true, Some(remain));
            // Force loading:true so the frontend keeps its countdown/badge
            // state instead of treating the empty slice as terminal.
            body["loading"] = serde_json::Value::Bool(true);
            body["stale"] = serde_json::Value::Bool(true);
            info!(method = "GET", route = "/api/videos", sub = %sub, limit = limit, refresh = refresh_req, status = 200, served_from = "cooldown", videos = 0, loading = true, retry_after_ms = remain, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (UPSTREAM_429 cooldown active skip)");
            return (StatusCode::OK, Json(body)).into_response();
        }
        // Non-empty timeline under cooldown: serve the cached slice now
        // with the retry hint, no new job, no nav-gate accounting.
        let r = tl.read().await;
        let body = slice_body(&sub, &r, start, limit as usize, true, Some(remain));
        let videos_count = body["videos"].as_array().map(|a| a.len()).unwrap_or(0);
        info!(method = "GET", route = "/api/videos", sub = %sub, limit = limit, after = ?after, refresh = refresh_req, status = 200, served_from = "cooldown-cached", videos = videos_count, retry_after_ms = remain, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (UPSTREAM_429 cooldown cached)");
        return (StatusCode::OK, Json(body)).into_response();
    }

    // Snapshot for routing: fresh? stale? near the tail?
    let (is_stale, len, has_cursor, already_loading, end) = {
        let r = tl.read().await;
        let now = now_ms();
        let stale = r
            .fetched_at_ms
            .map(|f| now - f > TTL_MS)
            .unwrap_or(true);
        let len = r.posts.len();
        let end = start.min(len).saturating_add(limit as usize).min(len);
        (
            stale,
            len,
            r.reddit_after.is_some(),
            r.loading,
            end,
        )
    };
    let near_tail = end.saturating_add(PREFETCH_TAIL_THRESHOLD) >= len;
    // Priority lane: a never-fetched sub (empty timeline) bypasses the
    // sliding-window count — it fetches then and there. Only a nav
    // literally in flight (already_loading via the per-sub single-flight
    // lock) may surface loading; never a 429/countdown with empty videos.
    let never_fetched = len == 0;

    // Fast switch: never block on the browser. Serve the current slice now;
    // prefetch older pages when within 5 items of the tail, otherwise queue
    // a head refresh when stale/missing/forced. The 45s MIN_NAV_GAP never
    // blocks: it surfaces as a retryAfterMs hint with loading:true.
    #[derive(Clone, Copy)]
    enum Job {
        Paginate,
        Head,
        None,
    }
    // Head/paginate locks are split (inflight vs paginate_inflight): a tail
    // prefetch proceeds over HTTP even while a head fill holds its lock.
    // Likewise the handler must not stall the tail while head loading is
    // true — paginate is dispatched whenever the cursor is near the tail,
    // independent of already_loading (its own try_lock stays fail-fast).
    let job = if len > 0 && near_tail && has_cursor {
        // Paginate needs its own single-flight check; if a paginate job is
        // already running its try_lock fails fast inside the task. Only
        // suppress re-spawn when the timeline already reports loading AND
        // there is no head work pending — head loading alone must not stall
        // the tail. We dispatch paginate regardless; the task de-dupes.
        Job::Paginate
    } else if (is_stale || refresh_req || len == 0) && !already_loading {
        Job::Head
    } else if (is_stale || refresh_req || len == 0) && already_loading {
        Job::None // job already running; just report loading
    } else {
        Job::None
    };
    let retry_hint = match job {
        Job::Paginate | Job::Head if !never_fetched => gate_retry_hint(&state).await,
        Job::Paginate | Job::Head => None,
        Job::None => {
            if already_loading && !never_fetched {
                gate_retry_hint(&state).await
            } else {
                None
            }
        }
    };
    match job {
        Job::Paginate => {
            set_loading(&tl, true).await;
            let bg_state = state.clone();
            let bg_sub = sub.clone();
            tokio::spawn(async move {
                background_paginate(bg_state, bg_sub).await;
            });
        }
        Job::Head => {
            set_loading(&tl, true).await;
            let bg_state = state.clone();
            let bg_sub = sub.clone();
            tokio::spawn(async move {
                background_head_refresh(bg_state, bg_sub).await;
            });
        }
        Job::None => {}
    }

    let r = tl.read().await;
    let mut body = slice_body(&sub, &r, start, limit as usize, true, retry_hint);
    if refresh_req {
        // Requested fresh data that is still being fetched: flag stale so
        // the UI can show a refreshing state.
        body["stale"] = serde_json::Value::Bool(true);
        if matches!(job, Job::Head) || r.loading {
            body["loading"] = serde_json::Value::Bool(true);
        }
    }
    let videos_count = body["videos"].as_array().map(|a| a.len()).unwrap_or(0);
    let loading = body["loading"].as_bool().unwrap_or(false);
    let has_more = body["hasMore"].as_bool().unwrap_or(false);
    let served_from = if never_fetched {
        "empty-loading"
    } else if is_stale || refresh_req {
        "stale"
    } else {
        "fresh-cache"
    };
    let job_label = match job {
        Job::Paginate => "paginate",
        Job::Head => "head",
        Job::None => "none",
    };
    info!(
        method = "GET",
        route = "/api/videos",
        sub = %sub,
        limit = limit,
        after = ?after,
        refresh = refresh_req,
        status = 200,
        served_from = served_from,
        videos = videos_count,
        loading = loading,
        has_more = has_more,
        job = job_label,
        retry_after_ms = ?retry_hint,
        elapsed_ms = req_start.elapsed().as_millis() as u64,
        "request"
    );
    (StatusCode::OK, Json(body)).into_response()
}

async fn refresh_handler(
    State(state): State<AppState>,
    Query(q): Query<RefreshQuery>,
) -> Response {
    let req_start = Instant::now();
    let sub = q.sub.unwrap_or_else(|| "videos".to_string());
    if !is_valid_sub(&sub) {
        info!(method = "POST", route = "/api/refresh", sub = %sub, status = 400, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (BAD_SUB)");
        return error_json(
            StatusCode::BAD_REQUEST,
            "BAD_SUB",
            format!("invalid subreddit name: {:?}", sub),
            None,
        );
    }
    let tl = timeline_arc(&state, &sub);
    // 429 cooldown: fail fast with the remaining hint, no new job, no gate.
    {
        let r = tl.read().await;
        if let Some(remain) = rate_limit_remaining_ms(&r, now_ms()) {
            info!(method = "POST", route = "/api/refresh", sub = %sub, status = 429, retry_after_ms = remain, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (UPSTREAM_429 cooldown active skip)");
            return error_json(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                format!("r/{} is rate-limited upstream; retry shortly", sub),
                Some(remain),
            );
        }
    }
    // Priority lane: a never-fetched sub bypasses the global nav-gate peek.
    // Only a nav literally in flight (per-sub single-flight try_lock) may
    // return 429; otherwise queue an immediate nav and report loading.
    let never_fetched = tl.read().await.posts.is_empty();
    // Rate limiter intact: per-sub single-flight + global nav-gate peek.
    {
        let lock = inflight_lock(&state, &sub);
        if lock.try_lock().is_err() {
            info!(method = "POST", route = "/api/refresh", sub = %sub, status = 429, served_from = "coalesced", elapsed_ms = req_start.elapsed().as_millis() as u64, "request (single-flight coalesced)");
            return error_json(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                format!("r/{} is already being fetched; retry shortly", sub),
                Some(10_000),
            );
        }
        if !never_fetched {
            let mut gate = state.nav_gate.lock().await;
            if let Err(retry_ms) = nav_gate_peek(&mut gate) {
                info!(method = "POST", route = "/api/refresh", sub = %sub, status = 429, retry_after_ms = retry_ms, elapsed_ms = req_start.elapsed().as_millis() as u64, "request (nav-gate peek miss)");
                return error_json(
                    StatusCode::TOO_MANY_REQUESTS,
                    "RATE_LIMITED",
                    "browser navigation budget exhausted; retry later".to_string(),
                    Some(retry_ms),
                );
            }
        } else {
            // Priority peek: only the short overlap guard applies. When hot,
            // still queue (the background job will fall through to HTTP) —
            // never 429 an empty timeline on the gate.
            let mut gate = state.nav_gate.lock().await;
            let _ = nav_gate_peek_priority(&mut gate);
        }
    }
    // Non-blocking: serve the snapshot now, refresh in the background so a
    // slow browser nav or the 45s gap never stalls the switch.
    set_loading(&tl, true).await;
    let bg_state = state.clone();
    let bg_sub = sub.clone();
    tokio::spawn(async move {
        background_head_refresh(bg_state, bg_sub).await;
    });
    let r = tl.read().await;
    let body = serde_json::json!({
        "sub": sub,
        "queued": true,
        "cached": true,
        "fetchedAt": r.fetched_at_ms,
        "stale": true,
        "loading": true,
    });
    info!(method = "POST", route = "/api/refresh", sub = %sub, status = 200, served_from = "queued-refresh", elapsed_ms = req_start.elapsed().as_millis() as u64, "request (refresh queued)");
    (StatusCode::OK, Json(body)).into_response()
}

#[tokio::main]
async fn main() {
    // Human-readable stdout logs with timestamps. RUST_LOG controls levels,
    // defaulting to info (requests + scrape start/end + fills/blocks);
    // per-leg details (public/RSS/OAuth/browser nav/extract) are debug.
    // Example: `RUST_LOG=debug cargo run` to watch scrapes live.
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "reddittv=info,tower_http=info".into()),
        )
        .init();

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("failed to build http client");

    let state = AppState {
        client,
        timelines: Arc::new(DashMap::new()),
        inflight: Arc::new(DashMap::new()),
        paginate_inflight: Arc::new(DashMap::new()),
        nav_gate: Arc::new(Mutex::new(NavGate::default())),
        oauth: Arc::new(OAuthState::default()),
        mirrors: Arc::new(MirrorState::default()),
    };

    // Startup auth mode (never log secrets — only the mode label).
    // script = REDDIT_AUTH_MODE=script with complete script creds (opt-in);
    // no-auth (default) needs no registration and uses the mirror chain.
    let mode = auth_mode();
    info!(auth_mode = auth_mode_label(mode), "reddit auth mode");
    if mode == AuthMode::Script && !is_script_ua(&reddit_ua()) {
        warn!("REDDIT_USER_AGENT is not a script UA (`<platform>:<appID>:<version> by /u/<username>`); Reddit may throttle or reject OAuth traffic");
    }

    let api = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/subs", get(subs))
        .route("/api/videos", get(videos_handler))
        .route("/api/refresh", post(refresh_handler))
        .layer(
            TraceLayer::new_for_http().make_span_with(
                |request: &axum::http::Request<_>| {
                    let matched = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str)
                        .unwrap_or_else(|| request.uri().path());
                    tracing::info_span!(
                        "request",
                        method = %request.method(),
                        route = %matched,
                    )
                },
            ),
        );

    // Static files with index fallback for SPA-ish root.
    let serve_dir = ServeDir::new("static")
        .append_index_html_on_directories(true)
        .not_found_service(ServeFile::new("static/index.html"));

    let app = api.fallback_service(serve_dir).with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");
    info!("reddittv listening on http://{}", addr);
    axum::serve(listener, app).await.expect("server error");
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    fn vid(yt: &str, reddit: &str) -> VideoItem {
        VideoItem {
            youtube_id: yt.to_string(),
            youtube_url: format!("https://www.youtube.com/watch?v={}", yt),
            title: format!("title {}", yt),
            reddit_url: reddit.to_string(),
            thumbnail: PLACEHOLDER_THUMB.to_string(),
            created_utc: None,
            reddit_id: reddit_id_from_permalink(reddit),
        }
    }

    #[test]
    fn tail_merge_appends_only_unseen_older() {
        let existing = vec![vid("aaa111aaa11", "https://www.reddit.com/r/v/comments/abc1/x/"), vid("bbb222bbb22", "https://www.reddit.com/r/v/comments/abc2/x/")];
        // Overlap on bbb + one truly older item.
        let next_page = vec![
            vid("bbb222bbb22", "https://www.reddit.com/r/v/comments/abc2/x/"),
            vid("ccc333ccc33", "https://www.reddit.com/r/v/comments/abc3/x/"),
        ];
        let merged = merge_older_tail(existing, next_page);
        let ids: Vec<&str> = merged.iter().map(|v| v.youtube_id.as_str()).collect();
        assert_eq!(ids, vec!["aaa111aaa11", "bbb222bbb22", "ccc333ccc33"]);
    }

    #[test]
    fn tail_merge_dedupes_by_reddit_id_even_with_new_youtube_id() {
        let existing = vec![vid("aaa111aaa11", "https://www.reddit.com/r/v/comments/abc1/x/")];
        // Same reddit post, different youtube id casing/format: must not duplicate.
        let dupe = VideoItem {
            youtube_id: "zzz999zzz99".to_string(),
            ..vid("zzz999zzz99", "https://www.reddit.com/r/v/comments/abc1/x/")
        };
        let merged = merge_older_tail(existing, vec![dupe]);
        assert_eq!(merged.len(), 1, "same reddit post must not append");
    }

    #[test]
    fn head_merge_prepends_unseen_newer_first() {
        let existing = vec![vid("bbb222bbb22", "https://www.reddit.com/r/v/comments/abc2/x/")];
        let fresh = vec![
            vid("aaa111aaa11", "https://www.reddit.com/r/v/comments/abc1/x/"),
            vid("bbb222bbb22", "https://www.reddit.com/r/v/comments/abc2/x/"),
        ];
        let merged = merge_newest_head(existing, fresh);
        let ids: Vec<&str> = merged.iter().map(|v| v.youtube_id.as_str()).collect();
        assert_eq!(ids, vec!["aaa111aaa11", "bbb222bbb22"]);
    }

    #[test]
    fn merge_caps_at_max_posts() {
        let mut existing = Vec::new();
        for i in 0..MAX_POSTS {
            existing.push(vid(&format!("id{:09}", i), &format!("https://www.reddit.com/r/v/comments/c{:06}/x/", i)));
        }
        let extra = vec![vid("extra00001", "https://www.reddit.com/r/v/comments/zzzzzz/x/")];
        let merged = merge_older_tail(existing, extra);
        assert_eq!(merged.len(), MAX_POSTS);
    }

    #[test]
    fn after_cursor_parses_next_href() {
        assert_eq!(
            after_from_next_href(Some("https://old.reddit.com/r/videos/new/?count=25&after=t3_abc123")),
            Some("t3_abc123".to_string())
        );
        assert_eq!(
            after_from_next_href(Some("/r/videos/new/?count=50&after=t3_xyz&foo=1")),
            Some("t3_xyz".to_string())
        );
        assert_eq!(after_from_next_href(None), None);
        assert_eq!(after_from_next_href(Some("https://old.reddit.com/r/videos/new/")), None);
    }

    #[test]
    fn reddit_id_from_comments_url() {
        assert_eq!(
            reddit_id_from_permalink("https://www.reddit.com/r/videos/comments/1abc23/some_title/"),
            Some("t3_1abc23".to_string())
        );
        assert_eq!(
            reddit_id_from_permalink("https://old.reddit.com/r/videos/comments/xyz9/slug/?count=25&after=t3_xyz9"),
            Some("t3_xyz9".to_string())
        );
        assert_eq!(reddit_id_from_permalink("https://www.youtube.com/watch?v=abc"), None);
    }

    #[test]
    fn index_cursor_parses_opaque_after() {
        assert_eq!(parse_after(&None), 0);
        assert_eq!(parse_after(&Some("".to_string())), 0);
        assert_eq!(parse_after(&Some("25".to_string())), 25);
        // Legacy t3_* cursors restart at 0.
        assert_eq!(parse_after(&Some("t3_abc123".to_string())), 0);
    }

    fn tl_with(n: usize, loading: bool, reddit_after: Option<&str>) -> SubTimeline {
        SubTimeline {
            posts: (0..n)
                .map(|i| VideoItem {
                    youtube_id: format!("id{:011}", i),
                    youtube_url: format!("https://www.youtube.com/watch?v=id{:011}", i),
                    title: format!("title {}", i),
                    reddit_url: format!("https://www.reddit.com/r/v/comments/c{}/x/", i),
                    thumbnail: PLACEHOLDER_THUMB.to_string(),
                    created_utc: None,
                    reddit_id: Some(format!("t3_c{}", i)),
                })
                .collect(),
            fetched_at_ms: Some(now_ms()),
            last_error: None,
            blocked_until_ms: None,
            rate_limited_until_ms: None,
            reddit_after: reddit_after.map(|s| s.to_string()),
            loading,
        }
    }

    #[test]
    fn slice_body_echoes_tail_cursor_while_more_pending() {
        // At the tail with a background job pending, the client must get a
        // retryable cursor (not null) so it doesn't lose its position.
        let tl = tl_with(10, true, None);
        let body = slice_body("videos", &tl, 10, 25, true, None);
        assert_eq!(body["after"].as_str(), Some("10"));
        assert_eq!(body["hasMore"].as_bool(), Some(true));
        assert_eq!(body["loading"].as_bool(), Some(true));
    }

    #[test]
    fn slice_body_tail_cursor_null_when_truly_exhausted() {
        let tl = tl_with(10, false, None);
        let body = slice_body("videos", &tl, 10, 25, true, None);
        assert!(body["after"].is_null(), "exhausted tail: {:?}", body["after"]);
        assert_eq!(body["hasMore"].as_bool(), Some(false));
    }

    #[test]
    fn slice_body_mid_page_cursor_advances() {
        let tl = tl_with(10, false, None);
        let body = slice_body("videos", &tl, 0, 5, true, None);
        assert_eq!(body["after"].as_str(), Some("5"));
        assert_eq!(body["videos"].as_array().unwrap().len(), 5);
    }

    #[test]
    fn slice_body_empty_loading_echoes_zero_cursor() {
        // Never-fetched first paint: empty + loading must echo "0" (not
        // null) so the client retries the same tail position instead of
        // treating it as terminal.
        let tl = tl_with(0, true, None);
        let body = slice_body("videos", &tl, 0, 25, true, None);
        assert_eq!(body["after"].as_str(), Some("0"));
        assert_eq!(body["hasMore"].as_bool(), Some(true));
        assert_eq!(body["loading"].as_bool(), Some(true));
        assert!(body.get("retryAfterMs").is_none());
    }

    #[test]
    fn priority_gate_bypasses_hourly_cap() {
        // Fill the sliding window: normal reserve must fail (hourly cap)
        // while the priority lane still succeeds (only the short guard).
        let mut gate = NavGate::default();
        let now = Instant::now();
        gate.last_nav = Some(now - Duration::from_secs(10));
        for _ in 0..MAX_NAVS_PER_HOUR {
            gate.nav_times.push_back(now);
        }
        assert!(nav_gate_reserve(&mut gate).is_err());
        let mut gate2 = NavGate::default();
        gate2.last_nav = Some(now - Duration::from_secs(10));
        for _ in 0..MAX_NAVS_PER_HOUR {
            gate2.nav_times.push_back(now);
        }
        assert!(nav_gate_reserve_priority(&mut gate2).is_ok());
        // Short overlap guard still applies: nav 1s ago is rejected.
        let mut gate3 = NavGate::default();
        gate3.last_nav = Some(Instant::now() - Duration::from_secs(1));
        assert!(nav_gate_reserve_priority(&mut gate3).is_err());
        assert!(nav_gate_peek_priority(&mut gate3).is_err());
    }

    #[test]
    fn stale_equal_length_head_merge_still_prepends_unseen() {
        let existing: Vec<VideoItem> = (0..25)
            .map(|i| {
                vid(
                    &format!("id{:09}", i),
                    &format!("https://www.reddit.com/r/v/comments/c{:06}/x/", i),
                )
            })
            .collect();
        let mut fresh: Vec<VideoItem> = (100..105)
            .map(|i| {
                vid(
                    &format!("id{:09}", i),
                    &format!("https://www.reddit.com/r/v/comments/c{:06}/x/", i),
                )
            })
            .collect();
        fresh.extend(existing[..20].to_vec());
        assert_eq!(fresh.len(), existing.len());
        let merged = merge_newest_head(existing, fresh);
        assert_eq!(merged.len(), 30);
        for (i, n) in (100..105).enumerate() {
            assert_eq!(merged[i].youtube_id, format!("id{:09}", n));
        }
        assert_eq!(merged[5].youtube_id, "id000000000");
    }

    #[tokio::test]
    async fn rss_only_fill_keeps_known_cursor() {
        let tl = Arc::new(RwLock::new(SubTimeline::default()));
        {
            let mut w = tl.write().await;
            w.posts = vec![vid(
                "aaa111aaa11",
                "https://www.reddit.com/r/v/comments/abc1/x/",
            )];
            w.reddit_after = Some("t3_abc1".to_string());
        }
        store_head_fill(
            &tl,
            vec![vid(
                "bbb222bbb22",
                "https://www.reddit.com/r/v/comments/abc2/x/",
            )],
            None,
        )
        .await;
        assert_eq!(
            tl.read().await.reddit_after.as_deref(),
            Some("t3_abc1"),
            "RSS-only store must preserve the known cursor"
        );
        apply_head_merge(
            &tl,
            vec![vid(
                "ccc333ccc33",
                "https://www.reddit.com/r/v/comments/abc3/x/",
            )],
            None,
        )
        .await;
        assert_eq!(
            tl.read().await.reddit_after.as_deref(),
            Some("t3_abc1"),
            "cursor-less head merge must preserve the known cursor"
        );
        apply_head_merge(
            &tl,
            vec![vid(
                "ddd444ddd44",
                "https://www.reddit.com/r/v/comments/abc4/x/",
            )],
            Some("t3_abc4".to_string()),
        )
        .await;
        assert_eq!(
            tl.read().await.reddit_after.as_deref(),
            Some("t3_abc4")
        );
    }

    #[test]
    fn upstream_429_cooldown_defaults_to_5min_and_honors_retry_after() {
        assert_eq!(
            upstream_429_cooldown_ms(None),
            UPSTREAM_429_COOLDOWN_MS
        );
        // Upstream Retry-After (seconds->ms) is honored, clamped 5s..30min.
        assert_eq!(upstream_429_cooldown_ms(Some(120_000)), 120_000);
        assert_eq!(
            upstream_429_cooldown_ms(Some(1_000)),
            MIN_429_COOLDOWN_MS
        );
        assert_eq!(
            upstream_429_cooldown_ms(Some(3_600_000)),
            MAX_429_COOLDOWN_MS
        );
    }

    #[tokio::test]
    async fn upstream_429_sets_cooldown_and_second_get_sees_retry_after() {
        let tl = Arc::new(RwLock::new(SubTimeline::default()));
        // First failure: UPSTREAM_429 with no Retry-After -> 5min cooldown.
        finish_with_error(
            &tl,
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            None,
        )
        .await;
        {
            let r = tl.read().await;
            assert_eq!(r.last_error.as_deref(), Some("UPSTREAM_429"));
            let remain = rate_limit_remaining_ms(&r, now_ms());
            assert!(
                remain.is_some(),
                "429 must set negative-cache cooldown"
            );
            let ms = remain.unwrap();
            assert!(
                ms > UPSTREAM_429_COOLDOWN_MS as u64 - 10_000
                    && ms <= UPSTREAM_429_COOLDOWN_MS as u64,
                "default cooldown ~5min, got {ms}"
            );
        }
        // Cooldown response shape: empty + loading + retryAfterMs, no job.
        // (videos_handler early-return synthesizes this via slice_body.)
        {
            let r = tl.read().await;
            let remain = rate_limit_remaining_ms(&r, now_ms()).unwrap();
            let body = slice_body("educationalvideos", &r, 0, 25, true, Some(remain));
            assert_eq!(body["videos"].as_array().unwrap().len(), 0);
            // finish_with_error leaves loading=false; the handler forces
            // loading:true on the cooldown path — emulate + assert intent.
            assert!(remain > 0, "retryAfterMs must be positive, got {body}");
        }
        // Success clears the cooldown.
        store_head_fill(
            &tl,
            vec![vid(
                "aaa111aaa11",
                "https://www.reddit.com/r/v/comments/abc1/x/",
            )],
            None,
        )
        .await;
        assert!(
            rate_limit_remaining_ms(&*tl.read().await, now_ms()).is_none(),
            "successful fill must clear the 429 cooldown"
        );
    }

    #[tokio::test]
    async fn upstream_429_honors_retry_after_header_value() {
        let tl = Arc::new(RwLock::new(SubTimeline::default()));
        finish_with_error(
            &tl,
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            Some(60_000),
        )
        .await;
        let remain = rate_limit_remaining_ms(&*tl.read().await, now_ms()).unwrap();
        assert!(
            remain > 50_000 && remain <= 60_000,
            "Retry-After 60s must drive cooldown, got {remain}"
        );
    }

    // ---------- OAuth (offline-safe: no live Reddit calls) ----------

    #[test]
    fn token_ttl_subtracts_60s_skew_with_floor() {
        // 3600s grant caches for 3540s.
        assert_eq!(token_ttl_secs(3600), 3540);
        // Tiny expires_in never hot-loops: floored at the minimum TTL.
        assert_eq!(token_ttl_secs(30), OAUTH_MIN_CACHED_TTL_SECS);
        assert_eq!(token_ttl_secs(0), OAUTH_MIN_CACHED_TTL_SECS);
        assert_eq!(token_ttl_secs(61), OAUTH_MIN_CACHED_TTL_SECS.max(1));
    }

    #[test]
    fn token_cache_validity_is_time_bound() {
        let now = now_ms();
        assert!(token_cache_valid(now + 60_000, now));
        assert!(!token_cache_valid(now - 1, now));
        assert!(!token_cache_valid(now, now));
    }

    #[test]
    fn cached_token_valid_until_expiry() {
        let fresh = CachedToken {
            token: "t".to_string(),
            expires_at: Instant::now() + Duration::from_secs(60),
        };
        assert!(fresh.valid());
        let stale = CachedToken {
            token: "t".to_string(),
            expires_at: Instant::now() - Duration::from_secs(1),
        };
        assert!(!stale.valid());
    }

    #[tokio::test]
    async fn oauth_refresh_lock_is_single_flight() {
        // The refresh mutex must serialize concurrent refresh attempts:
        // a second try_lock while the first holds it fails fast (coalesce),
        // and succeeds once released. No network involved.
        let oauth = OAuthState::default();
        let guard = oauth.refresh.lock().await;
        assert!(
            oauth.refresh.try_lock().is_err(),
            "concurrent refresh must coalesce (single-flight)"
        );
        drop(guard);
        assert!(
            oauth.refresh.try_lock().is_ok(),
            "refresh lock must release after first refresh"
        );
    }

    #[tokio::test]
    async fn oauth_cached_token_short_circuits_refresh() {
        // A valid cached token is returned without touching the network
        // (no creds in env needed): proves the fast path is network-free.
        let oauth = OAuthState::default();
        *oauth.cached.lock().await = Some(CachedToken {
            token: "cached-bearer".to_string(),
            expires_at: Instant::now() + Duration::from_secs(300),
        });
        let client = reqwest::Client::builder().build().unwrap();
        let got = oauth_bearer_token(&oauth, &client, "test-ua").await;
        assert_eq!(got.as_deref(), Some("cached-bearer"));
    }

    #[test]
    fn oauth_listings_url_builder_shape() {
        assert_eq!(
            oauth_listings_url("videos", "new", 100, None),
            "https://oauth.reddit.com/r/videos/new.json?raw_json=1&limit=100"
        );
        assert_eq!(
            oauth_listings_url("videos", "hot", 25, Some("t3_abc123")),
            "https://oauth.reddit.com/r/videos/hot.json?raw_json=1&limit=25&after=t3_abc123"
        );
        // Empty cursor is omitted, never rendered as `after=`.
        assert!(
            !oauth_listings_url("videos", "new", 100, Some("")).contains("after=")
        );
    }

    #[test]
    fn public_json_url_builder_hosts() {
        assert_eq!(
            public_json_url("www.reddit.com", "videos", 100, None),
            "https://www.reddit.com/r/videos/new.json?limit=100&raw_json=1"
        );
        assert_eq!(
            public_json_url("old.reddit.com", "videos", 100, Some("t3_x")),
            "https://old.reddit.com/r/videos/new.json?limit=100&raw_json=1&after=t3_x"
        );
    }

    #[test]
    fn script_ua_format_is_reddit_compliant() {
        // Script UAs look like `<platform>:<appID>:<version> by /u/<username>`.
        let ua = script_default_ua(Some("someuser"));
        assert!(is_script_ua(&ua), "script UA must contain ' by /u/': {ua}");
        assert!(ua.contains("someuser"), "script UA must name the user: {ua}");
        assert!(is_script_ua("linux:redditv:0.1.0 by /u/someuser"));
        // Legacy parenthesized form is not a script UA.
        assert!(!is_script_ua(DEFAULT_UA));
    }

    #[test]
    fn oauth_creds_require_all_four_fields() {
        let full = || {
            oauth_creds_from(
                Some("id".to_string()),
                Some("secret".to_string()),
                Some("user".to_string()),
                Some("pass".to_string()),
            )
        };
        assert!(full().is_some());
        assert!(
            oauth_creds_from(None, Some("s".into()), Some("u".into()), Some("p".into())).is_none(),
            "missing client_id must yield no creds (creds-absent fallback)"
        );
        assert!(
            oauth_creds_from(Some("id".into()), None, Some("u".into()), Some("p".into())).is_none()
        );
        assert!(
            oauth_creds_from(Some("id".into()), Some("s".into()), None, Some("p".into())).is_none()
        );
        assert!(
            oauth_creds_from(Some("id".into()), Some("s".into()), Some("u".into()), None).is_none()
        );
        assert!(
            oauth_creds_from(Some("".into()), Some("s".into()), Some("u".into()), Some("p".into())).is_none(),
            "empty fields must yield no creds"
        );
    }

    #[test]
    fn auth_mode_script_is_opt_in_default_no_auth() {
        // No registration needed by default: unset/other values are no-auth
        // even when creds happen to be present. OAuth runs only with
        // REDDIT_AUTH_MODE=script AND complete creds.
        assert_eq!(auth_mode_from(None, true), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(None, false), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(Some("none"), true), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(Some("NONE"), true), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(Some("SCRIPT"), true), AuthMode::Script);
        assert_eq!(auth_mode_from(Some("script"), true), AuthMode::Script);
        assert_eq!(auth_mode_from(Some("script"), false), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(Some("none"), false), AuthMode::NoAuth);
        assert_eq!(auth_mode_from(Some(""), true), AuthMode::NoAuth);
    }

    // ---------- No-auth mirrors (offline-safe: URL builders + field maps) ----------

    #[test]
    fn arctic_search_url_builder_shape() {
        assert_eq!(
            arctic_search_url("videos", 100, None),
            "https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=videos&sort=desc&limit=100&fields=title,url,id,subreddit,created_utc,author,score,num_comments"
        );
        assert_eq!(
            arctic_search_url("videos", 25, Some("1700000000")),
            "https://arctic-shift.photon-reddit.com/api/posts/search?subreddit=videos&sort=desc&limit=25&fields=title,url,id,subreddit,created_utc,author,score,num_comments&after=1700000000"
        );
        // Empty cursor omitted, never rendered as `after=`.
        assert!(!arctic_search_url("videos", 100, Some("")).contains("after="));
        assert!(!arctic_search_url("videos", 100, None).contains("www.reddit.com"));
        assert!(!arctic_search_url("videos", 100, None).contains("oauth.reddit.com"));
    }

    #[test]
    fn arctic_maps_id_to_synthesized_permalink_and_yt_thumb() {
        let resp: ArcticResponse = serde_json::from_value(serde_json::json!({
            "data": [
                {"id": "abc123", "title": "Cool vid", "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "subreddit": "videos", "created_utc": 1700000000.0, "author": "u1"},
                {"id": "t3_def456", "title": "Short", "url": "https://youtu.be/abcdefghijk", "subreddit": "videos", "created_utc": 1700000100.0, "author": "u2"},
                {"id": "nope1", "title": "Not youtube", "url": "https://example.com/x", "subreddit": "videos", "created_utc": 1700000200.0, "author": "u3"},
                {"id": "", "title": "No id", "url": "https://www.youtube.com/watch?v=zzzzzzzzzzz", "subreddit": "videos", "created_utc": 1700000300.0, "author": "u4"}
            ]
        }))
        .unwrap();
        let (videos, cursor) = videos_from_arctic(&resp, "videos");
        // Non-YouTube + id-less rows filtered.
        assert_eq!(videos.len(), 2);
        // Newest-first by created_utc.
        assert_eq!(videos[0].youtube_id, "abcdefghijk");
        assert_eq!(
            videos[0].reddit_url,
            "https://www.reddit.com/r/videos/comments/def456/"
        );
        assert_eq!(videos[0].reddit_id.as_deref(), Some("t3_def456"));
        assert_eq!(
            videos[0].thumbnail,
            "https://i.ytimg.com/vi/abcdefghijk/hqdefault.jpg"
        );
        assert_eq!(videos[1].reddit_id.as_deref(), Some("t3_abc123"));
        assert_eq!(
            videos[1].reddit_url,
            "https://www.reddit.com/r/videos/comments/abc123/"
        );
        // Epoch cursor = oldest created_utc seen.
        assert_eq!(cursor.as_deref(), Some("1700000000"));
    }

    #[test]
    fn pullpush_search_url_builder_shape() {
        assert_eq!(
            pullpush_search_url("videos", 100, None),
            "https://api.pullpush.io/reddit/search/submission/?subreddit=videos&sort=desc&sort_type=created_utc&size=100"
        );
        assert_eq!(
            pullpush_search_url("videos", 50, Some("1700000000")),
            "https://api.pullpush.io/reddit/search/submission/?subreddit=videos&sort=desc&sort_type=created_utc&size=50&before=1700000000"
        );
        assert!(!pullpush_search_url("videos", 100, Some("")).contains("before="));
        assert!(!pullpush_search_url("videos", 100, None).contains("reddit.com"));
    }

    #[test]
    fn pullpush_direct_field_map() {
        let resp: PullPushResponse = serde_json::from_value(serde_json::json!({
            "data": [
                {"id": "ghi789", "title": "PP vid", "url": "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
                 "permalink": "/r/videos/comments/ghi789/slug/", "thumbnail": "https://cdn.example.com/t.jpg", "created_utc": 1700000500.0},
                {"id": "jkl012", "title": "", "url": "https://youtu.be/AAAAAAAAAAA",
                 "permalink": "https://www.reddit.com/r/videos/comments/jkl012/slug/", "thumbnail": "self", "created_utc": 1700000400.0},
                {"id": "x", "title": "No yt", "url": "https://example.com/", "permalink": "/r/videos/comments/x/", "thumbnail": "", "created_utc": 1700000600.0}
            ]
        }))
        .unwrap();
        let (videos, cursor) = videos_from_pullpush(&resp);
        assert_eq!(videos.len(), 2, "non-YouTube rows filtered");
        assert_eq!(videos[0].youtube_id, "dQw4w9WgXcQ");
        // Direct permalink + thumbnail passthrough.
        assert_eq!(
            videos[0].reddit_url,
            "https://www.reddit.com/r/videos/comments/ghi789/slug/"
        );
        assert_eq!(videos[0].thumbnail, "https://cdn.example.com/t.jpg");
        // Missing/placeholder thumbnail falls back to YouTube hqdefault.
        assert_eq!(
            videos[1].thumbnail,
            "https://i.ytimg.com/vi/AAAAAAAAAAA/hqdefault.jpg"
        );
        assert_eq!(videos[1].title, "Untitled");
        assert_eq!(cursor.as_deref(), Some("1700000400"));
    }

    #[test]
    fn redlib_url_builder_and_rotation_order() {
        assert_eq!(
            redlib_url("safereddit.com", "videos", 100, None),
            "https://safereddit.com/r/videos/new.json?limit=100&raw_json=1"
        );
        assert_eq!(
            redlib_url("redlib.cow.rip", "videos", 25, Some("t3_abc")),
            "https://redlib.cow.rip/r/videos/new.json?limit=25&raw_json=1&after=t3_abc"
        );
        // Round-robin: start index rotates, order preserved, all hosts tried.
        assert_eq!(
            redlib_order(0),
            vec!["safereddit.com", "redlib.catsarch.com", "redlib.r4fo.com", "redlib.cow.rip"]
        );
        assert_eq!(
            redlib_order(1),
            vec!["redlib.catsarch.com", "redlib.r4fo.com", "redlib.cow.rip", "safereddit.com"]
        );
        assert_eq!(redlib_order(4), redlib_order(0), "wraps around");
        assert_eq!(redlib_cooldown_key("safereddit.com"), "redlib:safereddit.com");
    }

    #[test]
    fn cursor_split_prefers_epoch_for_mirrors_t3_for_redlib() {
        assert_eq!(split_after_cursor(None), (None, None));
        assert_eq!(split_after_cursor(Some("")), (None, None));
        assert_eq!(
            split_after_cursor(Some("1700000000")),
            (Some("1700000000".to_string()), None)
        );
        assert_eq!(
            split_after_cursor(Some("t3_abc123")),
            (None, Some("t3_abc123".to_string()))
        );
        // Best cursor: t3 wins when present, else oldest epoch.
        let posts = vec![
            VideoItem {
                youtube_id: "a".into(),
                youtube_url: "u".into(),
                title: "t".into(),
                reddit_url: "r".into(),
                thumbnail: "th".into(),
                created_utc: Some(1700000200),
                reddit_id: None,
            },
            VideoItem {
                youtube_id: "b".into(),
                youtube_url: "u".into(),
                title: "t".into(),
                reddit_url: "r".into(),
                thumbnail: "th".into(),
                created_utc: Some(1700000100),
                reddit_id: None,
            },
        ];
        assert_eq!(
            best_cursor(Some("t3_x".to_string()), &posts).as_deref(),
            Some("t3_x")
        );
        assert_eq!(
            best_cursor(None, &posts).as_deref(),
            Some("1700000100")
        );
        assert_eq!(best_cursor(None, &[]), None);
    }

    #[test]
    fn mirror_pages_merge_newest_first_deduped() {
        let a = vec![VideoItem {
            youtube_id: "aaa111aaa11".into(),
            youtube_url: "https://www.youtube.com/watch?v=aaa111aaa11".into(),
            title: "old".into(),
            reddit_url: "https://www.reddit.com/r/v/comments/1/".into(),
            thumbnail: youtube_thumb("aaa111aaa11"),
            created_utc: Some(100),
            reddit_id: Some("t3_1".into()),
        }];
        let b = vec![
            VideoItem {
                youtube_id: "bbb222bbb22".into(),
                youtube_url: "https://www.youtube.com/watch?v=bbb222bbb22".into(),
                title: "new".into(),
                reddit_url: "https://www.reddit.com/r/v/comments/2/".into(),
                thumbnail: youtube_thumb("bbb222bbb22"),
                created_utc: Some(200),
                reddit_id: Some("t3_2".into()),
            },
            VideoItem {
                youtube_id: "aaa111aaa11".into(),
                youtube_url: "https://www.youtube.com/watch?v=aaa111aaa11".into(),
                title: "old dupe".into(),
                reddit_url: "https://www.reddit.com/r/v/comments/1/".into(),
                thumbnail: youtube_thumb("aaa111aaa11"),
                created_utc: Some(100),
                reddit_id: Some("t3_1".into()),
            },
        ];
        let merged = merge_mirror_pages(vec![a, b]);
        let ids: Vec<&str> = merged.iter().map(|v| v.youtube_id.as_str()).collect();
        assert_eq!(ids, vec!["bbb222bbb22", "aaa111aaa11"]);
    }

    #[test]
    fn mirror_cooldowns_are_per_host() {
        let mirrors = MirrorState::default();
        mirror_set_cooldown(&mirrors, "redlib:safereddit.com", Some(60_000));
        // Same host cools down…
        assert!(mirror_cooldown_remaining(&mirrors, "redlib:safereddit.com", now_ms()).is_some());
        // …other hosts and mirrors are unaffected.
        assert!(mirror_cooldown_remaining(&mirrors, "redlib:redlib.cow.rip", now_ms()).is_none());
        assert!(mirror_cooldown_remaining(&mirrors, "arctic", now_ms()).is_none());
        assert!(mirror_cooldown_remaining(&mirrors, "pullpush", now_ms()).is_none());
    }

    #[test]
    fn ratelimit_reset_header_parses_to_ms() {
        use axum::http::{HeaderMap, HeaderValue};
        let mut h = HeaderMap::new();
        h.insert("x-ratelimit-reset", HeaderValue::from_static("12.5"));
        assert_eq!(ratelimit_reset_ms(&h), Some(12_500));
        let empty = HeaderMap::new();
        assert_eq!(ratelimit_reset_ms(&empty), None);
    }
}
