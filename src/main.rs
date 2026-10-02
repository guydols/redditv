use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chromiumoxide::{Browser, BrowserConfig};
use dashmap::DashMap;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
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

// ---------- state ----------

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    timelines: Arc<DashMap<String, Arc<RwLock<SubTimeline>>>>,
    inflight: Arc<DashMap<String, Arc<Mutex<()>>>>,
    nav_gate: Arc<Mutex<NavGate>>,
}

#[derive(Default)]
struct SubTimeline {
    posts: Vec<VideoItem>,
    /// Epoch ms of last successful fetch; None = never fetched.
    fetched_at_ms: Option<i64>,
    last_error: Option<String>,
    /// Epoch ms until which this sub is backed off (after BLOCKED).
    blocked_until_ms: Option<i64>,
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

fn reddit_ua() -> String {
    std::env::var("REDDIT_UA").unwrap_or_else(|_| DEFAULT_UA.to_string())
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

async fn fetch_public_json(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let mut url = format!(
        "https://www.reddit.com/r/{}/new.json?limit={}&raw_json=1",
        sub, limit
    );
    if let Some(a) = after {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/json")
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("upstream request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        return Err((
            StatusCode::BAD_GATEWAY,
            "Reddit denied the request (403)".to_string(),
            None,
        ));
    }
    if status.is_server_error() || status == StatusCode::UNAUTHORIZED {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream returned {}", status.as_u16()),
            None,
        ));
    }
    if !status.is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream returned {}", status.as_u16()),
            None,
        ));
    }
    let listing: RedditListing = resp.json().await.map_err(|e| {
        (
            StatusCode::BAD_GATEWAY,
            format!("failed to parse Reddit response: {}", e),
            None,
        )
    })?;
    Ok(videos_from_listing(&listing))
}

struct OauthCreds {
    client_id: String,
    secret: String,
    username: String,
    password: String,
}

fn oauth_creds() -> Option<OauthCreds> {
    let client_id = std::env::var("REDDIT_CLIENT_ID").ok()?;
    let secret = std::env::var("REDDIT_CLIENT_SECRET")
        .or_else(|_| std::env::var("REDDIT_SECRET"))
        .ok()?;
    let username = std::env::var("REDDIT_USERNAME")
        .or_else(|_| std::env::var("REDDIT_USER"))
        .or_else(|_| std::env::var("REDDIT_USERNAME_OVERRIDE"))
        .ok()?;
    // NOTE: REDDIT_USER doubles as the script username when REDDIT_USERNAME is unset.
    let password = std::env::var("REDDIT_PASSWORD").ok()?;
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

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: Option<String>,
    token_type: Option<String>,
    error: Option<String>,
}

async fn fetch_oauth_json(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
    limit: u32,
    after: Option<&str>,
) -> Option<Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)>> {
    let creds = oauth_creds()?;
    // password grant
    let token_resp = client
        .post("https://www.reddit.com/api/v1/access_token")
        .header(header::USER_AGENT, ua)
        .basic_auth(&creds.client_id, Some(&creds.secret))
        .form(&[
            ("grant_type", "password"),
            ("username", creds.username.as_str()),
            ("password", creds.password.as_str()),
        ])
        .send()
        .await
        .ok()?;
    if !token_resp.status().is_success() {
        return None;
    }
    let token: TokenResponse = token_resp.json().await.ok()?;
    let access = token.access_token?;
    let _ = token.token_type;
    if token.error.is_some() && access.is_empty() {
        return None;
    }
    let mut url = format!(
        "https://oauth.reddit.com/r/{}/new?limit={}&raw_json=1",
        sub, limit
    );
    if let Some(a) = after {
        if !a.is_empty() {
            url.push_str(&format!("&after={}", a));
        }
    }
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/json")
        .bearer_auth(access)
        .send()
        .await
        .ok()?;
    if resp.status() == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        return Some(Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit (oauth) rate-limited the request".to_string(),
            ms,
        )));
    }
    if !resp.status().is_success() {
        return None; // let caller fall through to RSS
    }
    let listing: RedditListing = resp.json().await.ok()?;
    Some(Ok(videos_from_listing(&listing)))
}

async fn fetch_rss_fallback(
    client: &reqwest::Client,
    ua: &str,
    sub: &str,
) -> Result<Vec<VideoItem>, (StatusCode, String, Option<u64>)> {
    let url = format!("https://www.reddit.com/r/{}/new.rss", sub);
    let resp = client
        .get(&url)
        .header(header::USER_AGENT, ua)
        .header(header::ACCEPT, "application/rss+xml, application/xml, text/xml")
        .send()
        .await
        .map_err(|e| {
            (
                StatusCode::BAD_GATEWAY,
                format!("RSS fallback request failed: {}", e),
                None,
            )
        })?;
    let status = resp.status();
    if status == StatusCode::TOO_MANY_REQUESTS {
        let ms = retry_after_ms(resp.headers());
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            ms,
        ));
    }
    if status == StatusCode::FORBIDDEN {
        return Err((
            StatusCode::BAD_GATEWAY,
            "Reddit denied the request (403)".to_string(),
            None,
        ));
    }
    if !status.is_success() {
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
    Ok(out)
}

/// Reqwest JSON -> OAuth -> RSS fallback chain. Returns timeline posts
/// (newest-first) plus the reddit `after` cursor when known.
async fn fetch_http_fallback(
    client: &reqwest::Client,
    sub: &str,
    after: Option<&str>,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let ua = reddit_ua();
    // Timeline fill always grabs the newest page; pagination is served
    // from memory via the index cursor.
    const FILL_LIMIT: u32 = 100;
    match fetch_public_json(client, &ua, sub, FILL_LIMIT, after).await {
        Ok((videos, after)) => return Ok((dedupe_cap(videos), after)),
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                return Err((code, msg, retry_ms));
            }
            if code != StatusCode::BAD_GATEWAY {
                return Err((code, msg, retry_ms));
            }
            if let Some(oauth_result) =
                fetch_oauth_json(client, &ua, sub, FILL_LIMIT, after).await
            {
                match oauth_result {
                    Ok((videos, after)) => return Ok((dedupe_cap(videos), after)),
                    Err((c2, m2, r2)) => {
                        if c2 == StatusCode::TOO_MANY_REQUESTS {
                            return Err((c2, m2, r2));
                        }
                    }
                }
            }
            // RSS has no `after` cursor; only usable for the head fill.
            if after.is_some() {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!("Reddit upstream failed for r/{}: {}", sub, msg),
                    retry_ms,
                ));
            }
            match fetch_rss_fallback(client, &ua, sub).await {
                Ok(all) => Ok((dedupe_cap(all), None)),
                Err((c3, m3, r3)) => {
                    if c3 == StatusCode::TOO_MANY_REQUESTS {
                        return Err((c3, "Reddit rate-limited the request".to_string(), r3));
                    }
                    if msg.contains("403") || m3.contains("403") {
                        return Err((
                            StatusCode::BAD_GATEWAY,
                            format!("Reddit upstream denied the request. {}", m3),
                            r3,
                        ));
                    }
                    Err((
                        StatusCode::BAD_GATEWAY,
                        format!("Reddit upstream failed ({}; fallback: {})", msg, m3),
                        r3,
                    ))
                }
            }
        }
    }
}

/// Fast first-paint HTTP fill for never-fetched subs: public JSON and RSS run
/// concurrently, each capped at FAST_HTTP_LEG_TIMEOUT (~4s). OAuth is
/// deliberately skipped here — the token POST would block first paint on a
/// second sequential timeout chain; it stays deferred to background
/// enrichment (fetch_http_fallback) after first paint. Returns merged,
/// deduped rows (public order first, RSS unseen appended) with the public
/// `after` cursor when known. A 429 from either leg surfaces as 429 when no
/// rows are available; timeouts are treated as a missed leg, not fatal.
async fn fetch_http_first_paint(
    client: &reqwest::Client,
    sub: &str,
) -> Result<(Vec<VideoItem>, Option<String>), (StatusCode, String, Option<u64>)> {
    let ua = reddit_ua();
    const FILL_LIMIT: u32 = 100;
    let (pub_res, rss_res) = tokio::join!(
        tokio::time::timeout(
            FAST_HTTP_LEG_TIMEOUT,
            fetch_public_json(client, &ua, sub, FILL_LIMIT, None)
        ),
        tokio::time::timeout(FAST_HTTP_LEG_TIMEOUT, fetch_rss_fallback(client, &ua, sub)),
    );
    // Normalize: timeout -> None (leg missed its budget).
    enum Leg<T> {
        Hit(T),
        Miss429(Option<u64>),
        Miss(String),
        Timeout,
    }
    let pub_leg: Leg<(Vec<VideoItem>, Option<String>)> = match pub_res {
        Err(_) => Leg::Timeout,
        Ok(Ok(ok)) => Leg::Hit(ok),
        Ok(Err((code, msg, retry))) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                Leg::Miss429(retry)
            } else {
                Leg::Miss(msg)
            }
        }
    };
    let rss_leg: Leg<Vec<VideoItem>> = match rss_res {
        Err(_) => Leg::Timeout,
        Ok(Ok(ok)) => Leg::Hit(ok),
        Ok(Err((code, msg, retry))) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                Leg::Miss429(retry)
            } else {
                Leg::Miss(msg)
            }
        }
    };
    match (pub_leg, rss_leg) {
        (Leg::Hit((pvideos, pafter)), Leg::Hit(rvideos)) => {
            if pvideos.is_empty() && rvideos.is_empty() {
                Ok((Vec::new(), pafter))
            } else {
                let extra = filter_unseen_older(&pvideos, rvideos);
                let mut merged = Vec::with_capacity(pvideos.len() + extra.len());
                merged.extend(pvideos);
                merged.extend(extra);
                Ok((dedupe_cap(merged), pafter))
            }
        }
        (Leg::Hit((pvideos, pafter)), _) => Ok((dedupe_cap(pvideos), pafter)),
        (_, Leg::Hit(rvideos)) => Ok((dedupe_cap(rvideos), None)),
        (Leg::Miss429(r), Leg::Miss429(_)) => Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Reddit rate-limited the request".to_string(),
            r,
        )),
        (Leg::Miss429(r), _) | (_, Leg::Miss429(r)) => {
            // One leg rate-limited while the other missed/timed out with no
            // rows: surface the 429 so politeness backoff still applies.
            Err((
                StatusCode::TOO_MANY_REQUESTS,
                "Reddit rate-limited the request".to_string(),
                r,
            ))
        }
        (Leg::Miss(m1), Leg::Miss(m2)) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream failed ({}; fallback: {})", m1, m2),
            None,
        )),
        (Leg::Miss(m), Leg::Timeout) | (Leg::Timeout, Leg::Miss(m)) => Err((
            StatusCode::BAD_GATEWAY,
            format!("Reddit upstream failed: {}", m),
            None,
        )),
        (Leg::Timeout, Leg::Timeout) => Err((
            StatusCode::BAD_GATEWAY,
            "Reddit upstream timed out (fast first paint)".to_string(),
            None,
        )),
    }
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
        .map_err(|e| format!("browser launch failed: {}", e))?;
    // Drive the connection in the background.
    tokio::spawn(async move {
        while handler.next().await.is_some() {}
    });
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
    let nav_timeout = if fast { FAST_NAV_TIMEOUT } else { Duration::from_secs(45) };
    match tokio::time::timeout(nav_timeout, page.goto(url)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(format!("navigation failed: {}", e)),
        Err(_) => return Err("navigation timeout".to_string()),
    }
    // Wait for post content; a timeout here usually means a challenge/block.
    let sel_timeout = if fast { FAST_SELECTOR_TIMEOUT } else { Duration::from_secs(20) };
    match tokio::time::timeout(sel_timeout, page.find_element("a.title, shreddit-post")).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => return Err(format!("BLOCKED: selector not found: {}", e)),
        Err(_) => return Err("BLOCKED: selector timeout (possible challenge)".to_string()),
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
        return Err("CHROME_DISABLED".to_string());
    }
    let cell = browser_cell();
    let mut guard = if fast {
        match tokio::time::timeout(FAST_BROWSER_LOCK_TIMEOUT, cell.lock()).await {
            Ok(g) => g,
            Err(_) => {
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
            Err(e) => return Err(e),
        }
    }
    let browser = guard.as_ref().expect("browser just launched");
    let page = browser
        .new_page("about:blank")
        .await
        .map_err(|e| format!("new page failed: {}", e))?;
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
async fn store_head_fill(
    tl: &Arc<RwLock<SubTimeline>>,
    posts: Vec<VideoItem>,
    reddit_after: Option<String>,
) {
    let mut w = tl.write().await;
    w.posts = dedupe_cap(posts);
    w.reddit_after = reddit_after;
    w.fetched_at_ms = Some(now_ms());
    w.last_error = None;
    w.blocked_until_ms = None;
    w.loading = false;
}

/// Merge a refreshed newest page at the head (unseen newer first).
async fn apply_head_merge(
    tl: &Arc<RwLock<SubTimeline>>,
    fresh_page: Vec<VideoItem>,
    reddit_after: Option<String>,
) {
    let mut w = tl.write().await;
    let merged = merge_newest_head(std::mem::take(&mut w.posts), fresh_page);
    w.posts = merged;
    if reddit_after.is_some() {
        w.reddit_after = reddit_after;
    }
    w.fetched_at_ms = Some(now_ms());
    w.last_error = None;
    w.blocked_until_ms = None;
    w.loading = false;
}

/// Merge an older next-page at the tail: only unseen older items appended.
/// Keeps head freshness (`fetched_at_ms`) untouched so TTL still fires.
async fn apply_tail_merge(
    tl: &Arc<RwLock<SubTimeline>>,
    next_page: Vec<VideoItem>,
    next_after: Option<String>,
) {
    let mut w = tl.write().await;
    let merged = merge_older_tail(std::mem::take(&mut w.posts), next_page);
    w.posts = merged;
    w.reddit_after = next_after;
    w.last_error = None;
    w.loading = false;
}

async fn set_loading(tl: &Arc<RwLock<SubTimeline>>, loading: bool) {
    let mut w = tl.write().await;
    w.loading = loading;
}

async fn mark_blocked(tl: &Arc<RwLock<SubTimeline>>, reason: String) {
    let mut w = tl.write().await;
    w.last_error = Some("BLOCKED".to_string());
    w.blocked_until_ms = Some(now_ms() + BLOCKED_BACKOFF_MS);
    w.loading = false;
    let _ = reason;
}

async fn finish_with_error(tl: &Arc<RwLock<SubTimeline>>, code: StatusCode, msg: String) {
    let mut w = tl.write().await;
    if code == StatusCode::TOO_MANY_REQUESTS {
        w.last_error = Some("UPSTREAM_429".to_string());
    } else if msg.contains("403") || msg.contains("BLOCKED") {
        w.last_error = Some("BLOCKED".to_string());
        w.blocked_until_ms = Some(now_ms() + BLOCKED_BACKOFF_MS);
    } else {
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
/// HTTP legs (public JSON + RSS ~4s each via tokio::join, OAuth token POST
/// skipped/deferred to enrichment) so a never-fetched sub fills in ~4s
/// instead of stacking sequential 10s timeouts. Enrichment keeps the full
/// public -> OAuth -> RSS chain with longer budgets.
/// The first leg with rows>0 does the head fill; the loser head-merges
/// unseen rows if it arrives later. BROWSER_BUSY / gate exhaustion just
/// means the HTTP legs decide alone.
async fn background_head_refresh(state: AppState, sub: String) {
    let tl = timeline_arc(&state, &sub);
    {
        let r = tl.read().await;
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                set_loading(&tl, false).await;
                return;
            }
        }
    }
    let lock = inflight_lock(&state, &sub);
    let Ok(_guard) = lock.try_lock() else {
        return;
    };
    set_loading(&tl, true).await;
    let first_paint = tl.read().await.posts.is_empty();
    // Chrome-disabled (CI/tests): HTTP-only head fill, no race needed.
    // First paint uses the fast parallel legs (public+RSS ~4s each, OAuth
    // skipped); enrichment keeps the full sequential chain + OAuth.
    if chrome_disabled() {
        let res = if first_paint {
            fetch_http_first_paint(&state.client, &sub).await
        } else {
            fetch_http_fallback(&state.client, &sub, None).await
        };
        match res {
            Ok((posts, after)) => {
                if first_paint {
                    store_head_fill(&tl, posts, after).await;
                } else {
                    apply_head_merge(&tl, posts, after).await;
                }
            }
            Err((code, msg, _)) => finish_with_error(&tl, code, msg).await,
        }
        return;
    }
    let gate_ok = {
        let mut gate = state.nav_gate.lock().await;
        // Priority lane: a never-fetched sub bypasses the 45s+jitter +
        // hourly cap and only respects the short overlap guard so the
        // first paint fetches then and there.
        if first_paint {
            nav_gate_reserve_priority(&mut gate).is_ok()
        } else {
            nav_gate_reserve(&mut gate).is_ok()
        }
    };
    if !gate_ok {
        // Gate exhausted: HTTP needs no nav slot, so it still makes progress.
        // Fast legs on first paint; full chain for enrichment.
        let res = if first_paint {
            fetch_http_first_paint(&state.client, &sub).await
        } else {
            fetch_http_fallback(&state.client, &sub, None).await
        };
        match res {
            Ok((posts, after)) => {
                if first_paint {
                    store_head_fill(&tl, posts, after).await;
                } else {
                    apply_head_merge(&tl, posts, after).await;
                }
            }
            Err((code, msg, _)) => finish_with_error(&tl, code, msg).await,
        }
        return;
    }
    // Politeness: the nav slot above covers the browser leg. Race both legs
    // concurrently so HTTP first paint never waits out the ~7s+7s+2s
    // browser budget. First paint: fast parallel HTTP legs (public+RSS ~4s
    // each, OAuth skipped); enrichment: full chain with longer budgets.
    let http_client = state.client.clone();
    let sub_http = sub.clone();
    let sub_browser = sub.clone();
    let mut browser_handle =
        tokio::spawn(async move { scrape_sub_via_browser(&sub_browser, true).await });
    let mut http_handle = if first_paint {
        tokio::spawn(
            async move { fetch_http_first_paint(&http_client, &sub_http).await },
        )
    } else {
        tokio::spawn(
            async move { fetch_http_fallback(&http_client, &sub_http, None).await },
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
            if !posts.is_empty() {
                store_posts(&tl, first_paint, posts, after, &mut stored).await;
            } else {
                // Empty fast leg: don't store yet; the HTTP leg decides.
                // (If HTTP also comes back empty/failed we fall through to
                // the error/clear-loading handling below.)
            }
        }
        First::Browser(Ok(Err(e))) => {
            pending_http = Some(http_handle);
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
            let mut w = tl.write().await;
            w.last_error = Some(format!("browser task failed: {}", join_err));
        }
        First::Http(Ok(Ok((posts, after)))) => {
            pending_browser = Some(browser_handle);
            if !posts.is_empty() {
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
            http_err = Some(e);
        }
        First::Http(Err(join_err)) => {
            pending_browser = Some(browser_handle);
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
                if !posts.is_empty() {
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
                if !posts.is_empty() {
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
        return;
    }
    // Neither leg produced rows: surface block/rate-limit state.
    if let Some(e) = browser_err {
        if e.contains("BLOCKED") {
            mark_blocked(&tl, e).await;
            return;
        }
    }
    if let Some((code, msg, _)) = http_err {
        finish_with_error(&tl, code, msg).await;
    } else {
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
    let tl = timeline_arc(&state, &sub);
    {
        let r = tl.read().await;
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                return;
            }
        }
    }
    let cursor = {
        let r = tl.read().await;
        match r.reddit_after.clone() {
            Some(c) => c,
            None => return, // nothing older known; head refresh covers it
        }
    };
    let lock = inflight_lock(&state, &sub);
    let Ok(_guard) = lock.try_lock() else {
        return;
    };
    set_loading(&tl, true).await;
    match fetch_http_fallback(&state.client, &sub, Some(&cursor)).await {
        Ok((posts, next_after)) => apply_tail_merge(&tl, posts, next_after).await,
        Err((code, msg, _)) => finish_with_error(&tl, code, msg).await,
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
    let sub = q.sub.unwrap_or_else(|| "videos".to_string());
    if !is_valid_sub(&sub) {
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
    {
        let r = tl.read().await;
        if r.posts.is_empty() {
            if let Some(until) = r.blocked_until_ms {
                if now_ms() < until {
                    return error_json(
                        StatusCode::BAD_GATEWAY,
                        "UPSTREAM_BLOCKED",
                        format!(
                            "Reddit blocked automated access to r/{} (backoff); try again later",
                            sub
                        ),
                        None,
                    );
                }
            }
        }
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
    enum Job {
        Paginate,
        Head,
        None,
    }
    let job = if len > 0 && near_tail && has_cursor && !already_loading {
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
    (StatusCode::OK, Json(body)).into_response()
}

async fn refresh_handler(
    State(state): State<AppState>,
    Query(q): Query<RefreshQuery>,
) -> Response {
    let sub = q.sub.unwrap_or_else(|| "videos".to_string());
    if !is_valid_sub(&sub) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "BAD_SUB",
            format!("invalid subreddit name: {:?}", sub),
            None,
        );
    }
    let tl = timeline_arc(&state, &sub);
    // Priority lane: a never-fetched sub bypasses the global nav-gate peek.
    // Only a nav literally in flight (per-sub single-flight try_lock) may
    // return 429; otherwise queue an immediate nav and report loading.
    let never_fetched = tl.read().await.posts.is_empty();
    // Rate limiter intact: per-sub single-flight + global nav-gate peek.
    {
        let lock = inflight_lock(&state, &sub);
        if lock.try_lock().is_err() {
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
    (StatusCode::OK, Json(body)).into_response()
}

#[tokio::main]
async fn main() {
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
        nav_gate: Arc::new(Mutex::new(NavGate::default())),
    };

    let api = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/subs", get(subs))
        .route("/api/videos", get(videos_handler))
        .route("/api/refresh", post(refresh_handler));

    // Static files with index fallback for SPA-ish root.
    let serve_dir = ServeDir::new("static")
        .append_index_html_on_directories(true)
        .not_found_service(ServeFile::new("static/index.html"));

    let app = api.fallback_service(serve_dir).with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .expect("failed to bind");
    println!("reddittv listening on http://{}", addr);
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
}
