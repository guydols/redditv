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
const MAX_POSTS: usize = 300;
/// Minimum gap between two browser navigations (+ jitter 0-15s).
const MIN_NAV_GAP: Duration = Duration::from_secs(45);
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
        out.push(VideoItem {
            youtube_id: yt_id,
            youtube_url: yt_url,
            title: post.title.clone().unwrap_or_else(|| "Untitled".to_string()),
            reddit_url,
            thumbnail: post_thumbnail(post),
            created_utc: post.created_utc.map(|f| f as i64),
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
        out.push(VideoItem {
            youtube_id: yt_id,
            youtube_url: clean_url(&yt_url),
            title,
            reddit_url,
            thumbnail,
            created_utc,
        });
    }
    Ok(out)
}

/// Reqwest JSON -> OAuth -> RSS fallback chain. Returns timeline posts
/// (newest-first) on success.
async fn fetch_http_fallback(
    client: &reqwest::Client,
    sub: &str,
) -> Result<Vec<VideoItem>, (StatusCode, String, Option<u64>)> {
    let ua = reddit_ua();
    // Timeline fill always grabs the newest page; pagination is served
    // from memory via the index cursor.
    const FILL_LIMIT: u32 = 100;
    match fetch_public_json(client, &ua, sub, FILL_LIMIT, None).await {
        Ok((videos, _)) => return Ok(dedupe_cap(videos)),
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                return Err((code, msg, retry_ms));
            }
            if code != StatusCode::BAD_GATEWAY {
                return Err((code, msg, retry_ms));
            }
            if let Some(oauth_result) =
                fetch_oauth_json(client, &ua, sub, FILL_LIMIT, None).await
            {
                match oauth_result {
                    Ok((videos, _)) => return Ok(dedupe_cap(videos)),
                    Err((c2, m2, r2)) => {
                        if c2 == StatusCode::TOO_MANY_REQUESTS {
                            return Err((c2, m2, r2));
                        }
                    }
                }
            }
            match fetch_rss_fallback(client, &ua, sub).await {
                Ok(all) => Ok(dedupe_cap(all)),
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
               redditUrl: permalink || '', thumbnail: thumbnail || '', createdUtc: null });
    if (out.length >= 300) break;
  }
  return out;
})()"#;

/// Scrape one sub's newest posts via the shared headless browser.
/// Holds the browser lock for the whole scrape (global serialization).
/// Returns Err with "BLOCKED: ..." prefix when a selector timeout or
/// challenge page suggests bot detection.
async fn scrape_sub_via_browser(sub: &str) -> Result<Vec<VideoItem>, String> {
    if chrome_disabled() {
        return Err("CHROME_DISABLED".to_string());
    }
    let cell = browser_cell();
    let mut guard = cell.lock().await;
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
    let result: Result<Vec<VideoItem>, String> = async {
        page.enable_stealth_mode_with_agent(CHROME_UA)
            .await
            .map_err(|e| format!("stealth mode failed: {}", e))?;
        // Belt-and-braces webdriver hiding (stealth mode already covers this).
        let _ = page
            .evaluate_on_new_document(
                "Object.defineProperty(Object.getPrototypeOf(navigator), 'webdriver', { get: () => undefined });",
            )
            .await;
        // old.reddit first (lighter), then www fallback.
        let urls = [
            format!("https://old.reddit.com/r/{}/new/", sub),
            format!("https://www.reddit.com/r/{}/new/", sub),
        ];
        let mut nav_ok = false;
        let mut last_err = String::from("no navigation attempted");
        for u in &urls {
            match tokio::time::timeout(Duration::from_secs(45), page.goto(u.as_str())).await
            {
                Ok(Ok(_)) => {
                    nav_ok = true;
                    break;
                }
                Ok(Err(e)) => last_err = e.to_string(),
                Err(_) => last_err = "navigation timeout".to_string(),
            }
        }
        if !nav_ok {
            return Err(format!("navigation failed: {}", last_err));
        }
        // Wait for post content; a timeout here usually means a challenge/block.
        match tokio::time::timeout(
            Duration::from_secs(20),
            page.find_element("a.title, shreddit-post"),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => return Err(format!("BLOCKED: selector not found: {}", e)),
            Err(_) => return Err("BLOCKED: selector timeout (possible challenge)".to_string()),
        }
        // Human-ish dwell: 2-6s + jitter.
        let dwell = 2000 + (now_ms() as u64 % 4000) + jitter_15s_ms() % 1000;
        tokio::time::sleep(Duration::from_millis(dwell)).await;
        // One scroll to the bottom to trigger lazy content.
        let _ = page
            .evaluate("window.scrollTo(0, document.body.scrollHeight)")
            .await;
        tokio::time::sleep(Duration::from_secs(2)).await;
        let rows: Vec<ScrapedRow> = page
            .evaluate(EXTRACT_JS)
            .await
            .map_err(|e| format!("extract failed: {}", e))?
            .into_value()
            .map_err(|e| format!("extract decode failed: {}", e))?;
        let mut seen: HashSet<String> = HashSet::new();
        let mut out: Vec<VideoItem> = Vec::new();
        for r in rows {
            if r.youtube_id.is_empty() || !seen.insert(r.youtube_id.clone()) {
                continue;
            }
            let thumbnail = if r.thumbnail.starts_with("http") {
                clean_url(&r.thumbnail)
            } else {
                PLACEHOLDER_THUMB.to_string()
            };
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
            });
            if out.len() >= MAX_POSTS {
                break;
            }
        }
        Ok(out)
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
) -> serde_json::Value {
    let now = now_ms();
    let stale = tl
        .fetched_at_ms
        .map(|f| now - f > TTL_MS)
        .unwrap_or(true);
    let start = start.min(tl.posts.len());
    let end = (start + limit).min(tl.posts.len());
    let videos = &tl.posts[start..end];
    let next_after = if end < tl.posts.len() {
        Some(end.to_string())
    } else {
        None
    };
    let has_more = end < tl.posts.len();
    serde_json::json!({
        "sub": sub,
        "videos": videos,
        "after": next_after,
        "hasMore": has_more,
        "cached": cached,
        "fetchedAt": tl.fetched_at_ms,
        "stale": stale,
    })
}

/// Store fresh posts in the timeline.
async fn store_posts(tl: &Arc<RwLock<SubTimeline>>, posts: Vec<VideoItem>) {
    let mut w = tl.write().await;
    w.posts = posts;
    w.fetched_at_ms = Some(now_ms());
    w.last_error = None;
    w.blocked_until_ms = None;
}

async fn mark_blocked(tl: &Arc<RwLock<SubTimeline>>, reason: String) {
    let mut w = tl.write().await;
    w.last_error = Some("BLOCKED".to_string());
    w.blocked_until_ms = Some(now_ms() + BLOCKED_BACKOFF_MS);
    let _ = reason;
}

/// Blocking fetch for a sub with no usable timeline data.
/// Order: browser scrape -> reqwest fallback chain -> 502.
/// Only called when the sub was never successfully fetched.
async fn blocking_fetch(state: &AppState, sub: &str) -> Response {
    let tl = timeline_arc(state, sub);
    // Respect block backoff even when never fetched.
    {
        let r = tl.read().await;
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                return error_json(
                    StatusCode::BAD_GATEWAY,
                    "UPSTREAM_BLOCKED",
                    format!("Reddit blocked automated access to r/{} (backoff); try again later", sub),
                    None,
                );
            }
        }
    }
    // Per-sub single-flight.
    let lock = inflight_lock(state, sub);
    let Ok(_guard) = lock.try_lock() else {
        return error_json(
            StatusCode::TOO_MANY_REQUESTS,
            "RATE_LIMITED",
            format!("r/{} is already being fetched; retry shortly", sub),
            Some(10_000),
        );
    };
    // Global navigation throttle.
    {
        let mut gate = state.nav_gate.lock().await;
        if let Err(retry_ms) = nav_gate_reserve(&mut gate) {
            return error_json(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "browser navigation budget exhausted; retry later".to_string(),
                Some(retry_ms),
            );
        }
    }
    // 1) Stealth browser (skipped when CHROME_DISABLED=1).
    if !chrome_disabled() {
        match scrape_sub_via_browser(sub).await {
            Ok(posts) => {
                let posts = dedupe_cap(posts);
                store_posts(&tl, posts).await;
                let r = tl.read().await;
                // Fresh fill: serve from index 0 with default page size below;
                // the caller re-slices, so return the full first page generically.
                return (StatusCode::OK, Json(slice_body(sub, &r, 0, usize::MAX, false)))
                    .into_response();
            }
            Err(e) => {
                if e.contains("BLOCKED") {
                    mark_blocked(&tl, e).await;
                } else {
                    let mut w = tl.write().await;
                    w.last_error = Some(e);
                }
                // fall through to reqwest fallback
            }
        }
    }
    // 2) Reqwest JSON/OAuth/RSS fallback (also the CHROME_DISABLED path).
    match fetch_http_fallback(&state.client, sub).await {
        Ok(posts) => {
            store_posts(&tl, posts).await;
            let r = tl.read().await;
            (StatusCode::OK, Json(slice_body(sub, &r, 0, usize::MAX, false))).into_response()
        }
        Err((code, msg, retry_ms)) => {
            if code == StatusCode::TOO_MANY_REQUESTS {
                return error_json(code, "UPSTREAM_429", msg, retry_ms);
            }
            if msg.contains("403") {
                return error_json(
                    StatusCode::BAD_GATEWAY,
                    "UPSTREAM_BLOCKED",
                    format!("Reddit blocked automated access to r/{}. {}", sub, msg),
                    retry_ms,
                );
            }
            error_json(
                StatusCode::BAD_GATEWAY,
                "UPSTREAM_BLOCKED",
                format!("Reddit upstream failed for r/{}: {}", sub, msg),
                retry_ms,
            )
        }
    }
}

/// Background refresh: serve-stale trigger. Never blocks a response;
/// updates the timeline in place on success.
async fn background_refresh(state: AppState, sub: String) {
    let tl = timeline_arc(&state, &sub);
    {
        let r = tl.read().await;
        if let Some(until) = r.blocked_until_ms {
            if now_ms() < until {
                return;
            }
        }
    }
    let lock = inflight_lock(&state, &sub);
    let Ok(_guard) = lock.try_lock() else {
        return;
    };
    {
        let mut gate = state.nav_gate.lock().await;
        if nav_gate_reserve(&mut gate).is_err() {
            return;
        }
    }
    if !chrome_disabled() {
        match scrape_sub_via_browser(&sub).await {
            Ok(posts) => {
                store_posts(&tl, dedupe_cap(posts)).await;
                return;
            }
            Err(e) => {
                if e.contains("BLOCKED") {
                    mark_blocked(&tl, e).await;
                    return;
                }
                let mut w = tl.write().await;
                w.last_error = Some(e);
            }
        }
    }
    match fetch_http_fallback(&state.client, &sub).await {
        Ok(posts) => store_posts(&tl, posts).await,
        Err((code, msg, _)) => {
            let mut w = tl.write().await;
            if code == StatusCode::TOO_MANY_REQUESTS {
                w.last_error = Some("UPSTREAM_429".to_string());
            } else if msg.contains("403") || msg.contains("BLOCKED") {
                w.last_error = Some("BLOCKED".to_string());
                w.blocked_until_ms = Some(now_ms() + BLOCKED_BACKOFF_MS);
            } else {
                w.last_error = Some(msg);
            }
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
    let never_seen = tl.read().await.fetched_at_ms.is_none();

    // Only block on the browser when the sub was never fetched.
    if never_seen {
        let resp = blocking_fetch(&state, &sub).await;
        // blocking_fetch returns the full timeline; re-slice to the
        // requested page when it succeeded.
        if resp.status() == StatusCode::OK {
            let r = tl.read().await;
            return (
                StatusCode::OK,
                Json(slice_body(&sub, &r, start, limit as usize, false)),
            )
                .into_response();
        }
        return resp;
    }

    // Serve from memory; kick a background refresh when stale or forced.
    let (body, needs_bg) = {
        let r = tl.read().await;
        let now = now_ms();
        let is_stale = r
            .fetched_at_ms
            .map(|f| now - f > TTL_MS)
            .unwrap_or(true);
        let mut body = slice_body(&sub, &r, start, limit as usize, true);
        if refresh_req {
            // Requested fresh data that is still being fetched: flag stale so
            // the UI can show a refreshing state.
            body["stale"] = serde_json::Value::Bool(true);
        }
        (body, is_stale || refresh_req)
    };
    if needs_bg {
        let bg_state = state.clone();
        let bg_sub = sub.clone();
        tokio::spawn(async move {
            background_refresh(bg_state, bg_sub).await;
        });
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
    if tl.read().await.fetched_at_ms.is_none() {
        // Nothing to serve stale: block like a first fetch.
        return blocking_fetch(&state, &sub).await;
    }
    // Same limiter as foreground fetches (peek; the task reserves on nav).
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
        let mut gate = state.nav_gate.lock().await;
        if let Err(retry_ms) = nav_gate_peek(&mut gate) {
            return error_json(
                StatusCode::TOO_MANY_REQUESTS,
                "RATE_LIMITED",
                "browser navigation budget exhausted; retry later".to_string(),
                Some(retry_ms),
            );
        }
    }
    let bg_state = state.clone();
    let bg_sub = sub.clone();
    tokio::spawn(async move {
        background_refresh(bg_state, bg_sub).await;
    });
    let r = tl.read().await;
    let body = serde_json::json!({
        "sub": sub,
        "queued": true,
        "cached": true,
        "fetchedAt": r.fetched_at_ms,
        "stale": true,
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
