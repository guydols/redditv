use axum::{
    Json, Router,
    extract::{Query, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use moka::future::Cache;
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, time::Duration};
use tower_http::services::{ServeDir, ServeFile};

const DEFAULT_UA: &str = "linux:redditv:0.1.0 (by /u/redditv-dev)";
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

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    cache: Cache<String, serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct VideosQuery {
    sub: Option<String>,
    limit: Option<String>,
    after: Option<String>,
}

#[derive(Debug, Serialize)]
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

// ---------- helpers ----------

fn reddit_ua() -> String {
    std::env::var("REDDIT_UA").unwrap_or_else(|_| DEFAULT_UA.to_string())
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

// ---------- fetch paths ----------

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

    let cache_key = format!("{}:{}:{}", sub, limit, after.as_deref().unwrap_or("first"));
    if let Some(cached) = state.cache.get(&cache_key).await {
        return (StatusCode::OK, Json(cached)).into_response();
    }

    let ua = reddit_ua();

    // Primary: public JSON
    match fetch_public_json(&state.client, &ua, &sub, limit, after.as_deref()).await {
        Ok((videos, next_after)) => {
            let body = serde_json::json!({
                "sub": sub,
                "videos": videos,
                "after": next_after.clone(),
                "hasMore": next_after.is_some(),
            });
            state.cache.insert(cache_key, body.clone()).await;
            return (StatusCode::OK, Json(body)).into_response();
        }
        Err((code, msg, retry_ms)) => {
            // 429: honor Retry-After immediately
            if code == StatusCode::TOO_MANY_REQUESTS {
                return error_json(code, "UPSTREAM_429", msg, retry_ms);
            }
            // 403 / 5xx / network: try OAuth then RSS fallback
            let is_fallbackable = code == StatusCode::BAD_GATEWAY;
            if !is_fallbackable {
                return error_json(code, "UPSTREAM_5xx", msg, retry_ms);
            }
            // Optional OAuth retry
            if let Some(oauth_result) =
                fetch_oauth_json(&state.client, &ua, &sub, limit, after.as_deref()).await
            {
                match oauth_result {
                    Ok((videos, next_after)) => {
                        let body = serde_json::json!({
                            "sub": sub,
                            "videos": videos,
                            "after": next_after.clone(),
                            "hasMore": next_after.is_some(),
                        });
                        state.cache.insert(cache_key, body.clone()).await;
                        return (StatusCode::OK, Json(body)).into_response();
                    }
                    Err((c2, m2, r2)) => {
                        if c2 == StatusCode::TOO_MANY_REQUESTS {
                            return error_json(c2, "UPSTREAM_429", m2, r2);
                        }
                        // otherwise continue to RSS fallback
                    }
                }
            }
            // RSS fallback (no pagination cursor; RSS is a single page)
            match fetch_rss_fallback(&state.client, &ua, &sub).await {
                Ok(all) => {
                    // RSS has no `after` cursor; emulate limit client-side and report no more pages.
                    let videos: Vec<VideoItem> =
                        all.into_iter().take(limit as usize).collect();
                    let body = serde_json::json!({
                        "sub": sub,
                        "videos": videos,
                        "after": serde_json::Value::Null,
                        "hasMore": false,
                    });
                    state.cache.insert(cache_key, body.clone()).await;
                    return (StatusCode::OK, Json(body)).into_response();
                }
                Err((c3, m3, r3)) => {
                    if c3 == StatusCode::TOO_MANY_REQUESTS {
                        return error_json(c3, "UPSTREAM_429", m3, r3);
                    }
                    // Distinguish original 403 vs generic 5xx
                    if msg.contains("403") || m3.contains("403") {
                        return error_json(
                            StatusCode::BAD_GATEWAY,
                            "UPSTREAM_403",
                            format!("Reddit upstream denied the request. {}", m3),
                            r3,
                        );
                    }
                    return error_json(
                        StatusCode::BAD_GATEWAY,
                        "UPSTREAM_5xx",
                        format!("Reddit upstream failed ({}; fallback: {})", msg, m3),
                        r3,
                    );
                }
            }
        }
    }
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
    let cache: Cache<String, serde_json::Value> = Cache::builder()
        .max_capacity(1000)
        .time_to_live(Duration::from_secs(300))
        .build();

    let state = AppState { client, cache };

    let api = Router::new()
        .route("/healthz", get(healthz))
        .route("/api/subs", get(subs))
        .route("/api/videos", get(videos_handler));

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
