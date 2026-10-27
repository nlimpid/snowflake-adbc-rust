//! HTTP calls against the Snowflake SQL API, with the driver's retry rules.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use reqwest::Method;
use serde_json::Value;
use url::Url;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::protocol::Envelope;

#[derive(Clone, Copy)]
pub enum TokenKind<'a> {
    None,
    Session(&'a str),
    Master(&'a str),
}

pub struct JsonRequest<'a> {
    pub method: Method,
    pub path: &'a str,
    pub query: Vec<(&'a str, String)>,
    pub body: Option<Value>,
    pub token: TokenKind<'a>,
    pub user_agent: &'a str,
    pub attempt_timeout: Duration,
    pub retry_budget: Duration,
    /// Kept stable across HTTP retries so Snowflake can deduplicate the request.
    pub request_id: Option<String>,
}

pub async fn send_json(
    client: &reqwest::Client,
    base: &Url,
    request: JsonRequest<'_>,
) -> Result<Envelope> {
    let started = Instant::now();
    let request_id = request
        .request_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let mut attempt: u32 = 0;
    let mut delay = Duration::from_secs(1);
    loop {
        if started.elapsed() > request.retry_budget {
            return Err(Error::Timeout);
        }
        let remaining = request
            .retry_budget
            .saturating_sub(started.elapsed())
            .max(Duration::from_millis(1));
        let timeout = request.attempt_timeout.min(remaining);
        match send_once(client, base, &request, &request_id, attempt, timeout).await {
            Ok(envelope) => return Ok(envelope),
            Err(err) if err_is_retryable(&err) && started.elapsed() < request.retry_budget => {
                tracing::debug!(attempt, %err, "retrying snowflake request");
                tokio::time::sleep(delay.min(remaining)).await;
                delay = (delay * 2).min(Duration::from_secs(16));
                attempt = attempt.saturating_add(1);
            }
            Err(err) => return Err(err),
        }
    }
}

async fn send_once(
    client: &reqwest::Client,
    base: &Url,
    request: &JsonRequest<'_>,
    request_id: &str,
    attempt: u32,
    timeout: Duration,
) -> Result<Envelope> {
    let url = build_url(base, request.path, request_id, attempt, &request.query)?;
    let mut builder = client
        .request(request.method.clone(), url.clone())
        .timeout(timeout)
        .header("Accept", "application/snowflake")
        .header("Content-Type", "application/json")
        .header("User-Agent", request.user_agent);
    if let Some(token) = match request.token {
        TokenKind::None => None,
        TokenKind::Session(token) | TokenKind::Master(token) => Some(token),
    } {
        builder = builder.header("Authorization", format!("Snowflake Token=\"{token}\""));
    }
    if let Some(body) = &request.body {
        builder = builder.json(body);
    }
    let response = builder.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    if status.as_u16() == 429 || status.as_u16() == 408 || status.is_server_error() {
        return Err(Error::HttpStatus {
            status: status.as_u16(),
            body: truncate(&bytes),
        });
    }
    if !status.is_success() {
        return Err(Error::HttpStatus {
            status: status.as_u16(),
            body: truncate(&bytes),
        });
    }
    Envelope::parse(&bytes)
}

fn err_is_retryable(err: &Error) -> bool {
    match err {
        Error::Http(err) => err.is_timeout() || err.is_connect() || err.is_request(),
        Error::HttpStatus { status, .. } => *status == 429 || *status == 408 || *status >= 500,
        Error::Timeout => true,
        _ => false,
    }
}

fn build_url(
    base: &Url,
    path: &str,
    request_id: &str,
    attempt: u32,
    extra: &[(&str, String)],
) -> Result<Url> {
    let path = path.trim_start_matches('/');
    let mut url = base
        .join(path)
        .map_err(|err| Error::protocol(format!("join url {path}: {err}")))?;
    {
        let mut pairs = url.query_pairs_mut();
        pairs.append_pair("requestId", request_id);
        pairs.append_pair("request_guid", &Uuid::new_v4().to_string());
        pairs.append_pair("retryCount", &attempt.to_string());
        pairs.append_pair("clientStartTime", &epoch_millis().to_string());
        for (key, value) in extra {
            pairs.append_pair(key, value);
        }
    }
    Ok(url)
}

fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn truncate(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut text = text.trim().to_string();
    if text.len() > 512 {
        text.truncate(512);
        text.push_str("...");
    }
    text
}

/// Download a pre-signed chunk. Three attempts, no session token.
pub async fn download_bytes(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    user_agent: &str,
) -> Result<Vec<u8>> {
    let mut delay = Duration::from_secs(1);
    let mut last = Error::protocol("chunk download did not run");
    for attempt in 0..3 {
        match download_once(client, url, headers, user_agent).await {
            Ok(bytes) => return Ok(bytes),
            Err(err) => {
                last = err;
                if attempt < 2 {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(8));
                }
            }
        }
    }
    Err(last)
}

async fn download_once(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    user_agent: &str,
) -> Result<Vec<u8>> {
    let mut builder = client
        .get(url)
        .timeout(Duration::from_secs(120))
        .header("User-Agent", user_agent);
    for (key, value) in headers {
        builder = builder.header(key, value);
    }
    let response = builder.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?.to_vec();
    if !status.is_success() {
        return Err(Error::HttpStatus {
            status: status.as_u16(),
            body: truncate(&bytes),
        });
    }
    Ok(bytes)
}
