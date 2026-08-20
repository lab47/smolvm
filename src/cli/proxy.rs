//! `smolvm proxy` — an e2b-style preview reverse proxy.
//!
//! Maps `<guestPort>-<sandboxId>.<anything>` hostnames to a sandbox's published
//! port and forwards HTTP (and WebSocket upgrades) to it. Everything after the
//! FIRST dot of the Host header is ignored, so you can front it with any domain
//! (wildcard DNS `*.your.domain -> this proxy`). A request to a PAUSED sandbox
//! resumes it first (e2b auto-resume), then forwards.
//!
//! smolvm has no routable guest IP, so the proxy reaches the guest through the
//! sandbox's published host port (auto-allocated at create): it asks the serve
//! API for the machine, finds the host port mapped to the requested guest port,
//! and tunnels to `127.0.0.1:<hostPort>`.

use clap::Args;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UnixStream};

use smolvm::error::{Error, Result};

/// Run an HTTP preview proxy for sandboxes served by `smolvm serve`.
#[derive(Args, Debug)]
pub struct ProxyCmd {
    /// Address to listen on for inbound HTTP.
    #[arg(short, long, default_value = "127.0.0.1:8080", value_name = "ADDR:PORT")]
    listen: String,

    /// The `smolvm serve` API used to resolve and resume sandboxes.
    /// `unix:///path/to.sock` or `http://host:port`.
    #[arg(long, value_name = "URL")]
    serve: Option<String>,

    /// Bearer token for the serve API, if it requires one.
    #[arg(long, value_name = "TOKEN")]
    api_key: Option<String>,

    /// Resume a paused sandbox when a request arrives (e2b auto-resume). Pass
    /// `--auto-resume false` to instead return 409 for a paused sandbox.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    auto_resume: bool,

    /// Host the published ports are bound on (defaults to 127.0.0.1, or
    /// $SMOLVM_PUBLISH_ADDR if the node widened its bind).
    #[arg(long, value_name = "ADDR")]
    upstream_host: Option<String>,
}

impl ProxyCmd {
    pub fn run(self) -> Result<()> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(Error::Io)?;
        runtime.block_on(self.serve_forever())
    }

    async fn serve_forever(self) -> Result<()> {
        let serve_url = self
            .serve
            .clone()
            .or_else(|| std::env::var("SMOLVM_API_URL").ok())
            .unwrap_or_else(default_serve_url);
        let upstream_host = self
            .upstream_host
            .clone()
            .or_else(|| std::env::var("SMOLVM_PUBLISH_ADDR").ok())
            .unwrap_or_else(|| "127.0.0.1".to_string());

        let cfg = std::sync::Arc::new(ProxyConfig {
            serve: ServeTarget::parse(&serve_url)?,
            api_key: self.api_key.clone(),
            auto_resume: self.auto_resume,
            upstream_host,
        });

        let listener = TcpListener::bind(&self.listen).await.map_err(Error::Io)?;
        tracing::info!(
            listen = %self.listen,
            serve = %serve_url,
            auto_resume = self.auto_resume,
            "smolvm preview proxy listening"
        );

        loop {
            let (client, peer) = match listener.accept().await {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(error = %e, "accept failed");
                    continue;
                }
            };
            let cfg = cfg.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_conn(client, cfg).await {
                    tracing::debug!(%peer, error = %e, "connection closed with error");
                }
            });
        }
    }
}

struct ProxyConfig {
    serve: ServeTarget,
    api_key: Option<String>,
    auto_resume: bool,
    upstream_host: String,
}

/// How to reach the serve API: a Unix socket or a TCP host:port.
enum ServeTarget {
    Unix(String),
    Tcp(String),
}

impl ServeTarget {
    fn parse(url: &str) -> Result<Self> {
        if let Some(path) = url.strip_prefix("unix://") {
            Ok(ServeTarget::Unix(path.to_string()))
        } else if let Some(rest) = url.strip_prefix("http://") {
            Ok(ServeTarget::Tcp(rest.trim_end_matches('/').to_string()))
        } else {
            Err(Error::agent("proxy", format!(
                "unsupported --serve URL '{url}' (use unix:///path or http://host:port)"
            )))
        }
    }
}

fn default_serve_url() -> String {
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("unix://{dir}/smolvm.sock")
    } else {
        "http://127.0.0.1:8080".to_string()
    }
}

const MAX_HEAD_BYTES: usize = 64 * 1024;
const RESUME_READY_TIMEOUT: Duration = Duration::from_secs(60);

async fn handle_conn(mut client: TcpStream, cfg: std::sync::Arc<ProxyConfig>) -> Result<()> {
    // Read the request head (up to the blank line) so we can route on Host. The
    // bytes we consume here are replayed to the upstream before tunnelling.
    let mut head = Vec::with_capacity(4096);
    let mut buf = [0u8; 8192];
    loop {
        if head.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if head.len() > MAX_HEAD_BYTES {
            return reply(&mut client, 431, "request header too large").await;
        }
        let n = client.read(&mut buf).await.map_err(Error::Io)?;
        if n == 0 {
            return Ok(()); // client closed before sending a full head
        }
        head.extend_from_slice(&buf[..n]);
    }

    let host = match extract_host(&head) {
        Some(h) => h,
        None => return reply(&mut client, 400, "missing Host header").await,
    };
    let (guest_port, sandbox_id) = match parse_preview_host(&host) {
        Some(v) => v,
        None => {
            return reply(
                &mut client,
                404,
                "host must be <port>-<sandboxId>.<domain>",
            )
            .await
        }
    };

    // Resolve the sandbox's published host port for this guest port, resuming a
    // paused sandbox first when auto-resume is on.
    let host_port = match resolve_upstream(&cfg, &sandbox_id, guest_port).await {
        Ok(p) => p,
        Err(RouteError::NotFound) => {
            return reply(&mut client, 404, "sandbox not found").await
        }
        Err(RouteError::Paused) => {
            return reply(&mut client, 409, "sandbox is paused (auto-resume disabled)").await
        }
        Err(RouteError::PortNotPublished) => {
            return reply(
                &mut client,
                502,
                "requested port is not published by this sandbox",
            )
            .await
        }
        Err(RouteError::Upstream(msg)) => {
            tracing::warn!(sandbox = %sandbox_id, error = %msg, "resolve upstream failed");
            return reply(&mut client, 502, "failed to resolve sandbox").await;
        }
    };

    // Connect to the sandbox's published port and tunnel. Replay the head we
    // already read, then bidirectionally copy — this transparently carries HTTP
    // keep-alive and WebSocket upgrades on the same connection.
    let addr = format!("{}:{}", cfg.upstream_host, host_port);
    let mut upstream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(%addr, error = %e, "upstream connect failed");
            return reply(&mut client, 502, "sandbox service is not reachable").await;
        }
    };
    upstream.write_all(&head).await.map_err(Error::Io)?;
    let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
    Ok(())
}

enum RouteError {
    NotFound,
    Paused,
    PortNotPublished,
    Upstream(String),
}

async fn resolve_upstream(
    cfg: &ProxyConfig,
    sandbox_id: &str,
    guest_port: u16,
) -> std::result::Result<u16, RouteError> {
    let path = format!("/api/v1/machines/{}", urlencode(sandbox_id));
    let (status, body) = serve_api(cfg, "GET", &path, None)
        .await
        .map_err(|e| RouteError::Upstream(e.to_string()))?;
    if status == 404 {
        return Err(RouteError::NotFound);
    }
    if !(200..300).contains(&status) {
        return Err(RouteError::Upstream(format!("GET machine -> {status}")));
    }
    let mut info: MachineJson =
        serde_json::from_slice(&body).map_err(|e| RouteError::Upstream(e.to_string()))?;

    if info.state != "running" {
        if info.state == "paused" && cfg.auto_resume {
            resume_and_wait(cfg, sandbox_id).await?;
            let (_s, body) = serve_api(cfg, "GET", &path, None)
                .await
                .map_err(|e| RouteError::Upstream(e.to_string()))?;
            info = serde_json::from_slice(&body).map_err(|e| RouteError::Upstream(e.to_string()))?;
        } else if info.state == "paused" || info.state == "pausing" {
            return Err(RouteError::Paused);
        }
        // Any other non-running state falls through; the port lookup below fails
        // cleanly if the machine isn't actually serving.
    }

    info.ports
        .iter()
        .find(|p| p.guest == guest_port)
        .map(|p| p.host)
        .ok_or(RouteError::PortNotPublished)
}

async fn resume_and_wait(cfg: &ProxyConfig, sandbox_id: &str) -> std::result::Result<(), RouteError> {
    let path = format!("/api/v1/machines/{}/resume", urlencode(sandbox_id));
    let (status, _body) = serve_api(cfg, "POST", &path, None)
        .await
        .map_err(|e| RouteError::Upstream(e.to_string()))?;
    // The resume handler already blocks until the restored agent is ready, so a
    // 2xx means it's running. A 409 means someone else won the race — poll below.
    if (200..300).contains(&status) {
        return Ok(());
    }
    let get = format!("/api/v1/machines/{}", urlencode(sandbox_id));
    let deadline = tokio::time::Instant::now() + RESUME_READY_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Ok((_s, body)) = serve_api(cfg, "GET", &get, None).await {
            if let Ok(info) = serde_json::from_slice::<MachineJson>(&body) {
                if info.state == "running" {
                    return Ok(());
                }
            }
        }
    }
    Err(RouteError::Upstream("sandbox did not become ready".into()))
}

#[derive(serde::Deserialize)]
struct MachineJson {
    state: String,
    #[serde(default)]
    ports: Vec<PortJson>,
}

#[derive(serde::Deserialize)]
struct PortJson {
    host: u16,
    guest: u16,
}

/// Issue one request to the serve API over its Unix socket or TCP endpoint,
/// returning `(status, body)`. Uses `Connection: close` so the body is bounded by
/// EOF — no chunked/Content-Length parsing needed for these small JSON replies.
async fn serve_api(
    cfg: &ProxyConfig,
    method: &str,
    path: &str,
    body: Option<&[u8]>,
) -> Result<(u16, Vec<u8>)> {
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\naccept: application/json\r\n"
    );
    if let Some(ref key) = cfg.api_key {
        req.push_str(&format!("authorization: Bearer {key}\r\n"));
    }
    match body {
        Some(b) => {
            req.push_str(&format!(
                "content-type: application/json\r\ncontent-length: {}\r\n\r\n",
                b.len()
            ));
        }
        None => req.push_str("\r\n"),
    }

    let mut raw = req.into_bytes();
    if let Some(b) = body {
        raw.extend_from_slice(b);
    }

    let response = match &cfg.serve {
        ServeTarget::Unix(p) => {
            let mut s = UnixStream::connect(p).await.map_err(Error::Io)?;
            s.write_all(&raw).await.map_err(Error::Io)?;
            let mut out = Vec::new();
            s.read_to_end(&mut out).await.map_err(Error::Io)?;
            out
        }
        ServeTarget::Tcp(addr) => {
            let mut s = TcpStream::connect(addr).await.map_err(Error::Io)?;
            s.write_all(&raw).await.map_err(Error::Io)?;
            let mut out = Vec::new();
            s.read_to_end(&mut out).await.map_err(Error::Io)?;
            out
        }
    };
    parse_http_response(&response)
}

fn parse_http_response(bytes: &[u8]) -> Result<(u16, Vec<u8>)> {
    let split = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| Error::agent("proxy", "malformed HTTP response from serve".to_string()))?;
    let head = &bytes[..split];
    let body = bytes[split + 4..].to_vec();
    // Status line: "HTTP/1.1 200 OK"
    let first_line = head.split(|&b| b == b'\n').next().unwrap_or(head);
    let line = String::from_utf8_lossy(first_line);
    let status = line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or_else(|| Error::agent("proxy", "no status code in serve response".to_string()))?;
    Ok((status, body))
}

/// Pull the Host header value (without any `:port`) from a raw request head.
fn extract_host(head: &[u8]) -> Option<String> {
    let text = String::from_utf8_lossy(head);
    for line in text.split("\r\n") {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("host") {
                let v = value.trim();
                let host = v.rsplit_once(':').map(|(h, _)| h).unwrap_or(v);
                return Some(host.to_string());
            }
        }
    }
    None
}

/// Parse `<guestPort>-<sandboxId>` out of a Host header, ignoring everything
/// after the first dot (so any base domain works). Splits on the FIRST `-`, so a
/// sandbox id that itself contains `-` (e.g. `vm-abc123`) is preserved.
fn parse_preview_host(host: &str) -> Option<(u16, String)> {
    let label = host.split('.').next().unwrap_or(host);
    let (port_str, id) = label.split_once('-')?;
    let port: u16 = port_str.parse().ok()?;
    if id.is_empty() {
        return None;
    }
    Some((port, id.to_string()))
}

fn urlencode(s: &str) -> String {
    // Sandbox ids are validated names (alphanumerics, '-', '_'), so a light
    // encoder that passes those through and percent-encodes anything else is
    // enough for a path segment.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn reply(client: &mut TcpStream, status: u16, message: &str) -> Result<()> {
    let reason = match status {
        400 => "Bad Request",
        404 => "Not Found",
        409 => "Conflict",
        431 => "Request Header Fields Too Large",
        502 => "Bad Gateway",
        _ => "Error",
    };
    let body = format!("{status} {reason}: {message}\n");
    let resp = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = client.write_all(resp.as_bytes()).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_preview_host_ignoring_domain() {
        assert_eq!(
            parse_preview_host("3000-vm-abc123.preview.example.com"),
            Some((3000, "vm-abc123".to_string()))
        );
        // Any domain on the right works, including none.
        assert_eq!(
            parse_preview_host("8080-vm-xyz.localhost"),
            Some((8080, "vm-xyz".to_string()))
        );
        assert_eq!(
            parse_preview_host("443-mysandbox"),
            Some((443, "mysandbox".to_string()))
        );
    }

    #[test]
    fn rejects_non_preview_hosts() {
        assert_eq!(parse_preview_host("example.com"), None); // no port-
        assert_eq!(parse_preview_host("notaport-vm-abc.dev"), None); // non-numeric port
        assert_eq!(parse_preview_host("3000-.dev"), None); // empty id
    }

    #[test]
    fn extracts_host_without_port() {
        let head = b"GET / HTTP/1.1\r\nHost: 3000-vm-abc.dev:8080\r\n\r\n";
        assert_eq!(extract_host(head).as_deref(), Some("3000-vm-abc.dev"));
    }

    #[test]
    fn parses_status_line() {
        let (s, body) = parse_http_response(b"HTTP/1.1 200 OK\r\nx: y\r\n\r\n{\"ok\":true}").unwrap();
        assert_eq!(s, 200);
        assert_eq!(body, b"{\"ok\":true}");
    }
}
