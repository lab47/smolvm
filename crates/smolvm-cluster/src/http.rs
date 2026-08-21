//! Minimal HTTP/1.1 parsing for the frontend's request router.
//!
//! The frontend stays a byte tunnel for everything it can. It only needs to parse
//! far enough to (a) route by method+path and (b) for the create request, read
//! and rewrite the small JSON body to assign a name and forward to the chosen
//! backend. Streaming bodies (file upload, SSE, WS) are never buffered here.

use tokio::io::{AsyncRead, AsyncReadExt};

/// Cap on the request head we'll buffer while looking for the header terminator.
const HEAD_CAP: usize = 16 * 1024;

/// Read from `r` into `buf` until the `\r\n\r\n` header terminator appears.
/// Returns the index just past it (start of the body), or `None` on EOF/cap.
pub async fn read_headers<R>(r: &mut R, buf: &mut Vec<u8>) -> std::io::Result<Option<usize>>
where
    R: AsyncRead + Unpin,
{
    let mut tmp = [0u8; 1024];
    loop {
        if let Some(pos) = find_headers_end(buf) {
            return Ok(Some(pos));
        }
        if buf.len() >= HEAD_CAP {
            return Ok(None);
        }
        let n = r.read(&mut tmp).await?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&tmp[..n]);
    }
}

fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|p| p + 4)
}

/// A parsed request head: the method, the raw target (path + query), and headers
/// with lowercased names.
pub struct ReqHead {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
}

impl ReqHead {
    /// Path with any query string stripped.
    pub fn path(&self) -> &str {
        self.target.split('?').next().unwrap_or("")
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn content_length(&self) -> Option<usize> {
        self.header("content-length")?.trim().parse().ok()
    }
}

/// Parse the request line and headers from `head` (bytes up to `\r\n\r\n`).
pub fn parse_head(head: &[u8]) -> Option<ReqHead> {
    let text = std::str::from_utf8(head).ok()?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    Some(ReqHead {
        method,
        target,
        headers,
    })
}

/// Rebuild a request with `body`, setting Content-Length and forcing
/// `Connection: close` (the routed path doesn't reuse connections). Existing
/// Content-Length / Transfer-Encoding / Connection headers are dropped.
pub fn build_request(head: &ReqHead, body: &[u8]) -> Vec<u8> {
    let mut out = format!("{} {} HTTP/1.1\r\n", head.method, head.target).into_bytes();
    for (k, v) in &head.headers {
        if k == "content-length" || k == "transfer-encoding" || k == "connection" {
            continue;
        }
        out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }
    out.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
    out.extend_from_slice(b"connection: close\r\n\r\n");
    out.extend_from_slice(body);
    out
}

/// Parse the status code from a response's first line.
pub fn parse_status(resp: &[u8]) -> Option<u16> {
    let end = resp.windows(2).position(|w| w == b"\r\n").unwrap_or(resp.len());
    let line = std::str::from_utf8(&resp[..end]).ok()?;
    line.split_whitespace().nth(1)?.parse().ok()
}
