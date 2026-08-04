//! Shared hand-rolled HTTP/1.1 helpers for the node's production-only HTTP edges:
//! the DynamoDB JSON endpoint (`dynamo.rs`) and the admin interface (`admin.rs`,
//! ADR 0020). Real tokio sockets — this is `ProdEnv`-only edge code, never under
//! `SimEnv`, so it does its own I/O directly rather than through the `Env` seam.
//!
//! The parser handles the small slice of HTTP/1.1 these edges need: a request
//! line, `Content-Length`/`Connection`/`X-Amz-Target` headers, keep-alive, and
//! pipelined requests. Responses are a single content-type-tagged body.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Max request size (headers + body) we will buffer before erroring.
pub(crate) const MAX_BODY: usize = 1 << 20;

/// A parsed HTTP request.
pub(crate) struct HttpRequest {
    /// The request method (`GET`, `POST`, …).
    pub(crate) method: String,
    /// The request path, with any `?query` stripped off (see [`Self::query`]).
    pub(crate) path: String,
    /// The raw query string after `?` (empty if none). Parse with
    /// [`query_param`].
    pub(crate) query: String,
    /// The `X-Amz-Target` header value (used only by the DynamoDB edge; empty
    /// otherwise).
    pub(crate) target: String,
    /// The request body bytes.
    pub(crate) body: Vec<u8>,
    /// Whether the client wants the connection kept alive.
    pub(crate) keep_alive: bool,
}

/// Look up `name` in a raw `a=1&b=2` query string, returning its (un-decoded)
/// value. Only the minimal `%NN` and `+` decoding the admin edge needs is applied.
pub(crate) fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == name).then(|| percent_decode(v))
    })
}

/// Minimal `application/x-www-form-urlencoded` value decoding: `+` → space and
/// `%NN` → byte. Invalid escapes are passed through verbatim.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push((hi * 16 + lo) as u8);
                    i += 3;
                    continue;
                }
                out.push(b'%');
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Read one HTTP/1.1 request from `stream`, buffering into `buf` (which may
/// already hold bytes of the next pipelined request). Returns `None` at clean
/// EOF before any bytes of a new request.
pub(crate) async fn read_http_request(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
) -> std::io::Result<Option<HttpRequest>> {
    // Read until we have the full header block (terminated by CRLFCRLF).
    let header_end = loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return if buf.is_empty() {
                Ok(None)
            } else {
                Err(eof("connection closed mid-request"))
            };
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.len() > MAX_BODY {
            return Err(eof("request headers too large"));
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let mut lines = header_text.split("\r\n");
    // HTTP/1.1 defaults to keep-alive; HTTP/1.0 defaults to close. An explicit
    // `Connection` header overrides either way.
    let request_line = lines.next().unwrap_or("");
    // Request line: `METHOD SP request-target SP HTTP-version`.
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or("").to_owned();
    let target_path = request_parts.next().unwrap_or("");
    // Split the request-target into path + query string.
    let (path, query) = match target_path.split_once('?') {
        Some((p, q)) => (p.to_owned(), q.to_owned()),
        None => (target_path.to_owned(), String::new()),
    };
    let mut keep_alive = request_line.contains("HTTP/1.1");
    let mut target = String::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            let name = name.trim().to_ascii_lowercase();
            let value = value.trim();
            match name.as_str() {
                "x-amz-target" => target = value.to_owned(),
                "content-length" => {
                    content_length = value.parse().map_err(|_| eof("invalid Content-Length"))?;
                }
                "connection" => {
                    let v = value.to_ascii_lowercase();
                    if v.contains("close") {
                        keep_alive = false;
                    } else if v.contains("keep-alive") {
                        keep_alive = true;
                    }
                }
                _ => {}
            }
        }
    }
    if content_length > MAX_BODY {
        return Err(eof("request body too large"));
    }

    // Read the body (some of which may already be buffered).
    let mut body_buf = buf.split_off(header_end);
    while body_buf.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(eof("connection closed mid-body"));
        }
        body_buf.extend_from_slice(&chunk[..n]);
    }
    // Any surplus belongs to the next pipelined request.
    let leftover = body_buf.split_off(content_length);
    *buf = leftover;

    Ok(Some(HttpRequest {
        method,
        path,
        query,
        target,
        body: body_buf,
        keep_alive,
    }))
}

pub(crate) fn eof(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// CORS header lines for the admin edge (ADR 0021). The web dashboard is served
/// from one node's admin port but fans out to *every* node's `/admin/*` JSON, so
/// those cross-origin reads need a permissive `Access-Control-Allow-Origin`.
/// Scoped to the admin listener only — the data edges (dynamo/cql) never send it;
/// the admin port is assumed bound to a trusted interface (ADR 0020, no auth yet).
/// Each line ends with CRLF so it can be spliced straight into the header block.
pub(crate) const CORS_HEADERS: &str = "Access-Control-Allow-Origin: *\r\n\
     Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
     Access-Control-Allow-Headers: Content-Type\r\n";

/// The reason phrase for a status code (the small set our edges emit).
fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}

/// Write a minimal HTTP/1.1 response with the given `content_type` and `body`.
/// The `Connection` header echoes the client's keep-alive choice so a
/// `Connection: close` client (which then reads to EOF) is unblocked by the
/// socket closing.
pub(crate) async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response_with(stream, status, content_type, body, keep_alive, "").await
}

/// Like [`write_response`], but splices `extra_headers` (a block of complete
/// CRLF-terminated header lines, e.g. [`CORS_HEADERS`]) into the response before
/// the blank line. Pass `""` for none.
pub(crate) async fn write_response_with(
    stream: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    keep_alive: bool,
    extra_headers: &str,
) -> std::io::Result<()> {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Connection: {connection}\r\n\
         {extra_headers}\
         \r\n\
         {body}",
        reason = reason(status),
        len = body.len(),
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

/// Write a DynamoDB-protocol JSON response (`application/x-amz-json-1.0`).
pub(crate) async fn write_amz_json_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response(
        stream,
        status,
        "application/x-amz-json-1.0",
        body,
        keep_alive,
    )
    .await
}

/// Write a `text/plain` response — used by the `/metrics` route, whose body is the
/// line-oriented metrics export rather than JSON.
pub(crate) async fn write_text_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    keep_alive: bool,
) -> std::io::Result<()> {
    write_response(
        stream,
        status,
        "text/plain; charset=utf-8",
        body,
        keep_alive,
    )
    .await
}
