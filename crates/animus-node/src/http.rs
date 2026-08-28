//! The **pure** half of `animusd`'s hand-rolled HTTP/1.1 helpers (ADR 0061
//! rung C4a): header-block splitting into a decoded request line/headers/
//! derived fields, `Content-Length` validation, header lowercasing/
//! comma-joining (ADR 0057), `query_param`/`percent_decode`, and response
//! formatting from `(status, content_type, body, keep_alive, extra_headers)`.
//! No socket type crosses this boundary — `animusd`'s `read_http_request`/
//! `write_response_with` keep the actual `TcpStream` reads/writes and call
//! straight into [`parse_request_head`]/[`format_response`] for everything
//! that doesn't touch a socket. Mirrors [`crate::codec`] (C3a)'s split
//! exactly: same crate, same "no socket type, pure bytes/strings in and
//! out" discipline.

use std::collections::BTreeMap;
use std::io;

/// Max request size (headers + body) an edge will buffer before erroring.
/// Moved verbatim from `animusd::http` (rung C4a).
pub const MAX_BODY: usize = 1 << 20;

/// A parsed HTTP request — every field `animusd`'s wire edges
/// (`dynamo.rs`/`admin.rs`/`console.rs`) read, now built here so
/// [`crate::sigv4_gate`] and `animus-node`'s own moved `console::route`
/// (rung C4c) can consume it with no crate-boundary re-projection.
pub struct HttpRequest {
    /// The request method (`GET`, `POST`, …).
    pub method: String,
    /// The request path, with any `?query` stripped off (see [`query_param`]).
    pub path: String,
    /// The raw query string after `?` (empty if none). Parse with
    /// [`query_param`].
    pub query: String,
    /// The `X-Amz-Target` header value (used only by the DynamoDB edge; empty
    /// otherwise). A derived convenience over [`Self::headers`] — kept as its
    /// own field since every caller reads it, unlike an arbitrary
    /// `SignedHeaders` member.
    pub target: String,
    /// Every header on the request (ADR 0057): **lowercased** names →
    /// values, trimmed the same way [`Self::target`]/[`Self::keep_alive`]
    /// already were. A repeated header's values are comma-joined in the
    /// order received — the SigV4 canonical-header form
    /// (`animus_dynamo::sigv4::SigV4Request::headers`'s own contract) — so
    /// this map can be handed straight to `sigv4::verify` with no further
    /// massaging. `BTreeMap`, never `HashMap` (ADR 0003 determinism rule,
    /// lint-enforced).
    pub headers: BTreeMap<String, String>,
    /// The request body bytes.
    pub body: Vec<u8>,
    /// Whether the client wants the connection kept alive.
    pub keep_alive: bool,
}

/// The pure result of parsing one request's header block (everything up to
/// and including the terminating CRLFCRLF, as text) — [`parse_request_head`]'s
/// return value. Missing only `body`: the caller (`animusd`, still holding
/// the socket) reads exactly `content_length` more bytes and assembles the
/// final [`HttpRequest`].
#[derive(Debug)]
pub struct ParsedRequestHead {
    pub method: String,
    pub path: String,
    pub query: String,
    pub target: String,
    pub headers: BTreeMap<String, String>,
    pub keep_alive: bool,
    pub content_length: usize,
}

/// Parse one request's header text (everything before the terminating blank
/// line, CRLF-separated lines, the trailing CRLFCRLF itself excluded or
/// included — either way, an empty trailing split segment is harmless) into
/// a [`ParsedRequestHead`]. Pure: no socket, no I/O, just string parsing —
/// moved verbatim out of `animusd`'s `read_http_request` (rung C4a).
///
/// # Errors
/// An unparseable `Content-Length` value, or one exceeding [`MAX_BODY`],
/// is a hard error — the caller (`animusd`) treats both identically to a
/// malformed request today (connection closed with an `InvalidData` error).
pub fn parse_request_head(header_text: &str) -> io::Result<ParsedRequestHead> {
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
    let mut headers: BTreeMap<String, String> = BTreeMap::new();
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
            // Every header, not just the three special-cased above (ADR
            // 0057) — a repeated header's values comma-join in the order
            // received, the SigV4 canonical form.
            headers
                .entry(name)
                .and_modify(|existing| {
                    existing.push_str(", ");
                    existing.push_str(value);
                })
                .or_insert_with(|| value.to_owned());
        }
    }
    if content_length > MAX_BODY {
        return Err(eof("request body too large"));
    }
    Ok(ParsedRequestHead {
        method,
        path,
        query,
        target,
        headers,
        keep_alive,
        content_length,
    })
}

/// Look up `name` in a raw `a=1&b=2` query string, returning its (un-decoded)
/// value. Only the minimal `%NN` and `+` decoding the admin edge needs is applied.
pub fn query_param(query: &str, name: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let (k, v) = pair.split_once('=')?;
        (k == name).then(|| percent_decode(v))
    })
}

/// Minimal `application/x-www-form-urlencoded` value decoding: `+` → space and
/// `%NN` → byte. Invalid escapes are passed through verbatim. `pub` (not
/// module-private) since `console.rs`'s per-table path routing also needs
/// it, for a table/index name that needed `encodeURIComponent` on the way in.
pub fn percent_decode(s: &str) -> String {
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

/// Build an `InvalidData` I/O error carrying `msg` — the one error shape
/// every parsing failure in this module (and `animusd`'s own socket-reading
/// wrappers) uses.
pub fn eof(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Find the first occurrence of `needle` in `haystack`, or `None`. Used by
/// `animusd`'s own streaming header-block read loop (`buf` may hold a
/// partial or pipelined request) to find the terminating CRLFCRLF — the one
/// piece of this module still called from inside a `stream.read()` loop,
/// since it operates on a buffer that grows across multiple reads.
pub fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// CORS header lines for the admin edge (ADR 0021). The web dashboard is served
/// from one node's admin port but fans out to *every* node's `/admin/*` JSON, so
/// those cross-origin reads need a permissive `Access-Control-Allow-Origin`.
/// Scoped to the admin listener only — the dynamo data edge never sends it;
/// the admin port is assumed bound to a trusted interface (ADR 0020, no auth yet).
/// Each line ends with CRLF so it can be spliced straight into the header block.
pub const CORS_HEADERS: &str = "Access-Control-Allow-Origin: *\r\n\
     Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n\
     Access-Control-Allow-Headers: Content-Type\r\n";

/// The reason phrase for a status code (the small set our edges emit).
pub fn reason(status: u16) -> &'static str {
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

/// Format a minimal HTTP/1.1 response with the given `content_type`/`body`,
/// splicing `extra_headers` (a block of complete CRLF-terminated header
/// lines, e.g. [`CORS_HEADERS`], or `""` for none) in before the blank line.
/// The `Connection` header echoes the client's keep-alive choice so a
/// `Connection: close` client (which then reads to EOF) is unblocked by the
/// socket closing. Pure string formatting — `animusd`'s `write_response_with`
/// writes the result to a socket and does nothing else.
pub fn format_response(
    status: u16,
    content_type: &str,
    body: &str,
    keep_alive: bool,
    extra_headers: &str,
) -> String {
    let connection = if keep_alive { "keep-alive" } else { "close" };
    format!(
        "HTTP/1.1 {status} {reason}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Connection: {connection}\r\n\
         {extra_headers}\
         \r\n\
         {body}",
        reason = reason(status),
        len = body.len(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn head(text: &str) -> io::Result<ParsedRequestHead> {
        parse_request_head(text)
    }

    #[test]
    fn parses_a_simple_get() {
        let h =
            head("GET /admin/config HTTP/1.1\r\nHost: x\r\n").expect("well-formed request parses");
        assert_eq!(h.method, "GET");
        assert_eq!(h.path, "/admin/config");
        assert_eq!(h.query, "");
        assert!(h.keep_alive, "HTTP/1.1 defaults to keep-alive");
        assert_eq!(h.content_length, 0);
    }

    #[test]
    fn splits_path_and_query() {
        let h = head("GET /admin/storage/scan?tablet=5&limit=10 HTTP/1.1\r\n").unwrap();
        assert_eq!(h.path, "/admin/storage/scan");
        assert_eq!(h.query, "tablet=5&limit=10");
    }

    #[test]
    fn http_1_0_defaults_to_close_but_keep_alive_header_overrides() {
        let h10 = head("GET / HTTP/1.0\r\n").unwrap();
        assert!(!h10.keep_alive);
        let h10_ka = head("GET / HTTP/1.0\r\nConnection: keep-alive\r\n").unwrap();
        assert!(h10_ka.keep_alive);
        let h11_close = head("GET / HTTP/1.1\r\nConnection: close\r\n").unwrap();
        assert!(!h11_close.keep_alive);
    }

    #[test]
    fn malformed_request_line_yields_empty_method_and_path_rather_than_panicking() {
        let h = head("\r\nHost: x\r\n").expect("must not panic on a blank first line");
        assert_eq!(h.method, "");
        assert_eq!(h.path, "");
    }

    #[test]
    fn missing_content_length_defaults_to_zero() {
        let h = head("POST /x HTTP/1.1\r\n").unwrap();
        assert_eq!(h.content_length, 0);
    }

    #[test]
    fn invalid_content_length_is_an_error() {
        let err = head("POST /x HTTP/1.1\r\nContent-Length: not-a-number\r\n")
            .expect_err("a non-numeric Content-Length must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("invalid Content-Length"));
    }

    #[test]
    fn oversized_content_length_is_rejected_before_any_body_read() {
        let too_big = MAX_BODY + 1;
        let err = head(&format!(
            "POST /x HTTP/1.1\r\nContent-Length: {too_big}\r\n"
        ))
        .expect_err("a declared length over MAX_BODY must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("too large"));
    }

    #[test]
    fn content_length_exactly_at_max_body_is_accepted() {
        let h = head(&format!(
            "POST /x HTTP/1.1\r\nContent-Length: {MAX_BODY}\r\n"
        ))
        .expect("exactly at the bound is legal");
        assert_eq!(h.content_length, MAX_BODY);
    }

    #[test]
    fn duplicate_headers_comma_join_in_receipt_order() {
        let h = head("GET / HTTP/1.1\r\nX-Foo: a\r\nX-Foo: b\r\nX-Foo: c\r\n").unwrap();
        assert_eq!(h.headers.get("x-foo").map(String::as_str), Some("a, b, c"));
    }

    #[test]
    fn header_names_are_lowercased_and_values_trimmed() {
        let h = head("GET / HTTP/1.1\r\nX-Amz-Date:   20260101T000000Z  \r\n").unwrap();
        assert_eq!(
            h.headers.get("x-amz-date").map(String::as_str),
            Some("20260101T000000Z")
        );
    }

    #[test]
    fn x_amz_target_is_lifted_into_its_own_field() {
        let h = head("POST / HTTP/1.1\r\nX-Amz-Target: DynamoDB_20120810.GetItem\r\n").unwrap();
        assert_eq!(h.target, "DynamoDB_20120810.GetItem");
        // Still present in the generic map too, lowercased.
        assert_eq!(
            h.headers.get("x-amz-target").map(String::as_str),
            Some("DynamoDB_20120810.GetItem")
        );
    }

    #[test]
    fn query_param_looks_up_by_name() {
        assert_eq!(query_param("a=1&b=2", "b"), Some("2".to_string()));
        assert_eq!(query_param("a=1&b=2", "c"), None);
    }

    #[test]
    fn query_param_percent_decodes_the_value() {
        assert_eq!(
            query_param("name=hello%20world", "name"),
            Some("hello world".to_string())
        );
    }

    #[test]
    fn percent_decode_handles_plus_and_percent_escapes() {
        assert_eq!(percent_decode("a+b"), "a b");
        assert_eq!(percent_decode("100%25"), "100%");
        assert_eq!(percent_decode("%2F"), "/");
    }

    #[test]
    fn percent_decode_passes_through_an_invalid_escape_verbatim() {
        // Not enough trailing bytes / non-hex digits: the `%` and the raw
        // bytes that follow are passed through rather than panicking or
        // silently eating input.
        assert_eq!(percent_decode("100%2"), "100%2");
        assert_eq!(percent_decode("100%zz"), "100%zz");
    }

    #[test]
    fn find_subslice_locates_the_terminator() {
        let buf = b"GET / HTTP/1.1\r\nHost: x\r\n\r\nbody-bytes";
        let pos = find_subslice(buf, b"\r\n\r\n").expect("terminator present");
        assert_eq!(&buf[pos + 4..], b"body-bytes");
        assert_eq!(find_subslice(b"no terminator here", b"\r\n\r\n"), None);
    }

    #[test]
    fn format_response_round_trips_status_and_body() {
        let body = "hello";
        let rendered = format_response(200, "text/plain", body, true, "");
        assert!(rendered.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(rendered.contains("Content-Type: text/plain\r\n"));
        assert!(rendered.contains(&format!("Content-Length: {}\r\n", body.len())));
        assert!(rendered.contains("Connection: keep-alive\r\n"));
        assert!(rendered.ends_with(body));
    }

    #[test]
    fn format_response_close_connection_and_extra_headers() {
        let rendered = format_response(204, "text/plain", "", false, CORS_HEADERS);
        assert!(rendered.contains("Connection: close\r\n"));
        assert!(rendered.contains("Access-Control-Allow-Origin: *\r\n"));
    }

    #[test]
    fn reason_covers_every_status_this_module_emits() {
        assert_eq!(reason(200), "OK");
        assert_eq!(reason(404), "Not Found");
        assert_eq!(reason(999), "Status");
    }
}
