//! A minimal keep-alive HTTP/1.1 client for the DynamoDB JSON wire, with
//! SigV4 request signing.
//!
//! **Why hand-rolled rather than hyper / the AWS SDK:** the server side
//! speaks a tiny, fixed HTTP subset (`POST /`, `Content-Length` bodies), the
//! workspace already owns the signer ([`animus_dynamo::sigv4::sign`], the
//! same code the server's verifier is tested against), and a generator whose
//! measurements ride on the client's own overhead should keep that overhead
//! small and legible. No new HTTP dependency enters the tree.
//!
//! **TLS (ADR 0064, server-only):** a [`Conn`] runs over `animus-env`'s
//! `MaybeTlsStream` (plain TCP or a rustls client stream; an enum, so the
//! plain path is unchanged and TLS costs one `match` per poll). The TLS
//! handshake is part of [`Conn::connect`] — connection setup — never of a
//! request, so a healthy op's latency contains none of it; only a redial
//! after a broken connection does (as the TCP connect always did). See
//! [`crate::tls`].

use std::collections::BTreeMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use animus_dynamo::sigv4::{self, SigV4Request};
use animus_env::MaybeTlsStream;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

use crate::rt;
use crate::tls::{self, TlsClient};

/// Static SigV4 credentials (ADR 0057's bootstrap credential map).
#[derive(Clone, Debug)]
pub struct Credentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
}

impl Credentials {
    /// Credentials with the conventional `us-east-1` region (the server never
    /// pins region, ADR 0057).
    #[must_use]
    pub fn new(access_key_id: impl Into<String>, secret_access_key: impl Into<String>) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            region: "us-east-1".to_owned(),
        }
    }
}

/// An HTTP response (status + body bytes).
#[derive(Debug)]
pub struct Response {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Response {
    /// The body as lossy UTF-8.
    #[must_use]
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One keep-alive connection to a node's DynamoDB port.
pub struct Conn {
    reader: BufReader<MaybeTlsStream>,
    addr: SocketAddr,
    creds: Option<Arc<Credentials>>,
    broken: bool,
}

impl Conn {
    /// Connect (TCP_NODELAY on), through a server-only TLS handshake when
    /// `tls` is given.
    ///
    /// # Errors
    /// On a connect or TLS handshake failure.
    pub async fn connect(
        addr: SocketAddr,
        creds: Option<Arc<Credentials>>,
        tls: Option<&TlsClient>,
    ) -> io::Result<Self> {
        let stream = tls::dial(addr, tls).await?;
        Ok(Self {
            reader: BufReader::new(stream),
            addr,
            creds,
            broken: false,
        })
    }

    /// The node this connection talks to.
    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// True once any I/O error (or `Connection: close`) has made this
    /// connection unusable; the owner must drop and redial.
    #[must_use]
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// One DynamoDB call: `target` is the operation name (`GetItem`, ...),
    /// `body` the JSON request.
    ///
    /// # Errors
    /// On any socket / framing error (the connection is then marked broken).
    pub async fn call(&mut self, target: &str, body: &str) -> io::Result<Response> {
        let r = self.call_inner(target, body).await;
        if r.is_err() {
            self.broken = true;
        }
        r
    }

    async fn call_inner(&mut self, target: &str, body: &str) -> io::Result<Response> {
        let target = format!("DynamoDB_20120810.{target}");
        let host = self.addr.to_string();
        let mut head = format!(
            "POST / HTTP/1.1\r\nHost: {host}\r\nX-Amz-Target: {target}\r\n\
             Content-Type: application/x-amz-json-1.0\r\nContent-Length: {}\r\n\
             Connection: keep-alive\r\n",
            body.len()
        );
        if let Some(c) = &self.creds {
            let amz_date = amz_date(rt::wall_epoch_secs());
            let mut headers = BTreeMap::new();
            headers.insert("host".to_owned(), host);
            headers.insert("x-amz-date".to_owned(), amz_date.clone());
            headers.insert("x-amz-target".to_owned(), target);
            headers.insert(
                "content-type".to_owned(),
                "application/x-amz-json-1.0".to_owned(),
            );
            let req = SigV4Request {
                method: "POST",
                path: "/",
                query: "",
                headers: &headers,
                body: body.as_bytes(),
            };
            let auth = sigv4::sign(
                &req,
                &c.access_key_id,
                &c.secret_access_key,
                &amz_date,
                &c.region,
                "dynamodb",
                &["content-type", "host", "x-amz-date", "x-amz-target"],
            );
            head.push_str(&format!(
                "X-Amz-Date: {amz_date}\r\nAuthorization: {auth}\r\n"
            ));
        }
        head.push_str("\r\n");
        let stream = self.reader.get_mut();
        // One write for head+body: keeps a request in one segment.
        let mut out = head.into_bytes();
        out.extend_from_slice(body.as_bytes());
        stream.write_all(&out).await?;
        stream.flush().await?;
        let (status, body, close) = read_response(&mut self.reader).await?;
        if close {
            self.broken = true;
        }
        Ok(Response { status, body })
    }
}

/// Read one HTTP/1.x response with a `Content-Length` body. Returns
/// `(status, body, server_will_close)`.
async fn read_response<R: AsyncBufReadExt + Unpin>(r: &mut R) -> io::Result<(u16, Vec<u8>, bool)> {
    let mut line = String::new();
    if r.read_line(&mut line).await? == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "eof before status line",
        ));
    }
    let status: u16 = line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad status line {line:?}"),
            )
        })?;
    let http10 = line.starts_with("HTTP/1.0");
    let mut content_length: Option<usize> = None;
    let mut close = http10;
    loop {
        line.clear();
        if r.read_line(&mut line).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "eof in headers",
            ));
        }
        if line == "\r\n" || line == "\n" {
            break;
        }
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            content_length = v.trim().parse().ok();
        } else if let Some(v) = lower.strip_prefix("connection:") {
            close = v.trim() == "close";
        } else if lower.starts_with("transfer-encoding:") && lower.contains("chunked") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunked response unsupported",
            ));
        }
    }
    let mut body = Vec::new();
    match content_length {
        Some(n) => {
            body.resize(n, 0);
            r.read_exact(&mut body).await?;
        }
        None => {
            // No length: body runs to EOF, so the connection cannot be reused.
            r.read_to_end(&mut body).await?;
            close = true;
        }
    }
    Ok((status, body, close))
}

/// `GET path` against an admin address (HTTP/1.0, one shot, through `tls`
/// when the cluster has it) → `(status, JSON)`. `Value::Null` if the body is
/// not JSON.
///
/// # Errors
/// On a socket or TLS handshake error.
pub async fn admin_get(
    addr: SocketAddr,
    path: &str,
    tls: Option<&TlsClient>,
) -> io::Result<(u16, serde_json::Value)> {
    let mut stream = tls::dial(addr, tls).await?;
    let req = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await?;
    stream.flush().await?;
    let mut raw = Vec::new();
    match stream.read_to_end(&mut raw).await {
        Ok(_) => {}
        // A one-shot `Connection: close` server may drop the socket without
        // a TLS close_notify; the body is delimited by EOF here, and a
        // truncated one fails the header/JSON parse below.
        Err(e) if tls.is_some() && e.kind() == io::ErrorKind::UnexpectedEof => {}
        Err(e) => return Err(e),
    }
    let text = String::from_utf8_lossy(&raw);
    let (head, payload) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no header terminator"))?;
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad status line"))?;
    Ok((
        status,
        serde_json::from_str(payload.trim()).unwrap_or(serde_json::Value::Null),
    ))
}

/// SigV4 `X-Amz-Date` (`YYYYMMDDTHHMMSSZ`) for a Unix timestamp.
#[must_use]
pub fn amz_date(epoch_secs: u64) -> String {
    let days = i64::try_from(epoch_secs / 86_400).unwrap_or(0);
    let rem = epoch_secs % 86_400;
    // Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amz_date_matches_known_instants() {
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_700_000_000), "20231114T221320Z");
        // Leap day.
        assert_eq!(amz_date(1_709_211_896), "20240229T130456Z");
        assert_eq!(amz_date(4_102_444_799), "20991231T235959Z");
    }
}
