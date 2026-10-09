//! A minimal HTTP/1.1 client for the DynamoDB wire and the admin port.
//! One fresh connection per request (`Connection: close`), like the other
//! `animusd` integration tests.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Why a call produced no response. The distinction is load-bearing for the
/// history: `Connect` means no byte was ever sent (the op definitely did not
/// happen); `Io`/`Timeout` mean it may have.
#[derive(Debug, Clone)]
pub enum CallErr {
    Connect(String),
    Io(String),
    Timeout,
}

impl std::fmt::Display for CallErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CallErr::Connect(e) => write!(f, "connect failed: {e}"),
            CallErr::Io(e) => write!(f, "io error: {e}"),
            CallErr::Timeout => write!(f, "timed out"),
        }
    }
}

fn parse(raw: &[u8]) -> Result<(u16, String), CallErr> {
    let text = String::from_utf8_lossy(raw).into_owned();
    let Some((head, payload)) = text.split_once("\r\n\r\n") else {
        return Err(CallErr::Io("truncated response".into()));
    };
    let status = head
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .ok_or_else(|| CallErr::Io("bad status line".into()))?;
    Ok((status, payload.to_string()))
}

async fn request(
    addr: SocketAddr,
    head: String,
    body: &str,
    tmo: Duration,
) -> Result<(u16, String), CallErr> {
    let mut stream = match timeout(Duration::from_secs(2), TcpStream::connect(addr)).await {
        Ok(Ok(s)) => s,
        Ok(Err(e)) => return Err(CallErr::Connect(e.to_string())),
        Err(_) => return Err(CallErr::Connect("connect timeout".into())),
    };
    let req = format!(
        "{head}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let io = async {
        stream
            .write_all(req.as_bytes())
            .await
            .map_err(|e| CallErr::Io(e.to_string()))?;
        let mut raw = Vec::new();
        stream
            .read_to_end(&mut raw)
            .await
            .map_err(|e| CallErr::Io(e.to_string()))?;
        parse(&raw)
    };
    match timeout(tmo, io).await {
        Ok(r) => r,
        Err(_) => Err(CallErr::Timeout),
    }
}

/// One admin-port `POST` with a JSON body.
pub async fn http_post(
    addr: SocketAddr,
    path: &str,
    body: &str,
    tmo: Duration,
) -> Result<(u16, String), CallErr> {
    let head =
        format!("POST {path} HTTP/1.1\r\nHost: animus\r\nContent-Type: application/json\r\n");
    request(addr, head, body, tmo).await
}

/// One DynamoDB request.
pub async fn dynamo_call(
    addr: SocketAddr,
    target: &str,
    body: &str,
    tmo: Duration,
) -> Result<(u16, String), CallErr> {
    let head = format!(
        "POST / HTTP/1.1\r\nHost: animus\r\nX-Amz-Target: DynamoDB_20120810.{target}\r\n\
         Content-Type: application/x-amz-json-1.0\r\n"
    );
    request(addr, head, body, tmo).await
}

/// One admin-port `GET`.
pub async fn http_get(
    addr: SocketAddr,
    path: &str,
    tmo: Duration,
) -> Result<(u16, String), CallErr> {
    let head = format!("GET {path} HTTP/1.1\r\nHost: animus\r\n");
    request(addr, head, "", tmo).await
}
