//! A test-only TCP delay proxy that emulates a WAN link on loopback.
//!
//! It listens on a loopback port and forwards each accepted connection to an
//! upstream address, delaying every chunk by a configured **one-way** delay
//! (plus optional seeded jitter) and optionally capping the forwarded
//! bandwidth. Ordering within a direction is preserved, and a half-close is
//! propagated, so TLS (and anything else) is carried through untouched.
//!
//! **This is an emulation, not a WAN.** It adds latency and a rate cap to
//! loopback; it has no loss, no reordering, no congestion control, no
//! middleboxes, and the delay is applied per chunk read (up to 16 KiB), not
//! per packet. Numbers measured through it must be labelled "emulated WAN on
//! loopback".
//!
//! Real time/sockets (the `ProdEnv` edge): this is test code, so it uses
//! `tokio::spawn`/`tokio::time` directly. `animusd` carries a package-level
//! override of the `disallowed_methods` lint for exactly this class of code
//! (see `crates/animusd/Cargo.toml`). Included from a test with `mod
//! wan_proxy;` (the directory has no `main.rs`, so Cargo does not treat it as
//! a test target of its own); a bench could `#[path]`-include the same file.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

/// The emulated link: one-way delay, jitter and an optional bandwidth cap.
#[derive(Clone, Copy, Debug)]
pub struct WanLink {
    /// Added to every chunk in each direction (so RTT is about `2 * one_way`).
    pub one_way: Duration,
    /// Each chunk's delay is `one_way + U[0, jitter]`, from a seeded stream.
    /// Per-direction ordering is kept (a chunk is never delivered before the
    /// one ahead of it).
    pub jitter: Duration,
    /// Seed of the jitter stream (per connection and direction it is mixed
    /// with a connection counter, so runs with the same seed and the same
    /// connection order repeat).
    pub seed: u64,
    /// Forwarded bytes per second per direction; `None` = uncapped.
    pub bytes_per_sec: Option<u64>,
}

impl WanLink {
    pub fn delay(one_way_ms: u64) -> Self {
        Self {
            one_way: Duration::from_millis(one_way_ms),
            jitter: Duration::ZERO,
            seed: 0,
            bytes_per_sec: None,
        }
    }
}

/// Bytes forwarded so far, by direction (dialer to upstream is `forward`).
#[derive(Default, Debug)]
pub struct WanStats {
    forward: AtomicU64,
    back: AtomicU64,
    conns: AtomicU64,
}

impl WanStats {
    pub fn forward_bytes(&self) -> u64 {
        self.forward.load(Ordering::Relaxed)
    }
    pub fn back_bytes(&self) -> u64 {
        self.back.load(Ordering::Relaxed)
    }
    pub fn total_bytes(&self) -> u64 {
        self.forward_bytes() + self.back_bytes()
    }
    pub fn connections(&self) -> u64 {
        self.conns.load(Ordering::Relaxed)
    }
}

pub struct WanProxy {
    listen: SocketAddr,
    stats: Arc<WanStats>,
    accept: JoinHandle<()>,
}

impl WanProxy {
    /// Listen on an ephemeral loopback port and forward to `upstream`.
    pub async fn start(upstream: SocketAddr, link: WanLink) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let listen = listener.local_addr().expect("proxy addr");
        let stats = Arc::new(WanStats::default());
        let st = Arc::clone(&stats);
        let accept = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let n = st.conns.fetch_add(1, Ordering::Relaxed);
                let st = Arc::clone(&st);
                tokio::spawn(async move {
                    let Ok(up) = TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = client.set_nodelay(true);
                    let _ = up.set_nodelay(true);
                    let (cr, cw) = client.into_split();
                    let (ur, uw) = up.into_split();
                    let s0 = link.seed ^ n.wrapping_mul(0x9E37_79B9_7F4A_7C15);
                    let fwd = tokio::spawn(pump(cr, uw, link, s0, Arc::clone(&st), true));
                    let back = tokio::spawn(pump(ur, cw, link, s0 ^ 1, st, false));
                    let _ = tokio::join!(fwd, back);
                });
            }
        });
        Self {
            listen,
            stats,
            accept,
        }
    }

    /// The address a dialer should use instead of the upstream's.
    pub fn addr(&self) -> SocketAddr {
        self.listen
    }

    pub fn stats(&self) -> &Arc<WanStats> {
        &self.stats
    }
}

impl Drop for WanProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One direction: a reader stamps each chunk with its delivery time, a writer
/// task releases it then (and paces to the bandwidth cap).
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    link: WanLink,
    mut rng: u64,
    stats: Arc<WanStats>,
    forward: bool,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<(Instant, Vec<u8>)>();
    let writer = tokio::spawn(async move {
        let mut link_free = Instant::now();
        while let Some((due, chunk)) = rx.recv().await {
            tokio::time::sleep_until(due.max(link_free)).await;
            if to.write_all(&chunk).await.is_err() {
                return;
            }
            let counter = if forward { &stats.forward } else { &stats.back };
            counter.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            if let Some(bps) = link.bytes_per_sec {
                let secs = chunk.len() as f64 / bps.max(1) as f64;
                link_free = Instant::now() + Duration::from_secs_f64(secs);
            }
        }
        let _ = to.shutdown().await;
    });
    let mut last_due = Instant::now();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => break,
            Ok(n) => n,
        };
        let jitter = if link.jitter.is_zero() {
            Duration::ZERO
        } else {
            let span = link.jitter.as_micros() as u64 + 1;
            Duration::from_micros(splitmix(&mut rng) % span)
        };
        let due = (Instant::now() + link.one_way + jitter).max(last_due);
        last_due = due;
        if tx.send((due, buf[..n].to_vec())).is_err() {
            break;
        }
    }
    drop(tx);
    let _ = writer.await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_round_trip_pays_two_one_way_delays_and_counts_bytes() {
        let echo = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let echo_addr = echo.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut s, _) = echo.accept().await.unwrap();
            let mut b = [0u8; 64];
            let n = s.read(&mut b).await.unwrap();
            s.write_all(&b[..n]).await.unwrap();
        });
        let p = WanProxy::start(echo_addr, WanLink::delay(50)).await;
        let mut c = TcpStream::connect(p.addr()).await.unwrap();
        let t0 = Instant::now();
        c.write_all(b"hello").await.unwrap();
        let mut b = [0u8; 5];
        c.read_exact(&mut b).await.unwrap();
        let rtt = t0.elapsed();
        assert_eq!(&b, b"hello");
        assert!(rtt >= Duration::from_millis(100), "rtt {rtt:?}");
        assert_eq!((p.stats().forward_bytes(), p.stats().back_bytes()), (5, 5));
    }
}
