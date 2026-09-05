//! TLS material for [`ProdEnv`](crate::ProdEnv)'s intra-node wire (ADR 0064,
//! S-01 step 1) — config-gated, default off, plain TCP unchanged when
//! unconfigured.
//!
//! [`TlsConfig`] names three PEM files on disk (the same file-based shape
//! `--dynamo-auth` already uses elsewhere in this codebase); [`TlsConfig::
//! load`] reads them once at startup and builds the [`rustls`]
//! `ServerConfig`/`ClientConfig` pair a node needs to speak **mutual** TLS on
//! the internal Raft wire: every node presents a cert signed by the
//! cluster's own CA and verifies its peer's cert against that same CA. This
//! is the only mode this module builds today — the internal wire has no
//! server-only variant, so `ca_path` is required here; a server-only
//! (client-verifies-node-only) mode for the client/admin/console ports is
//! commit 2's job, layered onto this same [`TlsConfig`] shape (see ADR 0064).
//!
//! [`MaybeTlsStream`] is the transport wrapper every accept/dial path in
//! `prod.rs` moves through: a thin enum over a plain [`TcpStream`] or a
//! [`tokio_rustls::TlsStream`], chosen over boxing (this is the per-frame hot
//! path — Raft heartbeats included) and over a Cargo feature (a feature
//! would bifurcate the crate's own build, not just gate an optional
//! capability). It lives here, not folded into `prod.rs`'s network code,
//! specifically so `animusd` can reuse the exact same type for its own
//! client/intra-`ClientRequest`/admin/console listeners in commit 2 without
//! reimplementing the wrapper.
//!
//! ## Certificate SAN requirement
//!
//! A node's certificate must carry a Subject Alternative Name matching every
//! string another node's peer book might dial it by: its bind address's IP
//! *and*, if it is ever registered by hostname (a Kubernetes pod's stable
//! DNS name, ADR 0060's advertise/dial split), that DNS name too. TLS
//! hostname verification happens against whatever string the peer book
//! holds ([`server_name_for`] derives the `ServerName` from exactly that
//! string), so a cert missing the SAN the caller actually dials by fails
//! the handshake — this is a deployment/cert-issuance concern (documented
//! here and in ADR 0064), not something this module can paper over.

use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// Where a node's TLS material lives on disk, and whether the internal wire
/// speaks TLS at all. File-based like `--dynamo-auth` — no inline PEM in
/// config/CLI args — and default off: a node with no [`TlsConfig`] is
/// byte-for-byte the plain-TCP `ProdEnv` this crate already had.
///
/// `ca_path` is **required** for [`TlsConfig::load`] today (the internal
/// wire is mutual-TLS-only, see this module's doc) even though the field
/// itself is `Option` — the option exists because a future server-only mode
/// (the client/admin/console ports, commit 2) has no peer cert to verify
/// against a CA at all. A cluster is either all-TLS or all-plain on the
/// internal wire; validating that a whole cluster's config agrees is a
/// startup-time concern for the config layer that consumes this type
/// (`animusd`, commit 2), not this crate.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// PEM file: this node's own certificate (leaf, optionally followed by
    /// intermediates — the full chain `rustls_pemfile::certs` finds).
    pub cert_path: PathBuf,
    /// PEM file: this node's private key (PKCS#8, PKCS#1, or SEC1 — whatever
    /// `rustls_pemfile::private_key` recognizes).
    pub key_path: PathBuf,
    /// PEM file: the cluster CA certificate(s) trusted for both verifying an
    /// inbound peer's client cert and this node's own outbound dials.
    pub ca_path: Option<PathBuf>,
}

/// The loaded, ready-to-use TLS acceptor/connector pair for one node —
/// [`TlsConfig::load`]'s output. Cheap to clone (both fields are themselves
/// `Arc`-backed, per `tokio_rustls`'s own design), so one [`TlsMaterial`] is
/// built once at bind time and shared across every accept/dial this env
/// performs.
#[derive(Clone)]
pub struct TlsMaterial {
    /// Wraps an inbound [`TcpStream`] as a TLS server, requiring (and
    /// verifying) the peer's client certificate against the cluster CA.
    pub acceptor: tokio_rustls::TlsAcceptor,
    /// Wraps an outbound [`TcpStream`] as a TLS client, presenting this
    /// node's own certificate and verifying the peer's server certificate
    /// against the same CA.
    pub connector: tokio_rustls::TlsConnector,
}

/// One end of the intra-node wire: either a plain [`TcpStream`] (TLS
/// unconfigured — the default, byte-for-byte the same transport this crate
/// always had) or a [`tokio_rustls::TlsStream`] (client- or server-initiated
/// — `tokio_rustls`'s own `TlsStream` already unifies both directions, so
/// this enum need not distinguish them any further).
///
/// Chosen over `Box<dyn AsyncRead + AsyncWrite>` because this sits on the
/// hot path every frame (including every Raft heartbeat) moves through, and
/// over a Cargo feature because a feature would bifurcate this crate's own
/// build rather than gate one optional capability behind a runtime config
/// (see this module's doc). Lives here rather than in `prod.rs` so
/// `animusd` can reuse it verbatim for its own client/intra-`ClientRequest`/
/// admin/console listeners in commit 2.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

impl TlsConfig {
    /// Read this config's PEM files once and build the mutual-TLS
    /// [`TlsMaterial`] the internal wire needs: a `ServerConfig` that
    /// requires and verifies a peer's client certificate against `ca_path`,
    /// and a `ClientConfig` that presents this node's own certificate and
    /// verifies the peer's server certificate against the same CA.
    ///
    /// # Errors
    /// Returns an error if `ca_path` is absent (mutual TLS has no meaning
    /// without a trust anchor to verify the peer against — see this
    /// module's doc for why the internal wire has no server-only mode), if
    /// any PEM file cannot be read or contains no usable cert/key, or if
    /// `rustls` rejects the resulting material (e.g. an unparseable key).
    pub fn load(&self) -> io::Result<TlsMaterial> {
        let Some(ca_path) = self.ca_path.as_deref() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS ca_path is required for the internal wire (mutual TLS) — see ADR 0064",
            ));
        };

        let provider = Arc::new(rustls::crypto::ring::default_provider());

        // Two independent `RootCertStore`s (one per config side) rather
        // than sharing one via `Clone` — `WebPkiClientVerifier::
        // builder_with_provider` consumes an `Arc<RootCertStore>` and
        // `with_root_certificates` consumes an owned one, and re-reading
        // one small CA PEM file twice at startup is simpler than routing a
        // shared store through both call shapes.
        let server_verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(root_cert_store(ca_path)?),
            provider.clone(),
        )
        .build()
        .map_err(to_io_error)?;

        let server_certs = load_certs(&self.cert_path)?;
        let server_key = load_private_key(&self.key_path)?;
        let server_config = rustls::ServerConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(to_io_error)?
            .with_client_cert_verifier(server_verifier)
            .with_single_cert(server_certs, server_key)
            .map_err(to_io_error)?;

        let client_certs = load_certs(&self.cert_path)?;
        let client_key = load_private_key(&self.key_path)?;
        let client_config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(to_io_error)?
            .with_root_certificates(root_cert_store(ca_path)?)
            .with_client_auth_cert(client_certs, client_key)
            .map_err(to_io_error)?;

        Ok(TlsMaterial {
            acceptor: tokio_rustls::TlsAcceptor::from(Arc::new(server_config)),
            connector: tokio_rustls::TlsConnector::from(Arc::new(client_config)),
        })
    }
}

fn root_cert_store(ca_path: &Path) -> io::Result<rustls::RootCertStore> {
    let mut store = rustls::RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        store.add(cert).map_err(to_io_error)?;
    }
    if store.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in CA file {}", ca_path.display()),
        ));
    }
    Ok(store)
}

fn load_certs(path: &Path) -> io::Result<Vec<CertificateDer<'static>>> {
    let bytes = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;
    let certs = rustls_pemfile::certs(&mut bytes.as_slice())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("parsing certs in {}: {e}", path.display()),
            )
        })?;
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates found in {}", path.display()),
        ));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> io::Result<PrivateKeyDer<'static>> {
    let bytes = std::fs::read(path)
        .map_err(|e| io::Error::new(e.kind(), format!("reading {}: {e}", path.display())))?;
    rustls_pemfile::private_key(&mut bytes.as_slice())
        .map_err(|e| io::Error::new(e.kind(), format!("parsing key in {}: {e}", path.display())))?
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no private key found in {}", path.display()),
            )
        })
}

fn to_io_error<E: std::fmt::Display>(err: E) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

/// Derive the [`ServerName`] a TLS client presents (and verifies the peer's
/// certificate against) for one dial address, from exactly the string
/// `ProdEnv`'s peer book holds for that peer (`host:port`, per the
/// advertise/dial split — a numeric address or a hostname).
///
/// A numeric address (with or without a port) becomes `ServerName::
/// IpAddress`; anything else is treated as a DNS name. This must agree with
/// how the peer's own certificate names itself: **a node's cert SAN must
/// cover every string its peers might dial it by** (see this module's doc).
pub(crate) fn server_name_for(addr: &str) -> io::Result<ServerName<'static>> {
    let invalid = |host: &str| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("cannot derive a TLS server name from {host:?}"),
        )
    };
    if let Ok(socket_addr) = addr.parse::<SocketAddr>() {
        return Ok(ServerName::IpAddress(socket_addr.ip().into()));
    }
    let host = addr.rsplit_once(':').map_or(addr, |(host, _port)| host);
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }
    ServerName::try_from(host.to_string()).map_err(|_| invalid(host))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_name_for_numeric_v4_with_port_is_an_ip_address() {
        let name = server_name_for("127.0.0.1:4000").expect("numeric v4 must parse");
        assert!(matches!(name, ServerName::IpAddress(_)));
    }

    #[test]
    fn server_name_for_hostname_with_port_is_a_dns_name() {
        let name = server_name_for("node-a.internal.svc:4000").expect("hostname must parse");
        assert!(matches!(name, ServerName::DnsName(_)));
    }

    #[test]
    fn server_name_for_bare_hostname_no_port_is_a_dns_name() {
        let name = server_name_for("localhost").expect("bare hostname must parse");
        assert!(matches!(name, ServerName::DnsName(_)));
    }
}
