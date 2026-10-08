//! Server-only TLS for the DynamoDB and admin dials (ADR 0064; ADR 0076
//! as-built note), modelled on `animus-cli`'s `--tls-ca PATH`.
//!
//! The generator verifies the node it dials against the given CA but never
//! presents a client certificate (it is not a cluster member; mutual TLS is
//! only for the internal/intra ports). The stream type is `animus-env`'s
//! [`MaybeTlsStream`] (a two-variant enum, not a boxed trait object, so the
//! plain-TCP path pays nothing and the TLS path pays one `match` per poll).
//!
//! **The handshake happens in [`TlsClient::wrap`], i.e. at connection
//! setup** (`Conn::connect`, which the engine calls before a phase starts —
//! see `Cluster::prewarm` — and again only to redial a broken connection).
//! It is never part of a healthy request's measured latency.

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use animus_env::MaybeTlsStream;
use rustls_pki_types::ServerName;
use rustls_pki_types::pem::PemObject;
use tokio::net::TcpStream;

use crate::rt;

/// Bound on one TLS handshake (a stalled peer must not hang a dial).
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// A server-only TLS client configuration: trusted CA(s) plus how to name
/// the server for certificate verification.
#[derive(Clone)]
pub struct TlsClient {
    connector: tokio_rustls::TlsConnector,
    /// `None`: derive the name from the dialled address (an IP address for
    /// every `SocketAddr` endpoint, so the node cert needs an IP SAN).
    server_name: Option<ServerName<'static>>,
    /// What to record in the report.
    server_name_note: String,
}

impl std::fmt::Debug for TlsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsClient")
            .field("server_name", &self.server_name_note)
            .finish_non_exhaustive()
    }
}

impl TlsClient {
    /// Trust the certificates in the PEM file `ca_path`. `server_name`
    /// (`--tls-server-name`) overrides the name every node is verified
    /// against (a DNS name or an IP literal).
    ///
    /// # Errors
    /// A message if the file cannot be read or holds no certificate, the
    /// name is invalid, or `rustls` rejects the root store.
    pub fn from_ca_file(ca_path: &Path, server_name: Option<&str>) -> Result<Self, String> {
        let bytes = std::fs::read(ca_path)
            .map_err(|e| format!("reading --tls-ca {}: {e}", ca_path.display()))?;
        Self::from_ca_pem(&bytes, server_name)
            .map_err(|e| format!("--tls-ca {}: {e}", ca_path.display()))
    }

    /// As [`Self::from_ca_file`], from PEM bytes.
    ///
    /// # Errors
    /// See [`Self::from_ca_file`].
    pub fn from_ca_pem(pem: &[u8], server_name: Option<&str>) -> Result<Self, String> {
        let certs = rustls_pki_types::CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("parsing PEM: {e}"))?;
        let mut roots = rustls::RootCertStore::empty();
        for c in certs {
            roots.add(c).map_err(|e| e.to_string())?;
        }
        if roots.is_empty() {
            return Err("no certificates found".to_owned());
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let config = rustls::ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| format!("building TLS client config: {e}"))?
            .with_root_certificates(roots)
            .with_no_client_auth();
        let (server_name, note) = match server_name {
            None => (None, "the dialled node IP address".to_owned()),
            Some(n) => (
                Some(
                    ServerName::try_from(n.to_owned())
                        .map_err(|e| format!("invalid --tls-server-name `{n}`: {e}"))?,
                ),
                format!("`{n}` (--tls-server-name)"),
            ),
        };
        Ok(Self {
            connector: tokio_rustls::TlsConnector::from(Arc::new(config)),
            server_name,
            server_name_note: note,
        })
    }

    /// What every node certificate is verified against, for the report.
    #[must_use]
    pub fn server_name_note(&self) -> &str {
        &self.server_name_note
    }

    /// Run the server-only TLS handshake over `stream` (dialled to `addr`).
    ///
    /// # Errors
    /// On a handshake failure (untrusted cert, name mismatch, ...) or if it
    /// exceeds [`HANDSHAKE_TIMEOUT`].
    pub async fn wrap(&self, addr: SocketAddr, stream: TcpStream) -> io::Result<MaybeTlsStream> {
        let name = match &self.server_name {
            Some(n) => n.clone(),
            None => animus_env::tls::server_name_for(&addr.to_string())?,
        };
        let tls = rt::timeout(HANDSHAKE_TIMEOUT, self.connector.connect(name, stream))
            .await
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("TLS handshake with {addr} timed out"),
                )
            })?
            .map_err(|e| io::Error::new(e.kind(), format!("TLS handshake with {addr}: {e}")))?;
        Ok(MaybeTlsStream::Tls(Box::new(tls.into())))
    }
}

/// Dial `addr` (TCP_NODELAY on), through `tls` when given.
///
/// # Errors
/// On a connect or handshake failure.
pub async fn dial(addr: SocketAddr, tls: Option<&TlsClient>) -> io::Result<MaybeTlsStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    match tls {
        None => Ok(MaybeTlsStream::Plain(stream)),
        Some(t) => t.wrap(addr, stream).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ca_pem() -> String {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        let mut p = CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        p.self_signed(&KeyPair::generate().unwrap()).unwrap().pem()
    }

    #[test]
    fn a_ca_file_without_certificates_is_rejected() {
        let e = TlsClient::from_ca_pem(b"not pem at all", None).unwrap_err();
        assert!(e.contains("no certificates"), "{e}");
    }

    #[test]
    fn a_missing_ca_file_names_the_path() {
        let e = TlsClient::from_ca_file(Path::new("/nonexistent/ca.pem"), None).unwrap_err();
        assert!(e.contains("/nonexistent/ca.pem"), "{e}");
    }

    #[test]
    fn server_name_override_is_validated_and_noted() {
        let pem = ca_pem();
        let c = TlsClient::from_ca_pem(pem.as_bytes(), None).unwrap();
        assert!(c.server_name_note().contains("IP address"));
        let c = TlsClient::from_ca_pem(pem.as_bytes(), Some("bench.internal")).unwrap();
        assert!(c.server_name_note().contains("bench.internal"));
        assert!(TlsClient::from_ca_pem(pem.as_bytes(), Some("not a name!")).is_err());
    }
}
