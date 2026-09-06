//! AnimusDB's S3 client (S-04 PR 1, `docs/roadmap.md` §2; design amendment:
//! `docs/adr/0059-backup-restore.md`'s 2026-09-06 "S-04: S3 `SegmentStore`
//! backend" section). See `CLAUDE.md` for the crate's purpose, feature
//! flags, and testing story.
//!
//! Two independently useful halves:
//!
//! - [`sigv4`] — a **pure** AWS Signature Version 4 request signer (no I/O,
//!   no clock, no `Env`). Usable on its own.
//! - [`client`] — [`client::S3Client<T>`], a minimal `put`/`get`/`delete`/
//!   `head`/`list_objects_v2` client generic over an explicit
//!   [`client::Transport`] seam, so it needs no real socket to test.
//!
//! [`fake`] (feature `fake`, also compiled under `#[cfg(test)]`) is an
//! in-memory S3 double implementing [`client::Transport`] directly, with
//! real SigV4 signature verification. [`prod`] (feature `prod`, default
//! off) is the one real-socket/TLS `Transport` implementor.

pub mod client;
pub mod sigv4;
mod xml;

#[cfg(any(test, feature = "fake"))]
pub mod fake;

#[cfg(feature = "prod")]
pub mod prod;

/// The `host[:port]` portion of an `S3Config::endpoint` (`scheme://host` or
/// `scheme://host:port`, no trailing slash) — used both for the `Host`
/// header/SigV4 signing (`client.rs`) and to pick the dial target
/// (`prod.rs`, when that feature is on). `pub(crate)` since it is purely an
/// internal wiring detail between this crate's own modules.
pub(crate) fn endpoint_host(endpoint: &str) -> String {
    let without_scheme = endpoint
        .split_once("://")
        .map_or(endpoint, |(_, rest)| rest);
    without_scheme.trim_end_matches('/').to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_host_strips_scheme_and_trailing_slash() {
        assert_eq!(
            endpoint_host("https://s3.amazonaws.com"),
            "s3.amazonaws.com"
        );
        assert_eq!(endpoint_host("http://127.0.0.1:9000/"), "127.0.0.1:9000");
        assert_eq!(endpoint_host("127.0.0.1:9000"), "127.0.0.1:9000");
    }
}
