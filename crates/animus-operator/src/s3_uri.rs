//! A deliberately minimal, syntax-only check of an `s3://...` store URI.
//!
//! **Not** a reimplementation of `animusd::main::parse_s3_uri` (S-04 PR 2,
//! `crates/animusd/src/main.rs`) — this crate does not depend on `animusd`
//! at all (see `crates/animus-operator/CLAUDE.md`'s "does not depend on
//! `animusd`" note), so it cannot call that parser directly, and
//! duplicating its full behavior (query-key allowlist, region default,
//! `path_style` validation, the loopback-vs-`--allow-insecure-s3`
//! cross-check) here would just be a second copy to keep in sync by hand.
//! What this module checks is only what `crate::crd::S3StoreSpec::validate`
//! and `crate::desired::networkpolicy` actually need: that the URI has a
//! bucket name and an `endpoint=` query parameter naming a `http://`/
//! `https://` URL (so an obviously-malformed `spec.s3` value surfaces as a
//! status condition here, before it ever reaches a pod and crash-loops),
//! plus the query's own `insecure_http` flag and the endpoint's port (so
//! the generated `NetworkPolicy` egress rule opens the right port). The
//! real credential/region/loopback logic stays exactly where ADR 0059's
//! amendment put it: node-side, in `animusd`.

/// The handful of facts this operator needs out of an `s3://...` URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct S3UriInfo {
    /// Whether the URI's own query string sets `insecure_http=true`.
    pub insecure_http: bool,
    /// The endpoint's port: an explicit `:PORT` suffix if the host has one,
    /// else 443 for `https://` or 80 for `http://`. Always `Some` once
    /// [`parse`] returns `Ok` — the scheme check that would leave this
    /// `None` already fails parsing.
    pub port: Option<i32>,
}

/// `Ok` iff `uri` has the shape `s3://<bucket>[/prefix][?...&endpoint=
/// scheme://host[:port]&...]` — a non-empty bucket name and a query
/// parameter named `endpoint` whose value starts with `http://` or
/// `https://`. See this module's own doc for what is deliberately *not*
/// checked here (that's `animusd`'s own `parse_s3_uri`'s job, at node
/// startup).
///
/// # Errors
/// A message naming the specific problem (missing `s3://` prefix, empty
/// bucket, missing/malformed `endpoint=`) — never a panic.
pub fn parse(uri: &str) -> Result<S3UriInfo, String> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| format!("{uri:?}: not an s3:// URI"))?;
    let (path_part, query_part) = rest.split_once('?').unwrap_or((rest, ""));
    let bucket = path_part.split('/').next().unwrap_or("");
    if bucket.is_empty() {
        return Err(format!("{uri:?}: s3:// URI needs a bucket name"));
    }

    let mut endpoint: Option<&str> = None;
    let mut insecure_http = false;
    for pair in query_part.split('&').filter(|s| !s.is_empty()) {
        match pair.split_once('=') {
            Some(("endpoint", v)) => endpoint = Some(v),
            Some(("insecure_http", v)) => insecure_http = v == "true",
            _ => {}
        }
    }
    let endpoint = endpoint
        .ok_or_else(|| format!("{uri:?}: s3:// URI requires ?endpoint=scheme://host[:port]"))?;

    let scheme_https = endpoint.starts_with("https://");
    let scheme_http = endpoint.starts_with("http://");
    if !scheme_https && !scheme_http {
        return Err(format!(
            "{uri:?}: endpoint {endpoint:?} must start with http:// or https://"
        ));
    }

    let host_port = endpoint.split_once("://").map_or(endpoint, |(_, hp)| hp);
    // Defensive only: a well-formed endpoint value has no path component,
    // but don't let one confuse the port extraction below if it does.
    let host_port = host_port.split('/').next().unwrap_or(host_port);
    let explicit_port = host_port
        .rsplit_once(':')
        .and_then(|(_, p)| p.parse::<i32>().ok());
    let port = explicit_port.or(Some(if scheme_https { 443 } else { 80 }));

    Ok(S3UriInfo {
        insecure_http,
        port,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_s3_scheme() {
        assert!(parse("http://bucket").is_err());
    }

    #[test]
    fn rejects_empty_bucket() {
        assert!(parse("s3://?endpoint=https://minio:9000").is_err());
        assert!(parse("s3://").is_err());
    }

    #[test]
    fn rejects_missing_endpoint() {
        let err = parse("s3://bucket/prefix").unwrap_err();
        assert!(err.contains("endpoint"), "{err}");
    }

    #[test]
    fn rejects_endpoint_without_scheme() {
        let err = parse("s3://bucket?endpoint=minio:9000").unwrap_err();
        assert!(err.contains("http://"), "{err}");
    }

    #[test]
    fn accepts_bucket_and_prefix_with_https_endpoint_default_port() {
        let info = parse("s3://my-bucket/backups?endpoint=https://s3.example.com&region=us-east-1")
            .unwrap();
        assert!(!info.insecure_http);
        assert_eq!(info.port, Some(443));
    }

    #[test]
    fn accepts_explicit_port() {
        let info =
            parse("s3://bucket?endpoint=http://minio.ns.svc:9000&insecure_http=true").unwrap();
        assert!(info.insecure_http);
        assert_eq!(info.port, Some(9000));
    }

    #[test]
    fn http_endpoint_without_explicit_port_defaults_to_80() {
        let info = parse("s3://bucket?endpoint=http://minio.ns.svc").unwrap();
        assert_eq!(info.port, Some(80));
    }

    #[test]
    fn insecure_http_defaults_to_false_when_absent() {
        let info = parse("s3://bucket?endpoint=https://s3.example.com").unwrap();
        assert!(!info.insecure_http);
    }

    #[test]
    fn query_parameter_order_does_not_matter() {
        let info = parse("s3://bucket?region=us-east-1&insecure_http=true&endpoint=http://h:1234")
            .unwrap();
        assert_eq!(info.port, Some(1234));
        assert!(info.insecure_http);
    }
}
