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

/// The virtual-hosted (`path_style=false`) preconditions `animusd`'s own
/// `parse_s3_uri` enforces (via `animus_s3::client::validate_virtual_hosted`),
/// duplicated here because this crate does not depend on `animus-s3`: a DNS
/// endpoint host (not an IP) and a DNS-compatible bucket name (3-63 chars of
/// `[a-z0-9-]`, alphanumeric at both ends). The test
/// `virtual_hosted_rules_match_animusd` pins the shared cases.
fn validate_virtual_hosted(host_port: &str, bucket: &str) -> Result<(), String> {
    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host_port.rsplit_once(':').map_or(host_port, |(h, _)| h)
    };
    if host_port.starts_with('[') || host.parse::<std::net::IpAddr>().is_ok() {
        return Err(format!(
            "virtual-hosted addressing needs a DNS endpoint, but {host_port:?} is an IP address"
        ));
    }
    let ok_chars = bucket
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    let ends_ok = bucket
        .bytes()
        .next()
        .zip(bucket.bytes().last())
        .is_some_and(|(f, l)| f.is_ascii_alphanumeric() && l.is_ascii_alphanumeric());
    if !(3..=63).contains(&bucket.len()) || !ok_chars || !ends_ok {
        return Err(format!(
            "bucket name {bucket:?} is not DNS-compatible for virtual-hosted addressing"
        ));
    }
    Ok(())
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
    let mut virtual_hosted = false;
    for pair in query_part.split('&').filter(|s| !s.is_empty()) {
        match pair.split_once('=') {
            Some(("endpoint", v)) => endpoint = Some(v),
            Some(("insecure_http", v)) => insecure_http = v == "true",
            Some(("path_style", "true")) => virtual_hosted = false,
            Some(("path_style", "false")) => virtual_hosted = true,
            Some(("path_style", other)) => {
                return Err(format!(
                    "{uri:?}: path_style={other:?} must be `true` (path-style, the default) or \
                     `false` (virtual-hosted)"
                ));
            }
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
    if virtual_hosted {
        validate_virtual_hosted(host_port, bucket)
            .map_err(|e| format!("{uri:?}: path_style=false: {e}"))?;
    }
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
    fn path_style_accepts_true_false_and_rejects_garbage() {
        assert!(parse("s3://my-bucket?endpoint=https://s3.example.com&path_style=true").is_ok());
        assert!(parse("s3://my-bucket?endpoint=https://s3.example.com&path_style=false").is_ok());
        let err = parse("s3://my-bucket?endpoint=https://s3.example.com&path_style=x").unwrap_err();
        assert!(err.contains("path_style"), "{err}");
    }

    #[test]
    fn virtual_hosted_rules_match_animusd() {
        for bad in [
            "s3://my-bucket?endpoint=http://127.0.0.1:9000&insecure_http=true&path_style=false",
            "s3://my-bucket?endpoint=https://10.0.0.5&path_style=false",
            "s3://My_Bucket?endpoint=https://s3.example.com&path_style=false",
            "s3://ab?endpoint=https://s3.example.com&path_style=false",
            "s3://has.dot?endpoint=https://s3.example.com&path_style=false",
        ] {
            assert!(parse(bad).is_err(), "{bad}");
        }
        // The same inputs are fine path-style (the default).
        assert!(parse("s3://My_Bucket?endpoint=https://s3.example.com").is_ok());
        assert!(parse("s3://my-bucket?endpoint=http://127.0.0.1:9000&insecure_http=true").is_ok());
    }

    #[test]
    fn query_parameter_order_does_not_matter() {
        let info = parse("s3://bucket?region=us-east-1&insecure_http=true&endpoint=http://h:1234")
            .unwrap();
        assert_eq!(info.port, Some(1234));
        assert!(info.insecure_http);
    }
}
