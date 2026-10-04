//! A small, tolerant extractor for the handful of XML tags this crate's
//! `ListObjectsV2`/error-body responses need — deliberately **not** a real
//! XML parser and not a new dependency (`docs/roadmap.md`'s S-04 plan asks
//! for exactly this trade-off: "no XML crate is in tree today"). No crate
//! in this workspace's `Cargo.lock` provides XML parsing, and pulling one in
//! for four tag names would be a worse trade than a ~30-line tag scanner
//! that this module's own tests pin down directly.
//!
//! # What this deliberately does not handle
//!
//! - **Nested same-named tags** — `between` finds the first `<tag>...
//!   </tag>` pair by raw substring search, so a tag that legitimately
//!   nests inside itself would mis-pair. None of the tags this module reads
//!   ever do (`Contents`/`Key`/`Size`/`IsTruncated`/`NextContinuationToken`/
//!   `Code`/`Message` are all leaf-or-flat in the real `ListObjectsV2`/
//!   `Error` response shapes).
//! - **XML comments/CDATA/processing instructions** — a real response body
//!   never contains them for these particular elements; not scanned for.
//! - **Attributes** — none of the tags read here carry any in S3's actual
//!   responses.
//!
//! What it *does* handle: entity-unescaping element text (`&amp;`/`&lt;`/
//! `&gt;`/`&quot;`/`&apos;`/`&#NN;`/`&#xHH;`), since an object key can
//! legitimately contain any of the characters those entities encode.

/// One `<Contents>` entry from a `ListObjectsV2` response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
}

/// A parsed `ListBucketResult` body.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListBucketResult {
    pub contents: Vec<ObjectEntry>,
    pub is_truncated: bool,
    pub next_continuation_token: Option<String>,
}

/// An S3 XML error body's `<Code>`/`<Message>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorBody {
    pub code: String,
    pub message: String,
}

/// Parse a `ListObjectsV2` XML response body. Tolerant of a body this
/// module can't fully make sense of (returns an empty, non-truncated
/// result rather than erroring) — a caller only reaches this after already
/// checking the HTTP status was successful, so a genuinely malformed body
/// at that point is a server bug this client has no better response to.
#[must_use]
pub fn parse_list_objects_v2(xml: &str) -> ListBucketResult {
    let contents = between(xml, "Contents")
        .into_iter()
        .filter_map(|block| {
            let key = between(block, "Key").into_iter().next()?;
            let size = between(block, "Size")
                .into_iter()
                .next()
                .and_then(|s| s.trim().parse::<u64>().ok())
                .unwrap_or(0);
            Some(ObjectEntry {
                key: xml_unescape(key),
                size,
            })
        })
        .collect();

    let is_truncated = between(xml, "IsTruncated")
        .into_iter()
        .next()
        .map(|v| v.trim() == "true")
        .unwrap_or(false);
    let next_continuation_token = between(xml, "NextContinuationToken")
        .into_iter()
        .next()
        .map(xml_unescape);

    ListBucketResult {
        contents,
        is_truncated,
        next_continuation_token,
    }
}

/// Parse an S3 `<Error>...</Error>` XML body, if `xml` looks like one.
#[must_use]
pub fn parse_error(xml: &str) -> Option<ErrorBody> {
    let code = between(xml, "Code").into_iter().next()?;
    let message = between(xml, "Message")
        .into_iter()
        .next()
        .unwrap_or_default();
    Some(ErrorBody {
        code: xml_unescape(code),
        message: xml_unescape(message),
    })
}

/// The `<Credentials>` block of an STS `AssumeRoleWithWebIdentityResponse`
/// (S-08 M1). The session token is a secret: this type has no `Debug`.
pub struct StsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    /// `Expiration`, converted to epoch milliseconds.
    pub expiration_epoch_ms: u64,
}

/// Parse an STS `AssumeRoleWithWebIdentity` success body. `None` if any of
/// the four fields is missing or the expiration is not a valid timestamp.
/// (A failure body is an `<ErrorResponse><Error><Code>..</Code><Message>..`
/// document, which [`parse_error`] already reads.)
#[must_use]
pub fn parse_sts_credentials(xml: &str) -> Option<StsCredentials> {
    let block = between(xml, "Credentials").into_iter().next()?;
    let field = |tag: &str| {
        between(block, tag)
            .into_iter()
            .next()
            .map(|v| xml_unescape(v.trim()))
    };
    Some(StsCredentials {
        access_key_id: field("AccessKeyId")?,
        secret_access_key: field("SecretAccessKey")?,
        session_token: field("SessionToken")?,
        expiration_epoch_ms: parse_iso8601_epoch_ms(&field("Expiration")?)?,
    })
}

/// Parse an ISO-8601 / RFC-3339 UTC timestamp (`2019-11-09T13:34:41Z`,
/// optional fractional seconds, `Z` or `+00:00`/`-00:00` offset) to epoch
/// milliseconds. Pure — no clock. `None` on anything else (a non-UTC offset
/// included: AWS credential endpoints always answer in UTC).
#[must_use]
pub fn parse_iso8601_epoch_ms(s: &str) -> Option<u64> {
    let s = s.trim();
    let b = s.as_bytes();
    if b.len() < 20 || b[4] != b'-' || b[7] != b'-' || !(b[10] == b'T' || b[10] == b't') {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if !t.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        t.parse().ok()
    };
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (h, mi, sec) = (num(11..13)?, num(14..16)?, num(17..19)?);
    if b[13] != b':' || b[16] != b':' {
        return None;
    }
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    let mut rest = &s[19..];
    let mut millis: i64 = 0;
    if let Some(frac) = rest.strip_prefix('.') {
        let digits: String = frac.chars().take_while(char::is_ascii_digit).collect();
        if digits.is_empty() {
            return None;
        }
        let first3: String = digits.chars().chain("00".chars()).take(3).collect();
        millis = first3.parse().ok()?;
        rest = &frac[digits.len()..];
    }
    if !matches!(rest, "Z" | "z" | "+00:00" | "-00:00") {
        return None;
    }
    // days_from_civil (Howard Hinnant, public domain).
    let y2 = if mo <= 2 { y - 1 } else { y };
    let era = if y2 >= 0 { y2 } else { y2 - 399 } / 400;
    let yoe = y2 - era * 400;
    let mp = (mo + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let secs = days * 86_400 + h * 3600 + mi * 60 + sec;
    u64::try_from(secs * 1000 + millis).ok()
}

/// Every `<tag>...</tag>` inner-text block in `s`, in order. See this
/// module's own doc for the nesting/comment/CDATA limitations.
fn between<'a>(s: &'a str, tag: &str) -> Vec<&'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut out = Vec::new();
    let mut rest = s;
    while let Some(start) = rest.find(&open) {
        let after_open = &rest[start + open.len()..];
        let Some(end) = after_open.find(&close) else {
            break;
        };
        out.push(&after_open[..end]);
        rest = &after_open[end + close.len()..];
    }
    out
}

/// Unescape the five predefined XML entities plus decimal/hex numeric
/// character references. An entity this function doesn't recognize is left
/// verbatim (tolerant, not a parse error — see the module doc).
fn xml_unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.char_indices().peekable();
    let bytes = s.as_bytes();
    while let Some((i, ch)) = chars.next() {
        if ch != '&' {
            out.push(ch);
            continue;
        }
        let Some(semi_rel) = bytes[i..].iter().position(|&b| b == b';') else {
            out.push(ch);
            continue;
        };
        let entity = &s[i + 1..i + semi_rel];
        let replacement = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ if entity.starts_with("#x") || entity.starts_with("#X") => {
                u32::from_str_radix(&entity[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            _ if entity.starts_with('#') => {
                entity[1..].parse::<u32>().ok().and_then(char::from_u32)
            }
            _ => None,
        };
        match replacement {
            Some(c) => {
                out.push(c);
                // Skip past the entity's remaining chars (already consumed
                // `&`; advance the iterator past `entity;`).
                for _ in 0..semi_rel {
                    chars.next();
                }
            }
            None => out.push(ch),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_two_page_list_objects_v2_body() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>my-bucket</Name>
  <Prefix>backup/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>abc123==</NextContinuationToken>
  <Contents>
    <Key>backup/manifest.json</Key>
    <LastModified>2026-09-05T00:00:00.000Z</LastModified>
    <ETag>"deadbeef"</ETag>
    <Size>42</Size>
    <StorageClass>STANDARD</StorageClass>
  </Contents>
  <Contents>
    <Key>backup/chunk-0000</Key>
    <Size>1048576</Size>
  </Contents>
</ListBucketResult>"#;
        let parsed = parse_list_objects_v2(xml);
        assert!(parsed.is_truncated);
        assert_eq!(parsed.next_continuation_token.as_deref(), Some("abc123=="));
        assert_eq!(
            parsed.contents,
            vec![
                ObjectEntry {
                    key: "backup/manifest.json".to_string(),
                    size: 42
                },
                ObjectEntry {
                    key: "backup/chunk-0000".to_string(),
                    size: 1_048_576
                },
            ]
        );
    }

    #[test]
    fn parses_a_final_untruncated_page_with_no_continuation_token() {
        let xml = r#"<ListBucketResult><IsTruncated>false</IsTruncated>
          <Contents><Key>a</Key><Size>1</Size></Contents></ListBucketResult>"#;
        let parsed = parse_list_objects_v2(xml);
        assert!(!parsed.is_truncated);
        assert_eq!(parsed.next_continuation_token, None);
        assert_eq!(parsed.contents.len(), 1);
    }

    #[test]
    fn parses_an_empty_bucket_page() {
        let xml = "<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>";
        let parsed = parse_list_objects_v2(xml);
        assert!(!parsed.is_truncated);
        assert!(parsed.contents.is_empty());
    }

    #[test]
    fn unescapes_entities_in_a_key() {
        let xml = "<ListBucketResult><Contents><Key>a&amp;b &lt;c&gt; \"d\" &#39;e&#39; \
                   &#x2603;</Key><Size>0</Size></Contents></ListBucketResult>";
        let parsed = parse_list_objects_v2(xml);
        assert_eq!(parsed.contents[0].key, "a&b <c> \"d\" 'e' \u{2603}");
    }

    #[test]
    fn parses_a_no_such_key_error_body() {
        let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchKey</Code>
  <Message>The specified key does not exist.</Message>
  <Key>missing.txt</Key>
  <RequestId>ABC123</RequestId>
</Error>"#;
        let parsed = parse_error(xml).expect("parses");
        assert_eq!(parsed.code, "NoSuchKey");
        assert_eq!(parsed.message, "The specified key does not exist.");
    }

    #[test]
    fn parse_error_returns_none_for_a_non_error_body() {
        assert_eq!(parse_error("<ListBucketResult></ListBucketResult>"), None);
    }

    #[test]
    fn parses_sts_credentials() {
        let body = "<AssumeRoleWithWebIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
<AssumeRoleWithWebIdentityResult><Credentials><AccessKeyId>ASIAEXAMPLE</AccessKeyId>\
<SecretAccessKey>sec/ret+x</SecretAccessKey><SessionToken>tok&amp;en</SessionToken>\
<Expiration>2019-11-09T13:34:41Z</Expiration></Credentials></AssumeRoleWithWebIdentityResult>\
</AssumeRoleWithWebIdentityResponse>";
        let c = parse_sts_credentials(body).expect("parses");
        assert_eq!(c.access_key_id, "ASIAEXAMPLE");
        assert_eq!(c.secret_access_key, "sec/ret+x");
        assert_eq!(c.session_token, "tok&en");
        assert_eq!(c.expiration_epoch_ms, 1_573_306_481_000);
    }

    #[test]
    fn sts_credentials_missing_field_is_none() {
        assert!(
            parse_sts_credentials("<Credentials><AccessKeyId>a</AccessKeyId></Credentials>")
                .is_none()
        );
        assert!(parse_sts_credentials("<nope/>").is_none());
    }

    #[test]
    fn parses_an_sts_error_response() {
        let body = "<ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\"><Error>\
<Type>Sender</Type><Code>InvalidIdentityToken</Code><Message>Token expired</Message></Error>\
<RequestId>abc</RequestId></ErrorResponse>";
        let e = parse_error(body).expect("error");
        assert_eq!(e.code, "InvalidIdentityToken");
        assert_eq!(e.message, "Token expired");
    }

    #[test]
    fn iso8601_parsing() {
        assert_eq!(parse_iso8601_epoch_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(
            parse_iso8601_epoch_ms("2019-11-09T13:34:41Z"),
            Some(1_573_306_481_000)
        );
        assert_eq!(
            parse_iso8601_epoch_ms("2019-11-09T13:34:41.5Z"),
            Some(1_573_306_481_500)
        );
        assert_eq!(
            parse_iso8601_epoch_ms("2019-11-09T13:34:41.123456Z"),
            Some(1_573_306_481_123)
        );
        assert_eq!(
            parse_iso8601_epoch_ms("2019-11-09T13:34:41+00:00"),
            Some(1_573_306_481_000)
        );
        // Leap day.
        assert_eq!(
            parse_iso8601_epoch_ms("2024-02-29T00:00:00Z"),
            Some(1_709_164_800_000)
        );
        assert_eq!(parse_iso8601_epoch_ms("2019-11-09T13:34:41+02:00"), None);
        assert_eq!(parse_iso8601_epoch_ms("garbage"), None);
        assert_eq!(parse_iso8601_epoch_ms("2019-13-09T13:34:41Z"), None);
        assert_eq!(parse_iso8601_epoch_ms("2019-11-09 13:34:41Z"), None);
    }
}
