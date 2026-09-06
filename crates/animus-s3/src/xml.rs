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
}
