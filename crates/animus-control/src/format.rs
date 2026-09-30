//! Shared tagged-envelope convention for every on-disk format this crate
//! resets under ADR 0073 Phase 0 (`docs/adr/0073-upgrade-compatibility.md`,
//! Phase 0 workstream B) — and the convention Workstream C
//! (`animus-cp-data`) reuses for its own `SharedWal` outer envelope
//! (`SWL1`, wrapping the tagged `Line{tablet, record}` shape) rather than
//! independently inventing a second "envelope wraps an inner payload"
//! scheme for the same problem.
//!
//! Two shapes, both following the ADR's "Version tag shape" conventions:
//!
//! - **A binary envelope** ([`wrap`]/[`unwrap`]): `magic(4) || version(u8)
//!   || payload`, the shape already proven by the LSM manifest (`CMF1`) and
//!   the encryption envelope (`ADE1`) — for a self-contained blob with no
//!   internal line framing of its own (e.g. a snapshot/`InstallSnapshot`
//!   envelope).
//! - **A newline-delimited, checksummed line** ([`encode_line`]/
//!   [`decode_lines`]): `<crc32 as 8 lowercase hex chars>:<magic(4 ASCII)>
//!   <version as 2 lowercase hex digits><payload>\n`, the CRC covering
//!   everything after the colon (magic + version + payload) — for an
//!   append-only file whose records are read back one at a time
//!   (`persist.rs`'s `CONTROL_WAL`).
//!
//! **The hex-rendered version in the line shape is the one deliberate
//! deviation from a raw `u8` byte, and it is still a `u8` — only its
//! on-the-wire rendering differs.** A raw version byte can, in principle,
//! equal `\n` itself (version 10 = `0x0A`), which would corrupt this
//! format's own line delimiter the moment a format ever reached that
//! version. Rendering it as two lowercase hex digits keeps the *value*
//! space at `0..=255` like every other format tag in this codebase while
//! making that collision structurally impossible: every hex digit is ASCII
//! `[0-9a-f]`, none of which is `\n`. [`wrap`]/[`unwrap`] has no line
//! delimiter to protect, so it keeps the ADR's plain raw-byte shape.
//!
//! Pure and generic (ADR 0003): no `Env`, no I/O, no `HashMap`.

use std::fmt;

/// A format's identity: a 4-byte ASCII magic, the version an encoder using
/// this tag currently writes, and a name used only in error
/// messages/fixture directory names — never encoded on disk or the wire.
/// `Copy`, `Debug`, const-constructible: every format using this module
/// defines one `pub const FormatTag` (e.g. [`crate::persist::CONTROL_WAL`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FormatTag {
    /// 4-byte ASCII magic, unique across every format tag in this codebase
    /// (see ADR 0073's "Suggested magics" table when picking a new one).
    pub magic: [u8; 4],
    /// The version an encoder using this tag currently writes. A decoder
    /// accepts `1..=version`. `0` is never a valid version — it means "no
    /// version was ever written here," exactly the pre-tag shape this
    /// convention exists to stop recurring.
    pub version: u8,
    /// A short, stable name for this format, used only in error messages —
    /// never encoded on disk or the wire.
    pub name: &'static str,
}

/// A version-tagged format failed to decode. One shared enum across every
/// format in this codebase (ADR 0073 Phase 0 conventions), not one per
/// format — the loud, named-`Err`-never-silent-misdecode discipline the ADR
/// generalizes from the RaftKV/segment codecs' own pre-existing pattern.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FormatError {
    /// No recognized magic (or, for a `serde_json` format elsewhere in this
    /// codebase using the ADR's `"v"`-field convention instead of this
    /// module's binary/line shapes, no `"v"` field): pre-baseline data, or
    /// garbage. Nothing written before the ADR 0073 Phase 0 reset is owed
    /// compatibility, but silently misreading it — instead of refusing it
    /// by name — is exactly the failure mode this convention exists to
    /// close.
    PreBaselineFormat {
        /// The format's [`FormatTag::name`].
        format: &'static str,
    },
    /// The right magic, but a version this decoder doesn't know: either `0`
    /// (never a valid version) or greater than the decoding build's own
    /// `FormatTag::version` (a future binary's format, read by an older
    /// one).
    UnsupportedFormatVersion {
        /// The format's [`FormatTag::name`].
        format: &'static str,
        /// The version byte actually found.
        found: u8,
        /// The highest version this build's decoder accepts.
        max_supported: u8,
    },
    /// The right magic, a supported version, and (for the line shape) a
    /// valid checksum, but the payload itself doesn't decode (e.g.
    /// malformed JSON). Distinct from a torn tail (never an `Err` here —
    /// see [`decode_lines`]'s own doc) and from [`PreBaselineFormat`]/
    /// [`UnsupportedFormatVersion`]: this is real corruption inside an
    /// otherwise well-framed record (or a decoder/encoder bug), and is
    /// always loud, never silently dropped.
    Malformed {
        /// The format's [`FormatTag::name`].
        format: &'static str,
        /// A human-readable description of what went wrong (e.g. the
        /// underlying `serde_json` error's own `Display`).
        detail: String,
    },
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::PreBaselineFormat { format } => write!(
                f,
                "{format}: no recognized format tag (pre-baseline data, or corrupt)"
            ),
            FormatError::UnsupportedFormatVersion {
                format,
                found,
                max_supported,
            } => write!(
                f,
                "{format}: unsupported format version {found} (this build supports up to {max_supported})"
            ),
            FormatError::Malformed { format, detail } => {
                write!(f, "{format}: malformed record: {detail}")
            }
        }
    }
}

impl std::error::Error for FormatError {}

/// The named error for a version byte a dispatching decoder has no arm for
/// (ADR 0073 Phase 1 "upgrade on read"): the `found => Err(..)` arm of a
/// `match version { 1 => .., found => format::unsupported_version(tag, found) }`.
/// Never a panic. `max_supported` is `tag.version`.
#[must_use]
pub fn unsupported_version(tag: &FormatTag, found: u8) -> FormatError {
    FormatError::UnsupportedFormatVersion {
        format: tag.name,
        found,
        max_supported: tag.version,
    }
}

/// Encode `payload` as a binary envelope: `magic(4) || version(u8) ||
/// payload`. Pairs with [`unwrap`].
#[must_use]
pub fn wrap(tag: &FormatTag, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&tag.magic);
    out.push(tag.version);
    out.extend_from_slice(payload);
    out
}

/// Decode a [`wrap`]-produced envelope, returning `(version, payload)`.
/// `Err(FormatError::PreBaselineFormat)` if `bytes` is too short to hold a
/// full `magic + version` header or the magic doesn't match;
/// `Err(FormatError::UnsupportedFormatVersion)` for a version of `0` or
/// greater than `tag.version`. Never panics on any input.
pub fn unwrap<'a>(tag: &FormatTag, bytes: &'a [u8]) -> Result<(u8, &'a [u8]), FormatError> {
    if bytes.len() < 5 || bytes[..4] != tag.magic[..] {
        return Err(FormatError::PreBaselineFormat { format: tag.name });
    }
    let version = bytes[4];
    if version == 0 || version > tag.version {
        return Err(FormatError::UnsupportedFormatVersion {
            format: tag.name,
            found: version,
            max_supported: tag.version,
        });
    }
    Ok((version, &bytes[5..]))
}

/// Frame `payload` as one newline-delimited, checksummed WAL line:
/// `<crc32 as 8 lowercase hex chars>:<magic(4)><version as 2 lowercase hex
/// digits><payload>\n`. The CRC (`crc32fast::hash`, the same crate/impl
/// `animus-storage`'s own checksummed WAL/SSTable framing uses) covers
/// everything after the colon — magic, version, and payload alike. See the
/// module doc for why the version rides as two hex digits here specifically
/// (never for [`wrap`]/[`unwrap`], which has no line delimiter to protect).
/// Pairs with [`decode_lines`].
#[must_use]
pub fn encode_line(tag: &FormatTag, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(6 + payload.len());
    body.extend_from_slice(&tag.magic);
    body.extend_from_slice(format!("{:02x}", tag.version).as_bytes());
    body.extend_from_slice(payload);
    let crc = crc32fast::hash(&body);
    let mut line = Vec::with_capacity(9 + body.len());
    line.extend_from_slice(format!("{crc:08x}:").as_bytes());
    line.extend_from_slice(&body);
    line.push(b'\n');
    line
}

/// Decode a buffer of [`encode_line`]-framed lines back into `(version,
/// payload)` pairs, in file order.
///
/// - Empty lines are skipped.
/// - A line whose CRC **framing** itself fails to check out — no `:`
///   separator, a non-8-hex-digit prefix, a CRC mismatch, or (once the CRC
///   *does* match) too few bytes to even hold a full `magic + version`
///   header — **stops decoding silently**, returning every line collected
///   so far, never an `Err`. This is the torn-tail/at-rest-corruption
///   tolerance issue #495 established for `persist.rs`'s own WAL,
///   generalized here: a write torn by a crash can only ever be the
///   buffer's physical tail, so everything decoded before it is unaffected
///   and safe to trust.
/// - A CRC-valid line whose magic doesn't match `tag.magic` is
///   `Err(FormatError::PreBaselineFormat)` — exactly what a pre-baseline,
///   untagged line (a bare `<crc32>:<json>` record, the format this line
///   shape replaces) looks like to a post-baseline decoder.
/// - A CRC-valid, correctly-tagged line whose version is `0` or greater
///   than `tag.version` is `Err(FormatError::UnsupportedFormatVersion)`.
///
/// Never panics on any input.
pub fn decode_lines<'a>(
    tag: &FormatTag,
    bytes: &'a [u8],
) -> Result<Vec<(u8, &'a [u8])>, FormatError> {
    let mut out = Vec::new();
    for line in bytes.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let Some(colon) = line.iter().position(|&b| b == b':') else {
            break;
        };
        let (hex, rest) = line.split_at(colon);
        if hex.len() != 8 {
            break;
        }
        let Ok(hex_str) = std::str::from_utf8(hex) else {
            break;
        };
        let Ok(expected_crc) = u32::from_str_radix(hex_str, 16) else {
            break;
        };
        let body = &rest[1..];
        if crc32fast::hash(body) != expected_crc {
            break;
        }
        // CRC-valid from here on: every remaining decision is about real
        // content (an unrecognized tag, an unsupported version), never a
        // torn tail — except a body too short to even hold a magic+version
        // header, which (an adversarial CRC collision aside) can't
        // legitimately arise except by the same physical torn-write
        // process, so it's treated identically: a silent stop.
        if body.len() < 6 {
            break;
        }
        if body[..4] != tag.magic[..] {
            return Err(FormatError::PreBaselineFormat { format: tag.name });
        }
        let Some(version) = std::str::from_utf8(&body[4..6])
            .ok()
            .and_then(|s| u8::from_str_radix(s, 16).ok())
        else {
            return Err(FormatError::Malformed {
                format: tag.name,
                detail: format!("version field {:?} is not valid hex", &body[4..6]),
            });
        };
        if version == 0 || version > tag.version {
            return Err(FormatError::UnsupportedFormatVersion {
                format: tag.name,
                found: version,
                max_supported: tag.version,
            });
        }
        out.push((version, &body[6..]));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TAG: FormatTag = FormatTag {
        magic: *b"TST1",
        version: 1,
        name: "test-format",
    };

    #[test]
    fn wrap_unwrap_round_trips() {
        let payload = b"hello world";
        let bytes = wrap(&TAG, payload);
        let (version, decoded) = unwrap(&TAG, &bytes).expect("decodes");
        assert_eq!(version, 1);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn unwrap_rejects_too_short_input() {
        let err = unwrap(&TAG, b"TST").unwrap_err();
        assert_eq!(
            err,
            FormatError::PreBaselineFormat {
                format: "test-format"
            }
        );
    }

    #[test]
    fn unwrap_rejects_wrong_magic() {
        let mut bytes = wrap(&TAG, b"payload");
        bytes[0] = b'X';
        let err = unwrap(&TAG, &bytes).unwrap_err();
        assert_eq!(
            err,
            FormatError::PreBaselineFormat {
                format: "test-format"
            }
        );
    }

    #[test]
    fn unwrap_rejects_version_zero() {
        let mut bytes = wrap(&TAG, b"payload");
        bytes[4] = 0;
        let err = unwrap(&TAG, &bytes).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: "test-format",
                found: 0,
                max_supported: 1,
            }
        );
    }

    #[test]
    fn unwrap_rejects_a_future_version() {
        let mut bytes = wrap(&TAG, b"payload");
        bytes[4] = 2; // this build's decoder only knows up to TAG.version == 1
        let err = unwrap(&TAG, &bytes).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: "test-format",
                found: 2,
                max_supported: 1,
            }
        );
    }

    #[test]
    fn line_round_trips() {
        let payload = b"{\"hello\":\"world\"}";
        let line = encode_line(&TAG, payload);
        let decoded = decode_lines(&TAG, &line).expect("decodes");
        assert_eq!(decoded, vec![(1u8, payload.as_slice())]);
    }

    #[test]
    fn decode_lines_skips_blank_lines() {
        let mut bytes = encode_line(&TAG, b"a");
        bytes.push(b'\n'); // a stray blank line between two real ones
        bytes.extend(encode_line(&TAG, b"b"));
        let decoded = decode_lines(&TAG, &bytes).expect("decodes");
        assert_eq!(
            decoded,
            vec![(1u8, b"a".as_slice()), (1u8, b"b".as_slice())]
        );
    }

    /// Every truncation point of a trailing, torn third line — short of
    /// omitting only its own trailing `\n` — must decode the first two
    /// complete lines and never panic: the torn-tail tolerance a crash
    /// mid-append relies on.
    #[test]
    fn decode_lines_tolerates_every_truncation_of_a_torn_tail() {
        let mut bytes = encode_line(&TAG, b"one");
        bytes.extend(encode_line(&TAG, b"two"));
        let third = encode_line(&TAG, b"three");
        // `..third.len() - 1`, not `..=third.len()`: cutting only the very
        // last byte (the trailing `\n`) leaves the checksum-covered body
        // fully intact and syntactically complete, so it decodes as a
        // legitimate third line rather than a torn one — see the next test.
        for cut in 0..(third.len() - 1) {
            let mut buf = bytes.clone();
            buf.extend_from_slice(&third[..cut]);
            let decoded = decode_lines(&TAG, &buf).expect("a torn tail is never an Err");
            assert_eq!(
                decoded,
                vec![(1u8, b"one".as_slice()), (1u8, b"two".as_slice())],
                "cut at {cut} of {} (torn line length)",
                third.len()
            );
        }
    }

    /// A crash landing exactly after a line's checksummed body but before
    /// its trailing `\n` produces a body that is, by itself, syntactically
    /// complete and checksum-valid — indistinguishable from a line that was
    /// fully written but not yet fsynced. This decodes as a real line, not
    /// a torn one, which is safe regardless: an un-fsynced write was never
    /// acknowledged either way (durable-before-visible, ADR 0009), so
    /// whether it's recovered or dropped, no acknowledged data is at risk.
    /// This is pre-existing framing behavior (`split(b'\n')`'s own last-
    /// segment-needs-no-trailing-delimiter shape), not new to this module.
    #[test]
    fn decode_lines_treats_a_missing_final_newline_as_a_complete_line() {
        let mut bytes = encode_line(&TAG, b"one");
        bytes.extend(encode_line(&TAG, b"two"));
        let third = encode_line(&TAG, b"three");
        bytes.extend_from_slice(&third[..third.len() - 1]);
        let decoded = decode_lines(&TAG, &bytes).expect("a complete-but-unterminated body decodes");
        assert_eq!(
            decoded,
            vec![
                (1u8, b"one".as_slice()),
                (1u8, b"two".as_slice()),
                (1u8, b"three".as_slice()),
            ]
        );
    }

    #[test]
    fn decode_lines_stops_at_crc_corruption() {
        let mut bytes = encode_line(&TAG, b"one");
        bytes.extend(encode_line(&TAG, b"two"));
        // Flip the last content byte before the trailing newline — always
        // inside the second line's checksummed body, whatever its exact
        // framing widths are.
        let flip_at = bytes.len() - 2;
        bytes[flip_at] ^= 0xFF;
        let decoded = decode_lines(&TAG, &bytes).expect("corruption is a stop, not an Err");
        assert_eq!(decoded, vec![(1u8, b"one".as_slice())]);
    }

    /// What a pre-Phase-0 line (`<crc32>:<json>`, no magic/version at all)
    /// looks like to a post-baseline decoder: CRC-valid, but its "magic"
    /// bytes are just the start of a JSON object.
    #[test]
    fn decode_lines_rejects_a_pre_baseline_line() {
        let payload = br#"{"Hard":{"term":1,"voted_for":null}}"#;
        let crc = crc32fast::hash(payload);
        let mut line = format!("{crc:08x}:").into_bytes();
        line.extend_from_slice(payload);
        line.push(b'\n');
        let err = decode_lines(&TAG, &line).unwrap_err();
        assert_eq!(
            err,
            FormatError::PreBaselineFormat {
                format: "test-format"
            }
        );
    }

    #[test]
    fn decode_lines_rejects_unsupported_version() {
        let future_tag = FormatTag {
            magic: TAG.magic,
            version: 2,
            name: TAG.name,
        };
        let future_line = encode_line(&future_tag, b"payload");
        let err = decode_lines(&TAG, &future_line).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: "test-format",
                found: 2,
                max_supported: 1,
            }
        );
    }
}
