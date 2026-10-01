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
    /// A line-framed WAL's line failed its checksum (or was otherwise
    /// unframed) at byte `offset`, **but a valid sync marker proves every
    /// byte before `durable_to` had been fsynced** (see [`decode_lines`]'s
    /// "Mid-file corruption" section) and `offset < durable_to`. A crash
    /// can only ever damage the un-synced tail, so this is damage to
    /// already-durable, possibly acknowledged history (at-rest corruption,
    /// or a disk that acked an `fsync` it then lost) — never a torn tail.
    /// Silently truncating here would drop acked Raft term/vote/log
    /// history, so it is always loud.
    MidFileCorruption {
        /// The format's [`FormatTag::name`].
        format: &'static str,
        /// Byte offset of the first bad line.
        offset: u64,
        /// Offset claimed by the greatest valid sync marker (the marker's
        /// own start): every byte before it was durable when it was written.
        durable_to: u64,
    },
    /// A CRC-valid sync marker whose claimed offset is not the byte offset
    /// the marker line actually starts at. A correct writer cannot produce
    /// this (it appends the marker at exactly the length it records, and
    /// every file rewrite regenerates the file from records, dropping
    /// markers), so it means the file was spliced, truncated from the
    /// front, or had a marker copied across offsets: the marker's durability
    /// claim can no longer be trusted, so decoding refuses by name.
    BadSyncMarker {
        /// The format's [`FormatTag::name`].
        format: &'static str,
        /// Byte offset the marker line actually starts at.
        offset: u64,
        /// The offset the marker claims.
        claimed: u64,
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
            FormatError::MidFileCorruption {
                format,
                offset,
                durable_to,
            } => write!(
                f,
                "{format}: corrupt record at byte offset {offset}, before the \
                 durable boundary {durable_to} proven by a later valid sync \
                 marker: not a torn tail - refusing to silently drop durable history"
            ),
            FormatError::BadSyncMarker {
                format,
                offset,
                claimed,
            } => write!(
                f,
                "{format}: sync marker at byte offset {offset} claims offset \
                 {claimed}: marker does not sit where it says (spliced or damaged file)"
            ),
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

/// The first version whose writers emit sync markers
/// ([`encode_sync_marker`]); lines of an older version never carry one.
pub const SYNC_MARKER_MIN_VERSION: u8 = 2;

/// Payload prefix of a sync-marker line. Every real record payload is
/// `serde_json` of an object/enum (`{` / `"`), so `!` can never begin one.
const SYNC_MARKER_PREFIX: &[u8] = b"!sync:";

/// Frame a **sync marker**: an ordinary [`encode_line`] line (so it is
/// CRC-checked like any other, and carries `tag.version`) whose payload is
/// `!sync:<durable_to>`. `durable_to` must be the file's length at the
/// moment the marker is appended — i.e. the marker's own start offset — and
/// the marker must be appended only **after** an `fsync` that returned `Ok`
/// and covered every byte before it. See [`decode_lines`].
///
/// # Panics
/// Never; a `tag.version` below [`SYNC_MARKER_MIN_VERSION`] simply produces a
/// marker no decoder will recognise (callers only use v2+ tags).
#[must_use]
pub fn encode_sync_marker(tag: &FormatTag, durable_to: u64) -> Vec<u8> {
    encode_line(tag, format!("!sync:{durable_to}").as_bytes())
}

/// A [`decode_lines`] result plus how much of the buffer is clean.
#[derive(Debug)]
pub struct DecodedLines<'a> {
    /// Every record line, in file order, as `(version, payload)`. Sync
    /// markers are consumed by the decoder and never appear here.
    pub lines: Vec<(u8, &'a [u8])>,
    /// Length of the clean prefix: the offset of the first bad line (a
    /// tolerated torn tail), or `bytes.len()` when every line was good. A
    /// writer reopening the file must cut it back to this length (see
    /// [`repaired_image`]) before appending, or its next write would sit
    /// after garbage.
    pub valid_len: usize,
}

/// The bytes a writer must `replace` the file with before appending to it
/// again, or `None` when the file is already clean. A torn/corrupt tail is
/// cut off at [`DecodedLines::valid_len`]; a final valid line that merely
/// lacks its `\n` (a crash between a line's body and its terminator) gets one,
/// so the next append starts on a fresh line instead of fusing with it.
///
/// This is load-bearing for the mid-file rule, not just tidiness: leaving
/// torn garbage in place and appending after it would put a bad line *before*
/// later valid sync markers, and the next recovery would (correctly!) refuse
/// the file as mid-file corruption.
#[must_use]
pub fn repaired_image(bytes: &[u8], valid_len: usize) -> Option<Vec<u8>> {
    let mut clean = bytes[..valid_len].to_vec();
    if clean.last().is_some_and(|&b| b != b'\n') {
        clean.push(b'\n');
    }
    (clean != bytes).then_some(clean)
}

/// CRC-check one line (no trailing `\n`): `Some(body)` (magic + version +
/// payload, at least 6 bytes) when it is framed correctly, else `None`.
fn frame_body(line: &[u8]) -> Option<&[u8]> {
    let colon = line.iter().position(|&b| b == b':')?;
    let (hex, rest) = line.split_at(colon);
    if hex.len() != 8 {
        return None;
    }
    let expected_crc = u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
    let body = &rest[1..];
    if crc32fast::hash(body) != expected_crc {
        return None;
    }
    // CRC-valid, but too short to hold magic + version: can only arise by
    // the same physical torn-write process, so it counts as unframed.
    (body.len() >= 6).then_some(body)
}

/// The version of a CRC-valid body, or the same named errors
/// [`decode_lines`] has always returned for it.
fn body_version(tag: &FormatTag, body: &[u8]) -> Result<u8, FormatError> {
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
    Ok(version)
}

/// If `payload` is a sync marker, its claimed offset. `Err` for a marker
/// whose number does not parse (CRC-valid, so real damage or an encoder bug).
fn parse_marker(tag: &FormatTag, payload: &[u8]) -> Result<Option<u64>, FormatError> {
    let Some(num) = payload.strip_prefix(SYNC_MARKER_PREFIX) else {
        return Ok(None);
    };
    std::str::from_utf8(num)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(Some)
        .ok_or_else(|| FormatError::Malformed {
            format: tag.name,
            detail: format!("sync marker offset {num:?} is not a decimal u64"),
        })
}

/// Decode a buffer of [`encode_line`]-framed lines back into `(version,
/// payload)` pairs, in file order. Thin wrapper over
/// [`decode_lines_extent`] for callers that do not repair the file.
pub fn decode_lines<'a>(
    tag: &FormatTag,
    bytes: &'a [u8],
) -> Result<Vec<(u8, &'a [u8])>, FormatError> {
    decode_lines_extent(tag, bytes).map(|d| d.lines)
}

/// Decode a buffer of [`encode_line`]-framed lines.
///
/// - Empty lines are skipped.
/// - A line whose CRC **framing** itself fails to check out — no `:`
///   separator, a non-8-hex-digit prefix, a CRC mismatch, or (once the CRC
///   *does* match) too few bytes to even hold a full `magic + version`
///   header — is a **bad line**. Records are returned only up to the first
///   bad line; whether that is tolerated is decided below.
/// - A CRC-valid line whose magic doesn't match `tag.magic` is
///   `Err(FormatError::PreBaselineFormat)` — exactly what a pre-baseline,
///   untagged line (a bare `<crc32>:<json>` record, the format this line
///   shape replaces) looks like to a post-baseline decoder.
/// - A CRC-valid, correctly-tagged line whose version is `0` or greater
///   than `tag.version` is `Err(FormatError::UnsupportedFormatVersion)`.
///   (Only for lines reached *before* the first bad line, unchanged.)
///
/// # Mid-file corruption (version 2+; the sync-marker rule)
///
/// A crash tears only the file's un-synced tail, but that tail can hold
/// **several complete lines** (a persist round appends N records and syncs
/// once), and `animus-sim`'s `corrupt_on_crash` flips a byte anywhere in the
/// kept part of it. So "bad line followed by a valid line" is *not* proof of
/// mid-file corruption here — measured at 72 of 300 crash seeds with a
/// correct writer. (`animus-storage`'s LSM WAL resync proof is sound only
/// when at most one frame can be un-synced.) The decoder therefore needs a
/// positional proof it cannot forge: the **sync marker**. After every `fsync`
/// that returns `Ok`, the writer appends a marker line `!sync:<N>` where `N`
/// is the file length at that moment == the marker's own start offset. A
/// valid marker at offset `M` proves every byte `< M` was durable, hence
/// untouchable by any later crash. (The marker is written *after* the sync;
/// written before, a kept-prefix tear could preserve it next to a flipped
/// byte in the same un-synced round and fake a proof. The marker itself is
/// un-synced until the next sync and may be torn — which only loses a proof,
/// never invents one.)
///
/// Exact algorithm, one pass over the lines with their byte offsets:
/// 1. Walk lines in order. Before any bad line, a valid line is processed as
///    above; a marker line (version `>= 2`, payload `!sync:<N>`) is not
///    emitted, and if `N` != its own start offset the decode is
///    `Err(BadSyncMarker)`; otherwise it raises `M` (the greatest marker
///    start seen).
/// 2. At the first bad line, remember its offset `B` and **keep scanning to
///    the end of the buffer for markers only** (this is what stops a bad
///    *first* line from hiding every later proof). Valid non-marker lines
///    after `B` are ignored; a valid marker after `B` is checked for
///    `N == own offset` (else `Err(BadSyncMarker)`) and raises `M`. Bad
///    lines after `B` are ignored.
/// 3. At the end: if `B` exists and `B < M`, `Err(MidFileCorruption {
///    offset: B, durable_to: M })`. Otherwise (`B >= M`, or no marker at
///    all, or no bad line) the bad line is a torn tail: return the records
///    before `B`, exactly as before.
///
/// Files with no markers (every version-1 file, or a v2 file crashed before
/// its first marker) keep the lenient behaviour: a bad line anywhere is a
/// tail. A mixed file (v1 lines, then v2 lines and markers) is valid; a
/// marker's proof covers v1 lines before it too.
///
/// Residual exposure, inherent: the *latest* round has no durable marker
/// until the next sync, so corruption of that one round alone is still
/// indistinguishable from a torn tail.
///
/// Never panics on any input.
pub fn decode_lines_extent<'a>(
    tag: &FormatTag,
    bytes: &'a [u8],
) -> Result<DecodedLines<'a>, FormatError> {
    let mut lines = Vec::new();
    let mut first_bad: Option<usize> = None;
    let mut durable_to: Option<usize> = None;
    let mut pos = 0usize;
    while pos < bytes.len() {
        let start = pos;
        let (end, next) = match bytes[pos..].iter().position(|&b| b == b'\n') {
            Some(r) => (pos + r, pos + r + 1),
            None => (bytes.len(), bytes.len()),
        };
        pos = next;
        let line = &bytes[start..end];
        if line.is_empty() {
            continue;
        }
        let body = frame_body(line);
        let Some(body) = body else {
            first_bad.get_or_insert(start);
            continue;
        };
        if first_bad.is_some() {
            // Past the first bad line: only a valid marker matters, and only
            // one this decoder understands (right magic, v2..=current).
            if body[..4] != tag.magic[..] {
                continue;
            }
            let Some(version) = std::str::from_utf8(&body[4..6])
                .ok()
                .and_then(|s| u8::from_str_radix(s, 16).ok())
            else {
                continue;
            };
            if !(SYNC_MARKER_MIN_VERSION..=tag.version).contains(&version) {
                continue;
            }
            if let Some(claimed) = parse_marker(tag, &body[6..])? {
                durable_to = Some(check_marker(tag, start, claimed)?);
            }
            continue;
        }
        let version = body_version(tag, body)?;
        if version >= SYNC_MARKER_MIN_VERSION
            && let Some(claimed) = parse_marker(tag, &body[6..])?
        {
            durable_to = Some(check_marker(tag, start, claimed)?);
            continue;
        }
        lines.push((version, &body[6..]));
    }
    if let (Some(bad), Some(durable)) = (first_bad, durable_to)
        && bad < durable
    {
        return Err(FormatError::MidFileCorruption {
            format: tag.name,
            offset: bad as u64,
            durable_to: durable as u64,
        });
    }
    Ok(DecodedLines {
        lines,
        valid_len: first_bad.unwrap_or(bytes.len()),
    })
}

/// A marker is only a proof if it sits where it says it does.
fn check_marker(tag: &FormatTag, start: usize, claimed: u64) -> Result<usize, FormatError> {
    if claimed == start as u64 {
        Ok(start)
    } else {
        Err(FormatError::BadSyncMarker {
            format: tag.name,
            offset: start as u64,
            claimed,
        })
    }
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

    const TAG2: FormatTag = FormatTag {
        magic: *b"TST1",
        version: 2,
        name: "test-format",
    };

    /// A bad *first* line followed by a valid marker must not hide the proof:
    /// the scan continues past the first bad line.
    #[test]
    fn a_bad_first_line_before_a_marker_is_mid_file_corruption() {
        let mut bytes = encode_line(&TAG2, b"{\"a\":1}");
        let second = bytes.len();
        bytes.extend(encode_line(&TAG2, b"{\"b\":2}"));
        let marker_at = bytes.len();
        bytes.extend(encode_sync_marker(&TAG2, marker_at as u64));
        bytes[12] ^= 0xFF;
        assert_eq!(
            decode_lines(&TAG2, &bytes).unwrap_err(),
            FormatError::MidFileCorruption {
                format: "test-format",
                offset: 0,
                durable_to: marker_at as u64,
            }
        );
        assert!(second > 0);
        // No marker at all: lenient, even with a valid line after the bad one.
        let lenient = &bytes[..marker_at];
        assert_eq!(decode_lines(&TAG2, lenient).unwrap(), vec![]);
    }

    #[test]
    fn a_malformed_marker_payload_is_malformed() {
        let line = encode_line(&TAG2, b"!sync:notanumber");
        assert!(matches!(
            decode_lines(&TAG2, &line),
            Err(FormatError::Malformed { .. })
        ));
    }

    #[test]
    fn repaired_image_cuts_the_tail_and_terminates_the_last_line() {
        let mut bytes = encode_line(&TAG2, b"x");
        let good = bytes.len();
        bytes.extend_from_slice(b"deadbeef:TST102garb");
        let d = decode_lines_extent(&TAG2, &bytes).unwrap();
        assert_eq!(d.valid_len, good);
        assert_eq!(
            repaired_image(&bytes, d.valid_len).unwrap(),
            bytes[..good].to_vec()
        );
        // Clean file: nothing to do.
        let clean = encode_line(&TAG2, b"x");
        assert!(repaired_image(&clean, clean.len()).is_none());
        // Valid last line with its `\n` lost gains one.
        let unterminated = &clean[..clean.len() - 1];
        let d = decode_lines_extent(&TAG2, unterminated).unwrap();
        assert_eq!(d.lines.len(), 1);
        assert_eq!(repaired_image(unterminated, d.valid_len).unwrap(), clean);
    }
}
