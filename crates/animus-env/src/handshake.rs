//! A per-connection handshake preamble (ADR 0073 Phase 0, workstream D).
//!
//! **Prep for Phase 2, not a compatibility mechanism yet.** This module is
//! pure codec + check: it builds the byte layout, the version-equality
//! policy, and the error type. It does not, itself, read or write a single
//! byte on a real socket or a `SimEnv` node — a caller wires that in,
//! calling [`encode`], [`decode`], and [`check_peer`] from here rather than
//! reinventing them. **`ProdEnv`'s real internal `Network` transport is
//! wired to it now** — `crates/animus-env/src/prod.rs`'s
//! `perform_handshake` runs this exchange once per connection on both the
//! accept and dial paths, before a single frame is read or written. Still
//! to come: `animusd`'s client/intra port (a distinct wire, [`CLIENT_
//! PROTOCOL`], below) and `SimEnv`'s per-node delivery check.
//!
//! # Why a per-connection preamble, not a per-message version field
//!
//! Three things about how this wire is actually used make a handshake, paid
//! once per connection, the right shape rather than stamping every message:
//!
//! - **Internal connections are pooled** (`animus-env/CLAUDE.md`'s "`ProdEnv`
//!   pools one outbound TCP connection per destination" entry) — a version
//!   check paid once per connection costs nothing against the connection's
//!   whole lifetime, where a per-message field would be paid on every single
//!   Raft heartbeat for as long as the connection lives.
//! - **ADR 0026 multiplexes many `(node, stream)` protocol instances over one
//!   connection** — the control-plane Raft group, and every per-tablet
//!   CP-data Raft group hosted on this node pair, all ride the same
//!   underlying socket. One handshake at connect time covers every one of
//!   those streams; there is no need (and no natural place) to re-check a
//!   version per stream, let alone per message.
//! - **Symmetry keeps the check trivial**: both sides write their own
//!   preamble immediately on connect/accept, then each reads and checks the
//!   peer's — no separate "client" and "server" preamble shape, no extra
//!   round trip to negotiate who goes first.
//!
//! `SimEnv` has no real connections at all (ADR 0003 — no sockets), so a
//! later layer models the identical check as a per-node version, verified on
//! delivery, using this same [`check_peer`] — see that layer's own doc for
//! how a connectionless simulator adapts a connection-shaped check.
//!
//! # Byte layout
//!
//! ```text
//! magic[4] | version:u8 | ext_len:u16 (LE) | ext[ext_len]
//! ```
//!
//! `ext` is the **Phase 2 extension area** (ADR 0073 Phase 2): a sequence of
//! TLVs, each `tag:u16 LE | len:u16 LE | value[len]` — tag 1 a supported
//! version range (`min:u32 LE | max:u32 LE`, opaque numbers to this crate),
//! tag 2 a display-only UTF-8 build string; unknown tags are skipped. See
//! [`encode_ext`], [`parse_ext_range`] and [`check_peer_ext`]; an empty `ext`
//! is a Phase 1 binary, read as the range `[1,1]`. A v1 [`encode`] always
//! wrote it empty; a v1 [`decode`] reads whatever bytes are there and carries them
//! through on the returned [`Preamble`] **without ever rejecting a non-empty
//! ext** — that is what makes this preamble forward-extensible without
//! another format reset once Phase 2 needs the field for real. `ext_len` is
//! capped at [`MAX_EXTENSION_LEN`] bytes so a misbehaving or malicious peer
//! cannot make a reader buffer an unbounded amount before the length itself
//! is even validated.
//!
//! # Version policy (v1)
//!
//! [`check_peer`] enforces **exact version equality**: both ends of a
//! connection must be running the identical build's protocol version, which
//! is already true of this codebase today (no rolling upgrade exists yet).
//! **Phase 2 relaxes this** to a supported-range intersection, negotiated
//! through the `ext` area this version already carries but does not yet
//! interpret — that relaxation is future work, not implemented here.

/// The maximum number of bytes a preamble's `ext` area may declare. Bounds
/// what a peer's declared `ext_len` can make a reader buffer, independent of
/// whether the bytes ever actually arrive — [`decode`] rejects an
/// out-of-bound `ext_len` before waiting for a single one of those bytes.
pub const MAX_EXTENSION_LEN: u16 = 1024;

/// The fixed header size in bytes: `magic[4] | version:u8 | ext_len:u16`.
/// A later layer reads exactly this many bytes first, decodes them to learn
/// `ext_len`, then reads that many more — see [`decode`]'s own doc for the
/// incremental-read shape this constant supports.
pub const HEADER_LEN: usize = 4 + 1 + 2;

/// One versioned wire protocol's handshake identity: its human-readable
/// name (for error messages), its 4-byte magic, and its current version.
///
/// Two instances of this exist, each versioning a different protocol
/// independently — a version bump on one must never require a version bump
/// on the other, since they are unrelated wire formats that happen to both
/// ride the same handshake mechanism:
///
/// - [`NETWORK_PROTOCOL`] — the `Network` seam's internal transport (ADR
///   0026): control-plane Raft, every per-tablet CP-data Raft group, and any
///   other traffic multiplexed over `(node, stream)`.
/// - [`CLIENT_PROTOCOL`] — the client/intra length-prefixed JSON-RPC wire
///   (`animus-node::wire`'s `ClientRequest`/`ClientResponse`), used both
///   between a client and a node and between nodes for forwarding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolSpec {
    /// Named in error messages, so a refusal says which wire it came from.
    pub name: &'static str,
    /// This protocol's own 4-byte magic. Distinct across protocols so a
    /// peer speaking the wrong one is refused with a clear cause rather
    /// than misread as an unsupported version of the right one.
    pub magic: [u8; 4],
    /// This build's version of this protocol. Bumped independently of the
    /// other `ProtocolSpec`'s own version counter.
    pub version: u8,
}

/// The internal `Network` transport's handshake identity (ADR 0026).
/// Magic checked against every existing 4-byte magic already in use in this
/// codebase (`CMF1`, `ADE1`, `SSIX`, the ADR 0073 "suggested magics" table's
/// `LWL1`/`CWL1`/`CSN1`/`SWL1`) — no collision.
pub const NETWORK_PROTOCOL: ProtocolSpec = ProtocolSpec {
    name: "network",
    magic: *b"NHS1",
    version: 1,
};

/// The client/intra JSON-RPC wire's handshake identity. Same collision
/// check as [`NETWORK_PROTOCOL`]; distinct magic since the two protocols
/// version independently.
pub const CLIENT_PROTOCOL: ProtocolSpec = ProtocolSpec {
    name: "client",
    magic: *b"CHS1",
    version: 1,
};

/// One connection's handshake preamble — what each side writes on connect
/// and what each side reads back from its peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Preamble {
    /// The protocol magic the sender is speaking.
    pub magic: [u8; 4],
    /// The sender's version of that protocol.
    pub version: u8,
    /// Reserved for Phase 2 (feature bits / a supported-version range).
    /// Always empty from a v1 [`encode`]; a v1 [`decode`] preserves
    /// whatever bytes a peer sent here, never rejecting them for being
    /// non-empty — see the module doc's "Byte layout" section.
    pub extensions: Vec<u8>,
}

impl Preamble {
    /// The preamble this build sends for `spec` — its magic and version,
    /// with an empty `ext` area (Phase 2 is the first layer with anything
    /// real to put there).
    #[must_use]
    pub fn for_protocol(spec: &ProtocolSpec) -> Self {
        Preamble {
            magic: spec.magic,
            version: spec.version,
            extensions: Vec::new(),
        }
    }
}

/// Every way a handshake can fail. Never constructed from a panic — every
/// path in [`decode`]/[`check_peer`] that could otherwise index out of
/// bounds or overflow returns one of these instead.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum HandshakeError {
    /// The peer's magic did not match the protocol being checked against.
    /// The most likely real-world cause is a **pre-baseline peer**: one
    /// whose first bytes are a raw, unversioned frame from before this
    /// handshake existed, per ADR 0073's "untagged pre-baseline input is a
    /// named loud Err" rule — not a truncation and not corruption, just a
    /// peer that was never going to send this preamble at all.
    #[error("{protocol} handshake: bad magic (found {found:02x?})")]
    BadMagic {
        /// Which protocol's handshake this was checked against.
        protocol: &'static str,
        /// The 4 bytes actually found where the magic was expected.
        found: [u8; 4],
    },
    /// The peer's magic matched, but its version did not — v1's policy is
    /// exact equality (see the module doc's "Version policy" section), so
    /// any mismatch here means the two ends are different builds.
    #[error("{protocol} handshake: unsupported version {found} (this build supports {supported})")]
    UnsupportedVersion {
        /// Which protocol's handshake this was checked against.
        protocol: &'static str,
        /// The peer's declared version.
        found: u8,
        /// This build's own version of the same protocol.
        supported: u8,
    },
    /// The declared `ext_len` exceeds [`MAX_EXTENSION_LEN`]. Checked against
    /// the length field alone, before waiting for any of the extension
    /// bytes themselves to arrive — a bad/hostile peer cannot use this to
    /// make a reader buffer an unbounded amount.
    #[error("handshake: extension too long ({len} bytes, max {max})")]
    ExtensionTooLong {
        /// The declared extension length, in bytes.
        len: usize,
        /// [`MAX_EXTENSION_LEN`], echoed for the message.
        max: u16,
    },
    /// Not enough bytes were available to decode a complete preamble yet —
    /// neither a bad header nor an oversized extension, just "read more and
    /// try again." A later layer reads [`HEADER_LEN`] bytes first; once
    /// that decodes far enough to learn `ext_len`, it reads that many more
    /// and decodes again.
    #[error("handshake: truncated input, need more bytes")]
    Incomplete,
    /// (Phase 2) The `ext` bytes are not a well-formed TLV sequence
    /// (truncated header or value, a range value that is not 8 bytes, or
    /// `min > max`).
    #[error("handshake: malformed extension area ({0})")]
    Malformed(&'static str),
    /// (Phase 2) The two ends' supported version ranges do not intersect.
    #[error(
        "handshake: version ranges are disjoint (peer supports {peer_min}..={peer_max}, \
         this node supports {own_min}..={own_max})"
    )]
    DisjointRanges {
        /// Peer's range minimum.
        peer_min: u32,
        /// Peer's range maximum.
        peer_max: u32,
        /// Own range minimum.
        own_min: u32,
        /// Own range maximum.
        own_max: u32,
    },
    /// (Phase 2) The peer sent no `ext` (a Phase 1 binary) while the
    /// cluster has versioning enabled.
    #[error("handshake: peer is a Phase 1 binary; cluster has versioning enabled")]
    Phase1Peer,
}

/// Encodes `preamble` to its wire bytes: `magic | version | ext_len (LE) |
/// ext`. `preamble` is always data this build itself constructed (typically
/// via [`Preamble::for_protocol`]), never untrusted peer input, so this
/// function has no `Result` — a caller that somehow builds a `Preamble`
/// whose `extensions` exceeds `u16::MAX` bytes has a programming error, not
/// a peer to refuse; that case panics rather than silently truncating or
/// wrapping the length.
#[must_use]
pub fn encode(preamble: &Preamble) -> Vec<u8> {
    let ext_len = u16::try_from(preamble.extensions.len())
        .expect("Preamble::extensions must fit in a u16 length prefix");
    let mut out = Vec::with_capacity(HEADER_LEN + preamble.extensions.len());
    out.extend_from_slice(&preamble.magic);
    out.push(preamble.version);
    out.extend_from_slice(&ext_len.to_le_bytes());
    out.extend_from_slice(&preamble.extensions);
    out
}

/// Decodes a [`Preamble`] from the start of `bytes`, returning it together
/// with how many bytes it consumed. Never panics on any input, including
/// empty, truncated, or adversarially large-`ext_len` input.
///
/// Built for incremental reads off a real socket: a caller reads
/// [`HEADER_LEN`] bytes, calls this, and on [`HandshakeError::Incomplete`]
/// reads more before retrying — once the header itself is present this
/// tells the caller exactly how many total bytes ([`HEADER_LEN`] +
/// `ext_len`) it needs before decoding will succeed. This function does
/// not, itself, check `magic`/`version` against any particular protocol —
/// that is [`check_peer`]'s job, since decoding is protocol-agnostic
/// (a caller decodes first, then checks the result against whichever
/// [`ProtocolSpec`] it expects on that connection).
pub fn decode(bytes: &[u8]) -> Result<(Preamble, usize), HandshakeError> {
    if bytes.len() < HEADER_LEN {
        return Err(HandshakeError::Incomplete);
    }
    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[0..4]);
    let version = bytes[4];
    let ext_len = u16::from_le_bytes([bytes[5], bytes[6]]);
    if ext_len > MAX_EXTENSION_LEN {
        return Err(HandshakeError::ExtensionTooLong {
            len: ext_len as usize,
            max: MAX_EXTENSION_LEN,
        });
    }
    let total = HEADER_LEN + ext_len as usize;
    if bytes.len() < total {
        return Err(HandshakeError::Incomplete);
    }
    let extensions = bytes[HEADER_LEN..total].to_vec();
    Ok((
        Preamble {
            magic,
            version,
            extensions,
        },
        total,
    ))
}

/// Checks a decoded peer [`Preamble`] against `expected`. v1 policy is
/// **exact version equality** (see the module doc's "Version policy"
/// section) — Phase 2 is what relaxes this to a supported-range
/// intersection negotiated via the `ext` area. Never inspects
/// `peer.extensions` at all in this version: an incoming preamble's
/// extension bytes are decoded and carried on the `Preamble` (never
/// rejected for being non-empty, per the module doc), but nothing in v1
/// interprets them yet.
pub fn check_peer(expected: &ProtocolSpec, peer: &Preamble) -> Result<(), HandshakeError> {
    if peer.magic != expected.magic {
        return Err(HandshakeError::BadMagic {
            protocol: expected.name,
            found: peer.magic,
        });
    }
    if peer.version != expected.version {
        return Err(HandshakeError::UnsupportedVersion {
            protocol: expected.name,
            found: peer.version,
            supported: expected.version,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Phase 2 (ADR 0073): the `ext` TLV area — a supported-version range and a
// display-only build string — plus the era-aware peer check.
// ---------------------------------------------------------------------

/// `ext` TLV tag: a supported version range, `min:u32 LE, max:u32 LE`
/// (8-byte value). The numbers are opaque to this crate (it does not depend
/// on `animus-control`); callers interpret them as a cluster-version range.
pub const EXT_TAG_RANGE: u16 = 1;
/// `ext` TLV tag: a build string, UTF-8, display only (never compared).
pub const EXT_TAG_BUILD: u16 = 2;

/// The range an empty `ext` stands for: a Phase 1 binary, which supports
/// exactly cluster version 1.
pub const PHASE1_RANGE: (u32, u32) = (1, 1);

/// The refusal text for a Phase 1 (empty-`ext`) peer once the cluster has
/// versioning enabled.
pub const PHASE1_PEER_REFUSAL: &str = "peer is a Phase 1 binary; cluster has versioning enabled";

/// Encodes an `ext` area. Layout: a sequence of TLVs, each
/// `tag:u16 LE | len:u16 LE | value[len]`. Tag [`EXT_TAG_RANGE`] carries
/// `min:u32 LE | max:u32 LE`; tag [`EXT_TAG_BUILD`] carries UTF-8 text. Both
/// `None` yields an empty vec (today's bytes exactly). A build string is
/// truncated (at a char boundary) so the whole area fits
/// [`MAX_EXTENSION_LEN`].
#[must_use]
pub fn encode_ext(range: Option<(u32, u32)>, build: Option<&str>) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some((min, max)) = range {
        out.extend_from_slice(&EXT_TAG_RANGE.to_le_bytes());
        out.extend_from_slice(&8u16.to_le_bytes());
        out.extend_from_slice(&min.to_le_bytes());
        out.extend_from_slice(&max.to_le_bytes());
    }
    if let Some(build) = build {
        let room = (MAX_EXTENSION_LEN as usize).saturating_sub(out.len() + 4);
        let mut end = build.len().min(room);
        while !build.is_char_boundary(end) {
            end -= 1;
        }
        out.extend_from_slice(&EXT_TAG_BUILD.to_le_bytes());
        out.extend_from_slice(&(end as u16).to_le_bytes());
        out.extend_from_slice(&build.as_bytes()[..end]);
    }
    out
}

/// Walks the TLVs of `ext`, calling `f(tag, value)` for each.
fn for_each_tlv(
    ext: &[u8],
    mut f: impl FnMut(u16, &[u8]) -> Result<(), HandshakeError>,
) -> Result<(), HandshakeError> {
    let mut rest = ext;
    while !rest.is_empty() {
        if rest.len() < 4 {
            return Err(HandshakeError::Malformed("truncated TLV header"));
        }
        let tag = u16::from_le_bytes([rest[0], rest[1]]);
        let len = u16::from_le_bytes([rest[2], rest[3]]) as usize;
        let body = &rest[4..];
        if body.len() < len {
            return Err(HandshakeError::Malformed("truncated TLV value"));
        }
        f(tag, &body[..len])?;
        rest = &body[len..];
    }
    Ok(())
}

/// Parses the supported range out of an `ext` area. An empty `ext` is
/// `Ok(None)` (a Phase 1 binary; callers read it as [`PHASE1_RANGE`]).
/// Unknown tags are ignored; a well-formed `ext` without a range tag is also
/// `Ok(None)`. A malformed `ext` is an `Err`, never a panic.
pub fn parse_ext_range(ext: &[u8]) -> Result<Option<(u32, u32)>, HandshakeError> {
    let mut range = None;
    for_each_tlv(ext, |tag, v| {
        if tag == EXT_TAG_RANGE {
            let v: [u8; 8] = v
                .try_into()
                .map_err(|_| HandshakeError::Malformed("range value is not 8 bytes"))?;
            let min = u32::from_le_bytes([v[0], v[1], v[2], v[3]]);
            let max = u32::from_le_bytes([v[4], v[5], v[6], v[7]]);
            if min > max {
                return Err(HandshakeError::Malformed("range min exceeds max"));
            }
            range = Some((min, max));
        }
        Ok(())
    })?;
    Ok(range)
}

/// Parses the display-only build string out of an `ext` area, if present and
/// valid UTF-8. Malformed input yields `None`.
#[must_use]
pub fn parse_ext_build(ext: &[u8]) -> Option<String> {
    let mut build = None;
    let _ = for_each_tlv(ext, |tag, v| {
        if tag == EXT_TAG_BUILD {
            build = std::str::from_utf8(v).ok().map(str::to_owned);
        }
        Ok(())
    });
    build
}

/// [`check_peer`] plus the Phase 2 `ext` checks. After the magic/version
/// check: a malformed peer or own `ext` is refused; an empty peer `ext` is
/// refused with [`PHASE1_PEER_REFUSAL`] when `require_peer_ext` is set;
/// otherwise the two ranges (empty `ext` = [`PHASE1_RANGE`]) must intersect,
/// else [`HandshakeError::DisjointRanges`]. With both `ext` areas empty and
/// `require_peer_ext` off this is exactly [`check_peer`].
pub fn check_peer_ext(
    expected: &ProtocolSpec,
    own_ext: &[u8],
    peer: &Preamble,
    require_peer_ext: bool,
) -> Result<(), HandshakeError> {
    check_peer(expected, peer)?;
    let peer_range = parse_ext_range(&peer.extensions)?;
    if require_peer_ext && peer.extensions.is_empty() {
        return Err(HandshakeError::Phase1Peer);
    }
    let own_range = parse_ext_range(own_ext)?;
    let (peer_min, peer_max) = peer_range.unwrap_or(PHASE1_RANGE);
    let (own_min, own_max) = own_range.unwrap_or(PHASE1_RANGE);
    if peer_min > own_max || own_min > peer_max {
        return Err(HandshakeError::DisjointRanges {
            peer_min,
            peer_max,
            own_min,
            own_max,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_and_client_magics_are_distinct() {
        assert_ne!(NETWORK_PROTOCOL.magic, CLIENT_PROTOCOL.magic);
    }

    #[test]
    fn no_collision_with_existing_workspace_magics() {
        // Every 4-byte magic already in use elsewhere in this codebase, as
        // of ADR 0073's inventory (`CMF1`/`ADE1`/`SSIX`) and its "suggested
        // magics" table for the other Phase 0 workstreams
        // (`LWL1`/`CWL1`/`CSN1`/`SWL1`). This is a static, hand-maintained
        // list (not a repo-wide grep at test time) — deliberately so: it
        // pins what this module was checked against at the time it was
        // written, and a real collision introduced later would be caught
        // by `check_peer`/`decode` misbehaving in an integration test long
        // before this one would need updating.
        let existing: &[[u8; 4]] = &[
            *b"CMF1", *b"ADE1", *b"SSIX", *b"LWL1", *b"CWL1", *b"CSN1", *b"SWL1",
        ];
        for magic in existing {
            assert_ne!(NETWORK_PROTOCOL.magic, *magic);
            assert_ne!(CLIENT_PROTOCOL.magic, *magic);
        }
    }

    #[test]
    fn round_trip_empty_extensions() {
        for spec in [&NETWORK_PROTOCOL, &CLIENT_PROTOCOL] {
            let sent = Preamble::for_protocol(spec);
            let bytes = encode(&sent);
            assert_eq!(bytes.len(), HEADER_LEN);
            let (got, consumed) = decode(&bytes).expect("decode of freshly-encoded bytes");
            assert_eq!(consumed, bytes.len());
            assert_eq!(got, sent);
            check_peer(spec, &got).expect("a build's own preamble must pass its own check");
        }
    }

    #[test]
    fn round_trip_nonempty_extensions_is_carried_through_unrejected() {
        let sent = Preamble {
            magic: NETWORK_PROTOCOL.magic,
            version: NETWORK_PROTOCOL.version,
            extensions: vec![1, 2, 3, 4, 5],
        };
        let bytes = encode(&sent);
        let (got, consumed) = decode(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(got.extensions, vec![1, 2, 3, 4, 5]);
        // v1 never rejects a peer for a non-empty, in-bound ext area.
        check_peer(&NETWORK_PROTOCOL, &got).expect("non-empty ext must not fail v1's check");
    }

    #[test]
    fn decode_reports_incomplete_before_a_full_header() {
        for len in 0..HEADER_LEN {
            let bytes = vec![0xAAu8; len];
            assert_eq!(decode(&bytes), Err(HandshakeError::Incomplete));
        }
    }

    #[test]
    fn decode_reports_incomplete_when_header_present_but_extension_bytes_missing() {
        let full = encode(&Preamble {
            magic: CLIENT_PROTOCOL.magic,
            version: CLIENT_PROTOCOL.version,
            extensions: vec![9, 9, 9],
        });
        // The full header plus zero, one, and two (but not all three) of
        // the declared extension bytes.
        for take in HEADER_LEN..full.len() {
            assert_eq!(decode(&full[..take]), Err(HandshakeError::Incomplete));
        }
    }

    #[test]
    fn decode_rejects_an_oversized_declared_extension_length_immediately() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(&NETWORK_PROTOCOL.magic);
        bytes[4] = NETWORK_PROTOCOL.version;
        let too_big = MAX_EXTENSION_LEN + 1;
        bytes[5..7].copy_from_slice(&too_big.to_le_bytes());
        // Note: none of the (huge) declared extension bytes are actually
        // present — this must still be rejected from the header alone,
        // never treated as `Incomplete`.
        assert_eq!(
            decode(&bytes),
            Err(HandshakeError::ExtensionTooLong {
                len: too_big as usize,
                max: MAX_EXTENSION_LEN,
            })
        );
    }

    #[test]
    fn check_peer_rejects_bad_magic_naming_the_protocol() {
        let peer = Preamble {
            magic: *b"XXXX",
            version: NETWORK_PROTOCOL.version,
            extensions: Vec::new(),
        };
        let err = check_peer(&NETWORK_PROTOCOL, &peer).unwrap_err();
        assert_eq!(
            err,
            HandshakeError::BadMagic {
                protocol: "network",
                found: *b"XXXX",
            }
        );
    }

    #[test]
    fn check_peer_rejects_a_raw_pre_baseline_frame_as_bad_magic() {
        // A pre-handshake peer's first bytes on this wire used to just be
        // the start of an ordinary frame (`ProdEnv`'s
        // `[from_len:u32][from]...` for the internal wire, or a raw
        // length-prefixed JSON frame for the client wire) — never this
        // magic. Simulate that with a plausible frame-length prefix.
        let raw_frame_start = [0x00, 0x00, 0x00, 0x10, 0xFF, 0x00, 0x00];
        let (peer, _) = decode(&raw_frame_start).expect("structurally decodable, just wrong");
        let err = check_peer(&CLIENT_PROTOCOL, &peer).unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::BadMagic {
                protocol: "client",
                ..
            }
        ));
    }

    #[test]
    fn check_peer_rejects_wrong_version_naming_the_protocol() {
        let peer = Preamble {
            magic: CLIENT_PROTOCOL.magic,
            version: CLIENT_PROTOCOL.version + 1,
            extensions: Vec::new(),
        };
        let err = check_peer(&CLIENT_PROTOCOL, &peer).unwrap_err();
        assert_eq!(
            err,
            HandshakeError::UnsupportedVersion {
                protocol: "client",
                found: CLIENT_PROTOCOL.version + 1,
                supported: CLIENT_PROTOCOL.version,
            }
        );
    }

    #[test]
    fn check_peer_rejects_mismatched_protocols_as_bad_magic_not_version() {
        // A client-protocol preamble checked against the network spec: the
        // magics differ, so this must surface as `BadMagic`, never as a
        // coincidental version comparison (both are `1` today).
        let peer = Preamble::for_protocol(&CLIENT_PROTOCOL);
        let err = check_peer(&NETWORK_PROTOCOL, &peer).unwrap_err();
        assert!(matches!(err, HandshakeError::BadMagic { .. }));
    }

    /// A small deterministic (seeded, no `thread_rng`) fuzz-ish sweep:
    /// `decode` must never panic on any byte string, truncated or not, and
    /// whatever it does return must never claim to have consumed more bytes
    /// than were given to it.
    #[test]
    fn decode_never_panics_over_many_random_and_truncated_inputs() {
        // A tiny, self-contained xorshift64 PRNG — deterministic across
        // runs from a fixed seed, and no new dependency. Not `animus_env`'s
        // own `Rng` trait: this is a plain unit test with no `Env` handle,
        // and `thread_rng`/`OsRng` are exactly what the workspace lints
        // forbid outside the seam.
        struct Xorshift64(u64);
        impl Xorshift64 {
            fn next(&mut self) -> u64 {
                let mut x = self.0;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                self.0 = x;
                x
            }
        }
        let mut rng = Xorshift64(0xC0FF_EE15_5EED_1234);
        for _ in 0..2000 {
            let len = (rng.next() % 40) as usize;
            let bytes: Vec<u8> = (0..len).map(|_| (rng.next() & 0xFF) as u8).collect();
            // Must not panic. Whatever comes back must be self-consistent.
            match decode(&bytes) {
                Ok((_, consumed)) => assert!(consumed <= bytes.len()),
                Err(HandshakeError::Incomplete) | Err(HandshakeError::ExtensionTooLong { .. }) => {}
                Err(other) => panic!(
                    "decode must only ever return Incomplete/ExtensionTooLong, got {other:?}"
                ),
            }
        }
    }

    #[test]
    fn ext_tlv_round_trip() {
        let e = encode_ext(Some((2, 5)), Some("build-x"));
        assert_eq!(parse_ext_range(&e), Ok(Some((2, 5))));
        assert_eq!(parse_ext_build(&e).as_deref(), Some("build-x"));
        assert_eq!(encode_ext(None, None), Vec::<u8>::new());
        assert_eq!(parse_ext_range(&encode_ext(None, Some("b"))), Ok(None));
    }

    #[test]
    fn ext_unknown_tag_ignored() {
        let mut e = Vec::new();
        e.extend_from_slice(&99u16.to_le_bytes());
        e.extend_from_slice(&3u16.to_le_bytes());
        e.extend_from_slice(b"abc");
        e.extend_from_slice(&encode_ext(Some((1, 3)), None));
        assert_eq!(parse_ext_range(&e), Ok(Some((1, 3))));
    }

    #[test]
    fn ext_malformed_refused_not_panicking() {
        let good = encode_ext(Some((1, 3)), Some("hello"));
        for cut in 1..good.len() {
            // every strict prefix either parses (cut on a TLV boundary) or errs
            let _ = parse_ext_range(&good[..cut]);
        }
        assert!(matches!(
            parse_ext_range(&[1]),
            Err(HandshakeError::Malformed(_))
        ));
        assert!(matches!(
            parse_ext_range(&good[..good.len() - 1]),
            Err(HandshakeError::Malformed(_))
        ));
        // range value of the wrong size
        let mut bad = Vec::new();
        bad.extend_from_slice(&EXT_TAG_RANGE.to_le_bytes());
        bad.extend_from_slice(&4u16.to_le_bytes());
        bad.extend_from_slice(&[0; 4]);
        assert!(matches!(
            parse_ext_range(&bad),
            Err(HandshakeError::Malformed(_))
        ));
        // min > max
        let inv = encode_ext(Some((5, 2)), None);
        assert!(matches!(
            parse_ext_range(&inv),
            Err(HandshakeError::Malformed(_))
        ));
        let peer = Preamble {
            extensions: bad,
            ..Preamble::for_protocol(&NETWORK_PROTOCOL)
        };
        assert!(matches!(
            check_peer_ext(&NETWORK_PROTOCOL, &[], &peer, false),
            Err(HandshakeError::Malformed(_))
        ));
    }

    #[test]
    fn empty_ext_is_range_one_one() {
        let phase1 = Preamble::for_protocol(&NETWORK_PROTOCOL);
        let with = |r| Preamble {
            extensions: encode_ext(Some(r), None),
            ..Preamble::for_protocol(&NETWORK_PROTOCOL)
        };
        // both empty
        check_peer_ext(&NETWORK_PROTOCOL, &[], &phase1, false).unwrap();
        // own [1,3] vs empty peer: overlap at 1
        check_peer_ext(
            &NETWORK_PROTOCOL,
            &encode_ext(Some((1, 3)), None),
            &phase1,
            false,
        )
        .unwrap();
        // own [2,3] vs empty peer: disjoint
        let err = check_peer_ext(
            &NETWORK_PROTOCOL,
            &encode_ext(Some((2, 3)), None),
            &phase1,
            false,
        )
        .unwrap_err();
        assert!(matches!(
            err,
            HandshakeError::DisjointRanges {
                peer_min: 1,
                peer_max: 1,
                own_min: 2,
                own_max: 3
            }
        ));
        // empty own vs peer [2,3]: disjoint
        assert!(matches!(
            check_peer_ext(&NETWORK_PROTOCOL, &[], &with((2, 3)), false),
            Err(HandshakeError::DisjointRanges { .. })
        ));
    }

    #[test]
    fn disjoint_refused_by_name_overlap_accepted() {
        let own = encode_ext(Some((1, 3)), None);
        let peer = |r| Preamble {
            extensions: encode_ext(Some(r), None),
            ..Preamble::for_protocol(&NETWORK_PROTOCOL)
        };
        let err = check_peer_ext(&NETWORK_PROTOCOL, &own, &peer((4, 6)), false).unwrap_err();
        assert!(err.to_string().contains("disjoint"), "{err}");
        check_peer_ext(&NETWORK_PROTOCOL, &own, &peer((3, 6)), false).unwrap();
        check_peer_ext(&NETWORK_PROTOCOL, &own, &peer((2, 2)), false).unwrap();
    }

    #[test]
    fn require_flag_refuses_empty_peer_ext() {
        let phase1 = Preamble::for_protocol(&NETWORK_PROTOCOL);
        let err = check_peer_ext(
            &NETWORK_PROTOCOL,
            &encode_ext(Some((1, 2)), None),
            &phase1,
            true,
        )
        .unwrap_err();
        assert_eq!(err, HandshakeError::Phase1Peer);
        assert!(err.to_string().contains(PHASE1_PEER_REFUSAL));
        // a peer with a non-empty ext passes the flag
        let ok = Preamble {
            extensions: encode_ext(Some((1, 2)), None),
            ..phase1
        };
        check_peer_ext(&NETWORK_PROTOCOL, &[], &ok, true).unwrap();
    }

    #[test]
    fn check_peer_ext_still_checks_magic_and_version() {
        let mut p = Preamble::for_protocol(&NETWORK_PROTOCOL);
        p.version = 9;
        assert!(matches!(
            check_peer_ext(&NETWORK_PROTOCOL, &[], &p, false),
            Err(HandshakeError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn default_preamble_bytes_unchanged() {
        let bytes = encode(&Preamble::for_protocol(&NETWORK_PROTOCOL));
        assert_eq!(bytes, [b'N', b'H', b'S', b'1', 1, 0, 0]);
        let bytes = encode(&Preamble {
            extensions: encode_ext(None, None),
            ..Preamble::for_protocol(&NETWORK_PROTOCOL)
        });
        assert_eq!(bytes, [b'N', b'H', b'S', b'1', 1, 0, 0]);
    }

    #[test]
    fn build_string_fits_max_extension() {
        let long = "é".repeat(2000);
        let e = encode_ext(Some((1, 1)), Some(&long));
        assert!(e.len() <= MAX_EXTENSION_LEN as usize);
        assert!(parse_ext_build(&e).is_some());
    }
}
