//! Thin, `#[doc(hidden)]` decoder entry points for the `fuzz/` cargo-fuzz
//! project (roadmap R-01 (c)). Compiled only under the off-by-default
//! `fuzzing` feature; every function returns a count/bool or a rendered
//! error — the property under test is "never panics, never allocates
//! unboundedly, decode-or-named-error".

/// `raftkv-wire` frame (`codec::decode_wire`, magic + version dispatch).
pub fn raftkv_wire(bytes: &[u8]) -> Result<(), String> {
    crate::codec::decode_wire(bytes)
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `raftkv-image` snapshot image (`codec::decode_image`).
pub fn raftkv_image(bytes: &[u8]) -> Result<usize, String> {
    crate::codec::decode_image(bytes)
        .map(|(_, rows)| rows.len())
        .map_err(|e| e.to_string())
}

/// Engine-internal marker values (transaction record, split, ceiling, seal,
/// applied watermark). Each is `None` on malformed input; returns how many
/// of the five decoders accepted `bytes`.
pub fn engine_markers(bytes: &[u8]) -> usize {
    usize::from(crate::txn::decode_record(bytes).is_some())
        + usize::from(crate::split::decode_split_value(bytes).is_some())
        + usize::from(crate::ceiling::decode_ceiling_value(bytes).is_some())
        + usize::from(crate::seal::decode_seal_value(bytes).is_some())
        + usize::from(crate::applied::decode_applied_value(bytes).is_some())
}
