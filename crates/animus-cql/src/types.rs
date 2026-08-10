//! A small CQL type/value system for the columns this subset supports.
//!
//! CQL columns carry a *type* (a `[option]` in the protocol — a `u16` type id,
//! plus extra bytes for parameterized types) and *values* serialized to the
//! type's canonical wire form. We model the common scalar types only:
//!
//! | CQL type  | id       | wire form                                       |
//! |-----------|----------|-------------------------------------------------|
//! | `text`    | `0x000D` | UTF-8 bytes (no length prefix inside `[bytes]`) |
//! | `int`     | `0x0009` | 4-byte big-endian `i32`                         |
//! | `bigint`  | `0x0002` | 8-byte big-endian `i64`                         |
//! | `boolean` | `0x0004` | 1 byte (`0x00` false / `0x01` true)             |
//! | `blob`    | `0x0003` | raw bytes                                       |
//! | `uuid`    | `0x000C` | 16 bytes                                        |
//!
//! Everything here is pure (no I/O, no clock, no RNG), so it stays deterministic
//! (ADR 0003). Values round-trip: [`CqlType::encode`] turns a [`CqlValue`] into
//! the cell bytes the protocol carries inside a `[bytes]`, and
//! [`CqlType::decode`] reverses it. Both directions are used on the read path
//! (build `RESULT/Rows` cells) and the write path (decode `EXECUTE` bind
//! markers).

use std::fmt;

/// A CQL scalar type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CqlType {
    /// `text` / `varchar` — UTF-8 string.
    Text,
    /// `int` — 32-bit signed integer.
    Int,
    /// `bigint` — 64-bit signed integer.
    BigInt,
    /// `boolean`.
    Boolean,
    /// `blob` — arbitrary bytes.
    Blob,
    /// `uuid` — a 16-byte UUID.
    Uuid,
}

impl CqlType {
    /// The CQL protocol type id (`[option]` id) for this type.
    #[must_use]
    pub fn type_id(self) -> i16 {
        match self {
            CqlType::Text => 0x000D,
            CqlType::Int => 0x0009,
            CqlType::BigInt => 0x0002,
            CqlType::Boolean => 0x0004,
            CqlType::Blob => 0x0003,
            CqlType::Uuid => 0x000C,
        }
    }

    /// Parse a CQL type name (as written in `CREATE TABLE`, case-insensitive).
    /// `varchar` is an alias for `text`.
    #[must_use]
    pub fn parse(name: &str) -> Option<CqlType> {
        Some(match name.to_ascii_lowercase().as_str() {
            "text" | "varchar" | "ascii" => CqlType::Text,
            "int" => CqlType::Int,
            "bigint" | "long" => CqlType::BigInt,
            "boolean" | "bool" => CqlType::Boolean,
            "blob" => CqlType::Blob,
            "uuid" | "timeuuid" => CqlType::Uuid,
            _ => return None,
        })
    }

    /// The canonical CQL keyword for this type (for error messages / docs).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            CqlType::Text => "text",
            CqlType::Int => "int",
            CqlType::BigInt => "bigint",
            CqlType::Boolean => "boolean",
            CqlType::Blob => "blob",
            CqlType::Uuid => "uuid",
        }
    }

    /// Write this type as a column-spec `[option]`: the `u16` type id. None of
    /// our supported types are parameterized, so no extra bytes follow.
    pub fn write_option(self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.type_id().to_be_bytes());
    }

    /// Encode a [`CqlValue`] of (presumably) this type into the cell bytes that
    /// ride inside a protocol `[bytes]`.
    ///
    /// # Errors
    /// [`ValueError::TypeMismatch`] if the value's variant does not match
    /// `self`.
    pub fn encode(self, value: &CqlValue) -> Result<Vec<u8>, ValueError> {
        let ok = matches!(
            (self, value),
            (CqlType::Text, CqlValue::Text(_))
                | (CqlType::Int, CqlValue::Int(_))
                | (CqlType::BigInt, CqlValue::BigInt(_))
                | (CqlType::Boolean, CqlValue::Boolean(_))
                | (CqlType::Blob, CqlValue::Blob(_))
                | (CqlType::Uuid, CqlValue::Uuid(_))
        );
        if !ok {
            return Err(ValueError::TypeMismatch {
                expected: self,
                got: value.clone(),
            });
        }
        Ok(value.to_cell_bytes())
    }

    /// Decode cell bytes (the contents of a protocol `[bytes]`) of this type
    /// into a [`CqlValue`].
    ///
    /// # Errors
    /// [`ValueError::BadEncoding`] if the bytes are the wrong length / not UTF-8.
    pub fn decode(self, bytes: &[u8]) -> Result<CqlValue, ValueError> {
        match self {
            CqlType::Text => {
                let s = std::str::from_utf8(bytes)
                    .map_err(|_| ValueError::BadEncoding(self))?
                    .to_owned();
                Ok(CqlValue::Text(s))
            }
            CqlType::Int => {
                let arr: [u8; 4] = bytes
                    .try_into()
                    .map_err(|_| ValueError::BadEncoding(self))?;
                Ok(CqlValue::Int(i32::from_be_bytes(arr)))
            }
            CqlType::BigInt => {
                let arr: [u8; 8] = bytes
                    .try_into()
                    .map_err(|_| ValueError::BadEncoding(self))?;
                Ok(CqlValue::BigInt(i64::from_be_bytes(arr)))
            }
            CqlType::Boolean => match bytes {
                [0] => Ok(CqlValue::Boolean(false)),
                [_] => Ok(CqlValue::Boolean(true)),
                _ => Err(ValueError::BadEncoding(self)),
            },
            CqlType::Blob => Ok(CqlValue::Blob(bytes.to_vec())),
            CqlType::Uuid => {
                let arr: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| ValueError::BadEncoding(self))?;
                Ok(CqlValue::Uuid(arr))
            }
        }
    }

    /// Parse a CQL literal (as it appears in a textual `INSERT`/`WHERE`) into a
    /// value of this type. `quoted` records whether the literal was a quoted
    /// string in the source (so a bare `true`/number can be distinguished from a
    /// quoted one where it matters).
    ///
    /// # Errors
    /// [`ValueError::BadLiteral`] if the text is not a valid literal for `self`.
    pub fn parse_literal(self, text: &str, quoted: bool) -> Result<CqlValue, ValueError> {
        let bad = || ValueError::BadLiteral {
            ty: self,
            literal: text.to_owned(),
        };
        match self {
            CqlType::Text => Ok(CqlValue::Text(text.to_owned())),
            CqlType::Int => text.parse::<i32>().map(CqlValue::Int).map_err(|_| bad()),
            CqlType::BigInt => text.parse::<i64>().map(CqlValue::BigInt).map_err(|_| bad()),
            CqlType::Boolean => match text.to_ascii_lowercase().as_str() {
                "true" => Ok(CqlValue::Boolean(true)),
                "false" => Ok(CqlValue::Boolean(false)),
                _ => Err(bad()),
            },
            CqlType::Blob => {
                // `0x...` hex literal, or (when quoted) the raw string's bytes.
                if let Some(hex) = text.strip_prefix("0x").or_else(|| text.strip_prefix("0X")) {
                    parse_hex(hex).map(CqlValue::Blob).ok_or_else(bad)
                } else if quoted {
                    Ok(CqlValue::Blob(text.as_bytes().to_vec()))
                } else {
                    Err(bad())
                }
            }
            CqlType::Uuid => parse_uuid(text).map(CqlValue::Uuid).ok_or_else(bad),
        }
    }
}

impl fmt::Display for CqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A typed CQL value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CqlValue {
    /// `text`.
    Text(String),
    /// `int`.
    Int(i32),
    /// `bigint`.
    BigInt(i64),
    /// `boolean`.
    Boolean(bool),
    /// `blob`.
    Blob(Vec<u8>),
    /// `uuid` (16 raw bytes).
    Uuid([u8; 16]),
}

impl CqlValue {
    /// The cell bytes for this value (the contents of a protocol `[bytes]`).
    #[must_use]
    pub fn to_cell_bytes(&self) -> Vec<u8> {
        match self {
            CqlValue::Text(s) => s.as_bytes().to_vec(),
            CqlValue::Int(n) => n.to_be_bytes().to_vec(),
            CqlValue::BigInt(n) => n.to_be_bytes().to_vec(),
            CqlValue::Boolean(b) => vec![u8::from(*b)],
            CqlValue::Blob(b) => b.clone(),
            CqlValue::Uuid(u) => u.to_vec(),
        }
    }

    /// The `CqlType` of this value.
    #[must_use]
    pub fn cql_type(&self) -> CqlType {
        match self {
            CqlValue::Text(_) => CqlType::Text,
            CqlValue::Int(_) => CqlType::Int,
            CqlValue::BigInt(_) => CqlType::BigInt,
            CqlValue::Boolean(_) => CqlType::Boolean,
            CqlValue::Blob(_) => CqlType::Blob,
            CqlValue::Uuid(_) => CqlType::Uuid,
        }
    }

    /// A canonical, order-preserving byte encoding suitable for a data-plane
    /// **key** component. Text/blob keep their bytes; fixed-width numbers use
    /// their big-endian form with the sign bit flipped so the byte order matches
    /// the numeric order; booleans and uuids use their cell bytes. This is used
    /// only to derive a stable partition-key encoding, not for the value cells.
    #[must_use]
    pub fn to_key_bytes(&self) -> Vec<u8> {
        match self {
            CqlValue::Int(n) => (*n as u32 ^ 0x8000_0000).to_be_bytes().to_vec(),
            CqlValue::BigInt(n) => (*n as u64 ^ 0x8000_0000_0000_0000).to_be_bytes().to_vec(),
            other => other.to_cell_bytes(),
        }
    }

    /// Render this value as a human-readable string (for `text`-style display /
    /// debugging). UUIDs use the canonical `8-4-4-4-12` hyphenated hex form;
    /// blobs use `0x`-prefixed hex.
    #[must_use]
    pub fn display(&self) -> String {
        match self {
            CqlValue::Text(s) => s.clone(),
            CqlValue::Int(n) => n.to_string(),
            CqlValue::BigInt(n) => n.to_string(),
            CqlValue::Boolean(b) => b.to_string(),
            CqlValue::Blob(b) => format!("0x{}", to_hex(b)),
            CqlValue::Uuid(u) => format_uuid(u),
        }
    }
}

/// Why a value failed to encode/decode/parse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueError {
    /// A value's variant did not match the column's declared type.
    TypeMismatch {
        /// The column's declared type.
        expected: CqlType,
        /// The value that did not fit it.
        got: CqlValue,
    },
    /// Cell bytes were the wrong length / shape for the type.
    BadEncoding(CqlType),
    /// A textual literal was not valid for the type.
    BadLiteral {
        /// The target type.
        ty: CqlType,
        /// The offending literal text.
        literal: String,
    },
}

impl fmt::Display for ValueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValueError::TypeMismatch { expected, got } => {
                write!(f, "expected a {expected} value, got {got:?}")
            }
            ValueError::BadEncoding(ty) => write!(f, "malformed {ty} value bytes"),
            ValueError::BadLiteral { ty, literal } => {
                write!(f, "`{literal}` is not a valid {ty} literal")
            }
        }
    }
}

impl std::error::Error for ValueError {}

/// Parse a hex string (even length) into bytes. Returns `None` on odd length or
/// a non-hex digit.
fn parse_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = (bytes[i] as char).to_digit(16)?;
        let lo = (bytes[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(out)
}

/// Render bytes as lowercase hex.
fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((b & 0x0f) as u32, 16).unwrap());
    }
    s
}

/// Parse a canonical hyphenated UUID (`8-4-4-4-12` hex) into 16 bytes. Hyphens
/// are optional; any other punctuation is rejected.
fn parse_uuid(text: &str) -> Option<[u8; 16]> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let bytes = parse_hex(&hex)?;
    bytes.try_into().ok()
}

/// Format 16 bytes as a canonical hyphenated UUID string.
fn format_uuid(u: &[u8; 16]) -> String {
    let h = to_hex(u);
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_round_trips_through_cells() {
        let cases = [
            (CqlType::Text, CqlValue::Text("hello".into())),
            (CqlType::Int, CqlValue::Int(-7)),
            (CqlType::BigInt, CqlValue::BigInt(1 << 40)),
            (CqlType::Boolean, CqlValue::Boolean(true)),
            (CqlType::Boolean, CqlValue::Boolean(false)),
            (CqlType::Blob, CqlValue::Blob(vec![0, 1, 2, 255])),
            (CqlType::Uuid, CqlValue::Uuid([7u8; 16])),
        ];
        for (ty, val) in cases {
            let cell = ty.encode(&val).unwrap();
            assert_eq!(ty.decode(&cell).unwrap(), val, "round trip for {ty}");
        }
    }

    #[test]
    fn type_mismatch_is_rejected() {
        let err = CqlType::Int
            .encode(&CqlValue::Text("x".into()))
            .unwrap_err();
        assert!(matches!(err, ValueError::TypeMismatch { .. }));
    }

    #[test]
    fn literals_parse_per_type() {
        assert_eq!(
            CqlType::Int.parse_literal("42", false).unwrap(),
            CqlValue::Int(42)
        );
        assert_eq!(
            CqlType::Boolean.parse_literal("TRUE", false).unwrap(),
            CqlValue::Boolean(true)
        );
        assert_eq!(
            CqlType::BigInt.parse_literal("9000000000", false).unwrap(),
            CqlValue::BigInt(9_000_000_000)
        );
        assert!(CqlType::Int.parse_literal("notanum", false).is_err());
    }

    #[test]
    fn uuid_literal_round_trips_through_display() {
        let text = "550e8400-e29b-41d4-a716-446655440000";
        let v = CqlType::Uuid.parse_literal(text, false).unwrap();
        assert_eq!(v.display(), text);
    }

    #[test]
    fn blob_hex_literal_parses() {
        let v = CqlType::Blob.parse_literal("0xdeadbeef", false).unwrap();
        assert_eq!(v, CqlValue::Blob(vec![0xde, 0xad, 0xbe, 0xef]));
    }

    #[test]
    fn int_key_bytes_preserve_order() {
        let neg = CqlValue::Int(-1).to_key_bytes();
        let zero = CqlValue::Int(0).to_key_bytes();
        let pos = CqlValue::Int(1).to_key_bytes();
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn type_names_parse() {
        assert_eq!(CqlType::parse("VARCHAR"), Some(CqlType::Text));
        assert_eq!(CqlType::parse("BigInt"), Some(CqlType::BigInt));
        assert_eq!(CqlType::parse("uuid"), Some(CqlType::Uuid));
        assert_eq!(CqlType::parse("frozen<list>"), None);
    }
}
