//! A deliberately tiny CQL recognizer for the minimal `QUERY` path.
//!
//! This is **not** a CQL grammar. It recognizes exactly two statement shapes
//! and pulls out the table, primary key, and (for inserts) the value. Anything
//! else is rejected with [`QueryError::Unsupported`], which the wire layer turns
//! into a CQL `ERROR` frame.
//!
//! Accepted shapes (case-insensitive keywords, optional trailing `;`):
//!
//! ```cql
//! INSERT INTO <table> (pk, v) VALUES (<pk>, <v>)
//! SELECT * FROM <table> WHERE pk = <pk>
//! ```
//!
//! Identifiers are bare words; literals are either single-quoted strings
//! (`'ada'`, with `''` as an escaped quote) or bare numeric/word tokens. The
//! `(pk, v)` column list is fixed by the no-schema convention documented in the
//! crate root: a row is a single `(pk, v)` pair. Column names other than `pk`
//! and `v` are rejected so a typo fails loudly rather than silently writing the
//! wrong shape.

use std::fmt;

/// A recognized CQL statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Query {
    /// `INSERT INTO t (pk, v) VALUES (pk, v)`.
    Insert {
        /// The table name.
        table: String,
        /// The partition-key value (raw text bytes).
        pk: String,
        /// The `v` column value (raw text bytes).
        value: String,
    },
    /// `SELECT * FROM t WHERE pk = pk`.
    Select {
        /// The table name.
        table: String,
        /// The partition-key value to look up.
        pk: String,
    },
}

/// Why a query string was not recognized.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryError {
    /// The statement is empty or only whitespace.
    Empty,
    /// The statement does not match a supported shape (with a human reason).
    Unsupported(String),
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::Empty => write!(f, "empty query"),
            QueryError::Unsupported(why) => write!(f, "unsupported query: {why}"),
        }
    }
}

impl std::error::Error for QueryError {}

/// The convention partition-key column name (no schema catalog yet).
pub const PK_COLUMN: &str = "pk";
/// The convention value column name.
pub const V_COLUMN: &str = "v";

/// Parse a CQL query string into a [`Query`], or fail.
///
/// # Errors
/// [`QueryError::Empty`] for blank input, [`QueryError::Unsupported`] for
/// anything outside the two accepted shapes.
pub fn parse_query(cql: &str) -> Result<Query, QueryError> {
    let trimmed = cql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(QueryError::Empty);
    }
    let tokens = tokenize(trimmed);
    let lower_first = tokens.first().map(|t| t.to_ascii_lowercase());
    match lower_first.as_deref() {
        Some("insert") => parse_insert(&tokens),
        Some("select") => parse_select(&tokens),
        _ => Err(QueryError::Unsupported(
            "only INSERT and SELECT are supported".into(),
        )),
    }
}

/// `INSERT INTO <table> ( pk , v ) VALUES ( <pk> , <v> )`.
fn parse_insert(tokens: &[String]) -> Result<Query, QueryError> {
    // tokens: insert into <table> ( pk , v ) values ( <pk> , <v> )
    let mut it = TokenStream::new(tokens);
    expect_kw(&mut it, "insert")?;
    expect_kw(&mut it, "into")?;
    let table = next_ident(&mut it, "table name")?;
    expect_punct(&mut it, "(")?;
    let col1 = next_ident(&mut it, "first column")?;
    expect_punct(&mut it, ",")?;
    let col2 = next_ident(&mut it, "second column")?;
    expect_punct(&mut it, ")")?;
    expect_kw(&mut it, "values")?;
    expect_punct(&mut it, "(")?;
    let val1 = next_literal(&mut it, "first value")?;
    expect_punct(&mut it, ",")?;
    let val2 = next_literal(&mut it, "second value")?;
    expect_punct(&mut it, ")")?;
    if it.next().is_some() {
        return Err(QueryError::Unsupported(
            "trailing tokens after INSERT".into(),
        ));
    }
    if !col1.eq_ignore_ascii_case(PK_COLUMN) || !col2.eq_ignore_ascii_case(V_COLUMN) {
        return Err(QueryError::Unsupported(format!(
            "columns must be ({PK_COLUMN}, {V_COLUMN}); got ({col1}, {col2})"
        )));
    }
    Ok(Query::Insert {
        table,
        pk: val1,
        value: val2,
    })
}

/// `SELECT * FROM <table> WHERE pk = <pk>`.
fn parse_select(tokens: &[String]) -> Result<Query, QueryError> {
    let mut it = TokenStream::new(tokens);
    expect_kw(&mut it, "select")?;
    expect_punct(&mut it, "*")?;
    expect_kw(&mut it, "from")?;
    let table = next_ident(&mut it, "table name")?;
    expect_kw(&mut it, "where")?;
    let col = next_ident(&mut it, "predicate column")?;
    expect_punct(&mut it, "=")?;
    let pk = next_literal(&mut it, "predicate value")?;
    if it.next().is_some() {
        return Err(QueryError::Unsupported(
            "trailing tokens after SELECT".into(),
        ));
    }
    if !col.eq_ignore_ascii_case(PK_COLUMN) {
        return Err(QueryError::Unsupported(format!(
            "WHERE column must be {PK_COLUMN}; got {col}"
        )));
    }
    Ok(Query::Select { table, pk })
}

/// A simple cursor over the token slice yielding `&str`s.
struct TokenStream<'a> {
    tokens: &'a [String],
    pos: usize,
}

impl<'a> TokenStream<'a> {
    fn new(tokens: &'a [String]) -> Self {
        Self { tokens, pos: 0 }
    }
    fn next(&mut self) -> Option<&'a str> {
        let tok = self.tokens.get(self.pos)?;
        self.pos += 1;
        Some(tok.as_str())
    }
}

fn expect_kw(it: &mut TokenStream<'_>, kw: &str) -> Result<(), QueryError> {
    match it.next() {
        Some(tok) if tok.eq_ignore_ascii_case(kw) => Ok(()),
        other => Err(QueryError::Unsupported(format!(
            "expected `{kw}`, got {}",
            describe(other)
        ))),
    }
}

fn expect_punct(it: &mut TokenStream<'_>, p: &str) -> Result<(), QueryError> {
    match it.next() {
        Some(tok) if tok == p => Ok(()),
        other => Err(QueryError::Unsupported(format!(
            "expected `{p}`, got {}",
            describe(other)
        ))),
    }
}

fn next_ident(it: &mut TokenStream<'_>, what: &str) -> Result<String, QueryError> {
    match it.next() {
        Some(tok) if is_ident(tok) => Ok(tok.to_owned()),
        other => Err(QueryError::Unsupported(format!(
            "expected {what}, got {}",
            describe(other)
        ))),
    }
}

/// A literal is a quoted string (already unquoted by the tokenizer) or a bare
/// word/number token.
fn next_literal(it: &mut TokenStream<'_>, what: &str) -> Result<String, QueryError> {
    match it.next() {
        // A quoted-string token is prefixed with a NUL sentinel by the tokenizer
        // so `''` (an empty string) is distinguishable from a missing token and
        // a quoted keyword is not treated as punctuation.
        Some(tok) if tok.starts_with('\0') => Ok(tok[1..].to_owned()),
        Some(tok) if is_ident(tok) => Ok(tok.to_owned()),
        other => Err(QueryError::Unsupported(format!(
            "expected {what}, got {}",
            describe(other)
        ))),
    }
}

fn describe(tok: Option<&str>) -> String {
    match tok {
        None => "end of input".to_owned(),
        Some(t) if t.starts_with('\0') => format!("string `{}`", &t[1..]),
        Some(t) => format!("`{t}`"),
    }
}

fn is_ident(tok: &str) -> bool {
    !tok.is_empty()
        && !tok.starts_with('\0')
        && tok
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// Split a statement into tokens: identifiers/numbers, single punctuation
/// characters (`( ) , = *`), and single-quoted strings. A quoted string is
/// emitted with a leading `\0` sentinel so the parser can tell it apart from a
/// bare word (and accept an empty string).
fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&c) = chars.peek() {
        match c {
            c if c.is_whitespace() => {
                chars.next();
            }
            '\'' => {
                chars.next(); // opening quote
                let mut s = String::new();
                loop {
                    match chars.next() {
                        Some('\'') => {
                            // `''` inside a string is an escaped single quote.
                            if chars.peek() == Some(&'\'') {
                                chars.next();
                                s.push('\'');
                            } else {
                                break;
                            }
                        }
                        Some(other) => s.push(other),
                        None => break, // unterminated; take what we have
                    }
                }
                tokens.push(format!("\0{s}"));
            }
            '(' | ')' | ',' | '=' | '*' => {
                chars.next();
                tokens.push(c.to_string());
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || matches!(c, '(' | ')' | ',' | '=' | '*' | '\'') {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                tokens.push(word);
            }
        }
    }
    tokens
}

/// Build the data-plane key for a row: `escape(table) || pk_bytes`.
///
/// The table name is length-escaped (its UTF-8 bytes prefixed with a big-endian
/// `u32` length) so two tables never produce overlapping keys even when one
/// partition key is a prefix of another table+key concatenation. This mirrors
/// the framing `custos-dynamo` uses for the same reason.
#[must_use]
pub fn data_key(table: &str, pk: &str) -> Vec<u8> {
    let table_bytes = table.as_bytes();
    let pk_bytes = pk.as_bytes();
    let mut key = Vec::with_capacity(4 + table_bytes.len() + pk_bytes.len());
    key.extend_from_slice(&(table_bytes.len() as u32).to_be_bytes());
    key.extend_from_slice(table_bytes);
    key.extend_from_slice(pk_bytes);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_simple_insert() {
        let q = parse_query("INSERT INTO t (pk, v) VALUES ('a', 'hello')").unwrap();
        assert_eq!(
            q,
            Query::Insert {
                table: "t".into(),
                pk: "a".into(),
                value: "hello".into(),
            }
        );
    }

    #[test]
    fn parses_a_simple_select_with_trailing_semicolon() {
        let q = parse_query("SELECT * FROM t WHERE pk = 'a';").unwrap();
        assert_eq!(
            q,
            Query::Select {
                table: "t".into(),
                pk: "a".into(),
            }
        );
    }

    #[test]
    fn keywords_are_case_insensitive() {
        let q = parse_query("insert into Users (PK, V) values ('u1', '42')").unwrap();
        assert_eq!(
            q,
            Query::Insert {
                table: "Users".into(),
                pk: "u1".into(),
                value: "42".into(),
            }
        );
    }

    #[test]
    fn accepts_bare_numeric_values() {
        let q = parse_query("INSERT INTO t (pk, v) VALUES (7, 99)").unwrap();
        assert_eq!(
            q,
            Query::Insert {
                table: "t".into(),
                pk: "7".into(),
                value: "99".into(),
            }
        );
    }

    #[test]
    fn handles_escaped_quotes_and_empty_strings() {
        let q = parse_query("INSERT INTO t (pk, v) VALUES ('a', 'it''s')").unwrap();
        let Query::Insert { value, .. } = q else {
            panic!("expected insert")
        };
        assert_eq!(value, "it's");

        let q = parse_query("INSERT INTO t (pk, v) VALUES ('a', '')").unwrap();
        let Query::Insert { value, .. } = q else {
            panic!("expected insert")
        };
        assert_eq!(value, "");
    }

    #[test]
    fn rejects_wrong_columns() {
        let err = parse_query("INSERT INTO t (id, v) VALUES ('a', 'b')").unwrap_err();
        assert!(matches!(err, QueryError::Unsupported(_)));
    }

    #[test]
    fn rejects_unknown_statements() {
        assert!(matches!(
            parse_query("DELETE FROM t WHERE pk = 'a'"),
            Err(QueryError::Unsupported(_))
        ));
        assert!(matches!(parse_query("   "), Err(QueryError::Empty)));
    }

    #[test]
    fn data_key_disambiguates_tables() {
        // "ab" + key "c" must differ from "a" + key "bc".
        assert_ne!(data_key("ab", "c"), data_key("a", "bc"));
    }
}
