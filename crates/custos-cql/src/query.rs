//! A small (still not full-grammar) CQL recognizer.
//!
//! This parses a practical subset of CQL into a [`Statement`] tree without
//! resolving it against a schema — schema resolution lives in [`crate::plan`].
//! Anything outside the subset is rejected with [`QueryError::Unsupported`],
//! which the wire layer turns into a CQL `ERROR` frame.
//!
//! Accepted shapes (case-insensitive keywords, optional trailing `;`):
//!
//! ```cql
//! USE <keyspace>
//! CREATE KEYSPACE [IF NOT EXISTS] <keyspace> [WITH ...]   -- WITH clause ignored
//! CREATE TABLE [IF NOT EXISTS] <table> (
//!     <col> <type>, ... , PRIMARY KEY (<col>)
//! )
//! INSERT INTO <table> (<c1>, <c2>, ...) VALUES (<v1>, <v2>, ...)
//! SELECT * | <c1>, <c2>, ... FROM <table> WHERE <pk> = <value>
//! ```
//!
//! Values are literals — single-quoted strings (`'ada'`, `''`-escaped), bare
//! numbers/words (`42`, `true`, a `uuid`/`0x..blob` literal), or a `?` bind
//! marker (filled in at `EXECUTE` time). Table names may be `keyspace.table`.
//!
//! It is **not** a CQL grammar: no clustering columns, no composite partition
//! keys, no `IF`/`USING`/`LIMIT`/ordering, no functions.

use std::fmt;

use crate::types::CqlType;

/// A recognized CQL statement (pre-schema-resolution).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Statement {
    /// `USE <keyspace>`.
    Use {
        /// The keyspace to select for the connection.
        keyspace: String,
    },
    /// `CREATE KEYSPACE [IF NOT EXISTS] <keyspace> [WITH ...]`.
    CreateKeyspace {
        /// The keyspace name.
        keyspace: String,
        /// Whether `IF NOT EXISTS` was present.
        if_not_exists: bool,
    },
    /// `CREATE TABLE [IF NOT EXISTS] <table> (...)`.
    CreateTable(CreateTable),
    /// `INSERT INTO <table> (...) VALUES (...)`.
    Insert(Insert),
    /// `SELECT ... FROM <table> WHERE <pk> = <value>`.
    Select(Select),
}

/// A parsed `CREATE TABLE`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTable {
    /// Optional `keyspace.` qualifier on the table name.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// Whether `IF NOT EXISTS` was present.
    pub if_not_exists: bool,
    /// Columns in declaration order: `(name, type)`.
    pub columns: Vec<(String, CqlType)>,
    /// The partition-key column name (a single column in this subset).
    pub partition_key: String,
}

/// A parsed `INSERT` (columns + literal/bind values, not yet typed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Insert {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The named columns being written.
    pub columns: Vec<String>,
    /// The values, positionally aligned with `columns`.
    pub values: Vec<Term>,
}

/// A parsed `SELECT` (projection + single `pk = value` predicate).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The projected columns; empty means `*` (all columns).
    pub projection: Vec<String>,
    /// The `WHERE` predicate column (must be the partition key).
    pub where_column: String,
    /// The `WHERE` predicate value.
    pub where_value: Term,
}

/// A value position in a statement: a literal or a positional bind marker.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Term {
    /// A literal value as written. `quoted` records whether it was a quoted
    /// string (so a quoted number stays text and an unquoted `true` is a bool).
    Literal {
        /// The literal text (quotes already stripped).
        text: String,
        /// Whether the source literal was single-quoted.
        quoted: bool,
    },
    /// A `?` positional bind marker.
    Bind,
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

/// Parse a CQL query string into a [`Statement`], or fail.
///
/// # Errors
/// [`QueryError::Empty`] for blank input, [`QueryError::Unsupported`] for
/// anything outside the accepted subset.
pub fn parse_statement(cql: &str) -> Result<Statement, QueryError> {
    let trimmed = cql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Err(QueryError::Empty);
    }
    let tokens = tokenize(trimmed);
    let mut it = TokenStream::new(&tokens);
    let first = it.peek().map(str::to_ascii_lowercase);
    match first.as_deref() {
        Some("use") => parse_use(&mut it),
        Some("create") => parse_create(&mut it),
        Some("insert") => parse_insert(&mut it),
        Some("select") => parse_select(&mut it),
        _ => Err(QueryError::Unsupported(
            "expected USE, CREATE, INSERT, or SELECT".into(),
        )),
    }
}

fn parse_use(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "use")?;
    let keyspace = next_ident(it, "keyspace name")?;
    end(it, "USE")?;
    Ok(Statement::Use { keyspace })
}

fn parse_create(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "create")?;
    match it.peek().map(str::to_ascii_lowercase).as_deref() {
        Some("keyspace") => parse_create_keyspace(it),
        Some("table") => parse_create_table(it),
        other => Err(QueryError::Unsupported(format!(
            "expected KEYSPACE or TABLE after CREATE, got {}",
            describe(other)
        ))),
    }
}

fn parse_create_keyspace(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "keyspace")?;
    let if_not_exists = take_if_not_exists(it)?;
    let keyspace = next_ident(it, "keyspace name")?;
    // An optional `WITH replication = {...}` clause is accepted and ignored:
    // this single-cluster engine has no per-keyspace replication settings.
    if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("with")) {
        while it.next().is_some() {} // consume the rest of the statement
    }
    end(it, "CREATE KEYSPACE")?;
    Ok(Statement::CreateKeyspace {
        keyspace,
        if_not_exists,
    })
}

fn parse_create_table(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "table")?;
    let if_not_exists = take_if_not_exists(it)?;
    let (keyspace, table) = next_table_name(it)?;
    expect_punct(it, "(")?;

    let mut columns: Vec<(String, CqlType)> = Vec::new();
    let mut inline_pk: Option<String> = None;
    let mut pk_clause: Option<String> = None;
    loop {
        // A `PRIMARY KEY (col)` clause, or a `<name> <type> [PRIMARY KEY]`.
        if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("primary")) {
            expect_kw(it, "primary")?;
            expect_kw(it, "key")?;
            expect_punct(it, "(")?;
            let pk = next_ident(it, "primary key column")?;
            // Reject composite / clustering keys for now (loudly).
            if it.peek() == Some(",") {
                return Err(QueryError::Unsupported(
                    "composite / clustering primary keys are not supported yet".into(),
                ));
            }
            expect_punct(it, ")")?;
            pk_clause = Some(pk);
        } else {
            let name = next_ident(it, "column name")?;
            let type_name = next_ident(it, "column type")?;
            let ty = CqlType::parse(&type_name).ok_or_else(|| {
                QueryError::Unsupported(format!("unsupported column type `{type_name}`"))
            })?;
            // Inline `PRIMARY KEY` on the column.
            if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("primary")) {
                expect_kw(it, "primary")?;
                expect_kw(it, "key")?;
                inline_pk = Some(name.clone());
            }
            columns.push((name, ty));
        }
        match it.next() {
            Some(",") => continue,
            Some(")") => break,
            other => {
                return Err(QueryError::Unsupported(format!(
                    "expected `,` or `)` in column list, got {}",
                    describe(other)
                )));
            }
        }
    }
    end(it, "CREATE TABLE")?;

    let partition_key = match (inline_pk, pk_clause) {
        (Some(_), Some(_)) => {
            return Err(QueryError::Unsupported(
                "primary key declared twice (inline and PRIMARY KEY clause)".into(),
            ));
        }
        (Some(pk), None) | (None, Some(pk)) => pk,
        (None, None) => {
            return Err(QueryError::Unsupported("no PRIMARY KEY declared".into()));
        }
    };
    if !columns
        .iter()
        .any(|(n, _)| n.eq_ignore_ascii_case(&partition_key))
    {
        return Err(QueryError::Unsupported(format!(
            "PRIMARY KEY column `{partition_key}` is not a declared column"
        )));
    }
    if columns.is_empty() {
        return Err(QueryError::Unsupported("table has no columns".into()));
    }

    Ok(Statement::CreateTable(CreateTable {
        keyspace,
        table,
        if_not_exists,
        columns,
        partition_key,
    }))
}

fn parse_insert(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "insert")?;
    expect_kw(it, "into")?;
    let (keyspace, table) = next_table_name(it)?;
    expect_punct(it, "(")?;
    let mut columns = Vec::new();
    loop {
        columns.push(next_ident(it, "column name")?);
        match it.next() {
            Some(",") => continue,
            Some(")") => break,
            other => {
                return Err(QueryError::Unsupported(format!(
                    "expected `,` or `)` in column list, got {}",
                    describe(other)
                )));
            }
        }
    }
    expect_kw(it, "values")?;
    expect_punct(it, "(")?;
    let mut values = Vec::new();
    loop {
        values.push(next_term(it, "value")?);
        match it.next() {
            Some(",") => continue,
            Some(")") => break,
            other => {
                return Err(QueryError::Unsupported(format!(
                    "expected `,` or `)` in values, got {}",
                    describe(other)
                )));
            }
        }
    }
    end(it, "INSERT")?;
    if columns.len() != values.len() {
        return Err(QueryError::Unsupported(format!(
            "INSERT has {} columns but {} values",
            columns.len(),
            values.len()
        )));
    }
    Ok(Statement::Insert(Insert {
        keyspace,
        table,
        columns,
        values,
    }))
}

fn parse_select(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "select")?;
    let mut projection = Vec::new();
    if it.peek() == Some("*") {
        it.next();
    } else {
        loop {
            projection.push(next_ident(it, "projected column")?);
            if it.peek() == Some(",") {
                it.next();
            } else {
                break;
            }
        }
    }
    expect_kw(it, "from")?;
    let (keyspace, table) = next_table_name(it)?;
    expect_kw(it, "where")?;
    let where_column = next_ident(it, "predicate column")?;
    expect_punct(it, "=")?;
    let where_value = next_term(it, "predicate value")?;
    end(it, "SELECT")?;
    Ok(Statement::Select(Select {
        keyspace,
        table,
        projection,
        where_column,
        where_value,
    }))
}

// --- token-stream helpers ---------------------------------------------------

/// A cursor over the token slice yielding `&str`s, with a one-token lookahead.
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
    /// Peek at the next token *as written* (a quoted-string token keeps its `\0`
    /// sentinel here; callers comparing to keywords/punct never match it).
    fn peek(&self) -> Option<&'a str> {
        self.tokens.get(self.pos).map(String::as_str)
    }
}

fn take_if_not_exists(it: &mut TokenStream<'_>) -> Result<bool, QueryError> {
    if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("if")) {
        expect_kw(it, "if")?;
        expect_kw(it, "not")?;
        expect_kw(it, "exists")?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn end(it: &mut TokenStream<'_>, what: &str) -> Result<(), QueryError> {
    match it.next() {
        None => Ok(()),
        Some(tok) => Err(QueryError::Unsupported(format!(
            "trailing tokens after {what}: {}",
            describe(Some(tok))
        ))),
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

/// A possibly-qualified table name `[keyspace.]table`. The tokenizer keeps a
/// dotted identifier as one token, so we split on the first `.`.
fn next_table_name(it: &mut TokenStream<'_>) -> Result<(Option<String>, String), QueryError> {
    let raw = next_ident(it, "table name")?;
    match raw.split_once('.') {
        Some((ks, table)) if !ks.is_empty() && !table.is_empty() && !table.contains('.') => {
            Ok((Some(ks.to_owned()), table.to_owned()))
        }
        Some(_) => Err(QueryError::Unsupported(format!(
            "malformed table name `{raw}`"
        ))),
        None => Ok((None, raw)),
    }
}

/// A value term: a `?` bind marker, a quoted string, or a bare literal.
fn next_term(it: &mut TokenStream<'_>, what: &str) -> Result<Term, QueryError> {
    match it.next() {
        Some("?") => Ok(Term::Bind),
        // A quoted string is sentinel-prefixed by the tokenizer.
        Some(tok) if tok.starts_with('\0') => Ok(Term::Literal {
            text: tok[1..].to_owned(),
            quoted: true,
        }),
        Some(tok) if is_ident(tok) => Ok(Term::Literal {
            text: tok.to_owned(),
            quoted: false,
        }),
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
        && tok != "?"
        && tok
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-')
}

/// Split a statement into tokens: identifiers/numbers, single punctuation
/// characters (`( ) , = * ? { } :`), and single-quoted strings. A quoted string
/// is emitted with a leading `\0` sentinel so the parser can tell it apart from
/// a bare word (and accept an empty string).
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
            '(' | ')' | ',' | '=' | '*' | '?' | '{' | '}' | ':' => {
                chars.next();
                tokens.push(c.to_string());
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace()
                        || matches!(
                            c,
                            '(' | ')' | ',' | '=' | '*' | '?' | '\'' | '{' | '}' | ':'
                        )
                    {
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
/// the framing `custos-dynamo` uses for the same reason. `pk_bytes` is the
/// type-canonical key encoding (see [`crate::types::CqlValue::to_key_bytes`]).
#[must_use]
pub fn data_key(table: &str, pk_bytes: &[u8]) -> Vec<u8> {
    let table_bytes = table.as_bytes();
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
    fn parses_create_table() {
        let stmt = parse_statement(
            "CREATE TABLE app.users (id uuid, name text, age int, PRIMARY KEY (id))",
        )
        .unwrap();
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(ct.keyspace.as_deref(), Some("app"));
        assert_eq!(ct.table, "users");
        assert_eq!(ct.partition_key, "id");
        assert_eq!(ct.columns.len(), 3);
        assert_eq!(ct.columns[2], ("age".into(), CqlType::Int));
    }

    #[test]
    fn parses_inline_primary_key() {
        let stmt = parse_statement("CREATE TABLE t (k int PRIMARY KEY, v text)").unwrap();
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(ct.partition_key, "k");
    }

    #[test]
    fn rejects_composite_pk() {
        let err = parse_statement("CREATE TABLE t (a int, b int, PRIMARY KEY (a, b))").unwrap_err();
        assert!(matches!(err, QueryError::Unsupported(_)));
    }

    #[test]
    fn parses_use_and_create_keyspace() {
        assert_eq!(
            parse_statement("USE app").unwrap(),
            Statement::Use {
                keyspace: "app".into()
            }
        );
        let stmt = parse_statement("CREATE KEYSPACE IF NOT EXISTS app WITH replication = {'x': 1}")
            .unwrap();
        assert_eq!(
            stmt,
            Statement::CreateKeyspace {
                keyspace: "app".into(),
                if_not_exists: true,
            }
        );
    }

    #[test]
    fn parses_insert_with_binds_and_literals() {
        let stmt =
            parse_statement("INSERT INTO users (id, name, age) VALUES (?, 'Ada', 36)").unwrap();
        let Statement::Insert(ins) = stmt else {
            panic!("expected insert");
        };
        assert_eq!(ins.columns, vec!["id", "name", "age"]);
        assert_eq!(ins.values[0], Term::Bind);
        assert_eq!(
            ins.values[1],
            Term::Literal {
                text: "Ada".into(),
                quoted: true
            }
        );
        assert_eq!(
            ins.values[2],
            Term::Literal {
                text: "36".into(),
                quoted: false
            }
        );
    }

    #[test]
    fn parses_select_projection_and_star() {
        let star = parse_statement("SELECT * FROM users WHERE id = ?").unwrap();
        let Statement::Select(s) = star else {
            panic!("expected select")
        };
        assert!(s.projection.is_empty());
        assert_eq!(s.where_column, "id");
        assert_eq!(s.where_value, Term::Bind);

        let proj = parse_statement("SELECT name, age FROM users WHERE id = 'u1'").unwrap();
        let Statement::Select(s) = proj else {
            panic!("expected select")
        };
        assert_eq!(s.projection, vec!["name", "age"]);
    }

    #[test]
    fn rejects_unknown_statements() {
        assert!(matches!(
            parse_statement("DROP TABLE t"),
            Err(QueryError::Unsupported(_))
        ));
        assert!(matches!(parse_statement("   "), Err(QueryError::Empty)));
    }

    #[test]
    fn data_key_disambiguates_tables() {
        assert_ne!(data_key("ab", b"c"), data_key("a", b"bc"));
    }
}
