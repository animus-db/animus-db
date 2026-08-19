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
//!     <col> <type>, ... , PRIMARY KEY (<pk> [, <ck1>, <ck2> ...])
//! )                                          -- WITH clause (e.g. CLUSTERING ORDER) ignored
//! INSERT INTO <table> (<c1>, <c2>, ...) VALUES (<v1>, <v2>, ...)
//! SELECT * | <c1>, <c2>, ... FROM <table> WHERE <pk> = <value> [AND <ck> = <value> ...]
//! UPDATE <table> SET <col> = <value>, ... WHERE <pk> = <value> [AND <ck> = <value> ...]
//! DELETE FROM <table> WHERE <pk> = <value> [AND <ck> = <value> ...]
//! DROP TABLE [IF EXISTS] <table>
//! ALTER TABLE <table> ADD <col> <type> [, <col> <type> ...]
//! BEGIN [UNLOGGED|LOGGED] BATCH <mutation>; ... APPLY BATCH
//! ```
//!
//! Values are literals — single-quoted strings (`'ada'`, `''`-escaped), bare
//! numbers/words (`42`, `true`, a `uuid`/`0x..blob` literal), or a `?` bind
//! marker (filled in at `EXECUTE` time). Table names may be `keyspace.table`.
//!
//! It is **not** a CQL grammar: a single-column partition key only (compound
//! clustering keys *are* supported, composite partition keys are not), equality
//! predicates only (no ranges/ordering/`LIMIT`), no `IF`/`USING`, no functions.

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
    /// `SELECT ... FROM <table> WHERE <pk> = <value> [AND <ck> = <value> ...]`.
    Select(Select),
    /// `UPDATE <table> SET ... WHERE <pk> = <value> [AND <ck> = <value> ...]`.
    Update(Update),
    /// `DELETE FROM <table> WHERE <pk> = <value> [AND <ck> = <value> ...]`.
    Delete(Delete),
    /// `DROP TABLE [IF EXISTS] <table>`.
    DropTable(DropTable),
    /// `ALTER TABLE <table> ADD <col> <type> [, <col> <type> ...]`.
    AlterTable(AlterTable),
    /// `BEGIN [UNLOGGED|LOGGED] BATCH <stmt>; ... APPLY BATCH` — a sequence of
    /// mutation statements (`INSERT`/`UPDATE`/`DELETE`) applied together.
    Batch(Batch),
}

/// A parsed `DROP TABLE [IF EXISTS] <table>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTable {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// Whether `IF EXISTS` was present (drop is then a no-op on a missing table).
    pub if_exists: bool,
}

/// A parsed `ALTER TABLE <table> ADD ...`. Only `ADD <col> <type>` is supported
/// (the common, schema-compatible alteration); `DROP`/`RENAME`/`WITH` are
/// rejected as unsupported.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTable {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The columns to add: `(name, type)`, in declaration order.
    pub add_columns: Vec<(String, CqlType)>,
}

/// A parsed `BATCH`: a sequence of mutation statements applied together. Each
/// member is an `INSERT`, `UPDATE`, or `DELETE` (no nested batches, no
/// `SELECT`/DDL inside a batch).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Batch {
    /// The batched mutation statements, in source order.
    pub statements: Vec<Statement>,
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
    /// The clustering-key column names, in clustering order (may be empty).
    pub clustering_keys: Vec<String>,
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

/// One `<column> = <term>` equality predicate from a `WHERE` clause.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Predicate {
    /// The predicate column name.
    pub column: String,
    /// The predicate value (literal or bind marker).
    pub value: Term,
}

/// A parsed `SELECT` (projection + a `pk = value [AND ck = value ...]` `WHERE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Select {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The projected columns; empty means `*` (all columns).
    pub projection: Vec<String>,
    /// The `WHERE` equality predicates (partition key, then any clustering keys
    /// in order). A partition-key-only `WHERE` selects the whole partition.
    pub predicates: Vec<Predicate>,
}

/// A parsed `UPDATE` (assignments + a full primary-key `WHERE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Update {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The `SET` assignments: `(column, value)`, positionally ordered.
    pub assignments: Vec<(String, Term)>,
    /// The `WHERE` equality predicates (partition key + every clustering key).
    pub predicates: Vec<Predicate>,
}

/// A parsed `DELETE` (whole-row delete by a full primary-key `WHERE`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Delete {
    /// Optional `keyspace.` qualifier.
    pub keyspace: Option<String>,
    /// The table name.
    pub table: String,
    /// The `WHERE` equality predicates (partition key, then any clustering keys
    /// in order). A partition-key-only `WHERE` deletes the whole partition.
    pub predicates: Vec<Predicate>,
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
    let trimmed = cql.trim();
    if trimmed.is_empty() {
        return Err(QueryError::Empty);
    }
    // A BATCH spans several `;`-separated statements, so it is parsed from the raw
    // text before the single-statement `;` trimming below would mangle it.
    if trimmed
        .split_whitespace()
        .next()
        .is_some_and(|w| w.eq_ignore_ascii_case("begin"))
    {
        return parse_batch(trimmed);
    }
    let trimmed = trimmed.trim_end_matches(';').trim();
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
        Some("update") => parse_update(&mut it),
        Some("delete") => parse_delete(&mut it),
        Some("drop") => parse_drop(&mut it),
        Some("alter") => parse_alter(&mut it),
        _ => Err(QueryError::Unsupported(
            "expected USE, CREATE, INSERT, SELECT, UPDATE, DELETE, DROP, ALTER, or BATCH".into(),
        )),
    }
}

/// Parse a `BEGIN [UNLOGGED|LOGGED] BATCH <stmt>; <stmt>; ... APPLY BATCH`. The
/// inner statements must be mutations (`INSERT`/`UPDATE`/`DELETE`); a nested
/// batch, a `SELECT`, or DDL inside the batch is rejected. Batch-level options
/// (`UNLOGGED`, `LOGGED`, `USING TIMESTAMP`) are accepted and ignored — this
/// subset applies the members in order with no atomicity guarantee across them
/// (documented).
fn parse_batch(raw: &str) -> Result<Statement, QueryError> {
    // Strip the `BEGIN [UNLOGGED|LOGGED|COUNTER] BATCH` prefix and the trailing
    // `APPLY BATCH`, then split the middle on unquoted `;` into member
    // statements. `find`/`rfind` (not a quote-aware scan) are safe here even
    // though a member's own text literal could coincidentally contain the
    // substring "batch"/"apply": the grammar requires the real `BATCH`
    // keyword to be the *first* thing in `raw` (nothing precedes
    // `BEGIN [modifier] BATCH`) and the real `APPLY` keyword to be the
    // *last* (nothing legitimately follows `APPLY BATCH`), and any literal
    // occurrence sits strictly between those two extremes — so leftmost/
    // rightmost search always lands on the genuine keyword, never inside a
    // literal.
    let lower = raw.to_ascii_lowercase();
    let begin = lower
        .find("batch")
        .ok_or_else(|| QueryError::Unsupported("BEGIN without BATCH".into()))?;
    let apply = lower
        .rfind("apply")
        .ok_or_else(|| QueryError::Unsupported("BATCH without APPLY BATCH".into()))?;
    if apply <= begin {
        return Err(QueryError::Unsupported("malformed BATCH".into()));
    }
    let body = &raw[begin + "batch".len()..apply];
    let mut statements = Vec::new();
    // Quote-aware split — unlike the plain `body.split(';')` this replaced,
    // a `;` inside a single-quoted text literal (e.g. `VALUES (1, 'a;b')`)
    // does not split a member in two.
    for piece in split_unquoted_semicolons(body) {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        let stmt = parse_statement(piece)?;
        match stmt {
            Statement::Insert(_) | Statement::Update(_) | Statement::Delete(_) => {
                statements.push(stmt);
            }
            _ => {
                return Err(QueryError::Unsupported(
                    "a BATCH may only contain INSERT, UPDATE, or DELETE statements".into(),
                ));
            }
        }
    }
    if statements.is_empty() {
        return Err(QueryError::Unsupported("empty BATCH".into()));
    }
    Ok(Statement::Batch(Batch { statements }))
}

fn parse_drop(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "drop")?;
    // Only `DROP TABLE` is supported (not `DROP KEYSPACE`/`INDEX`/`TYPE`).
    if !it.peek().is_some_and(|t| t.eq_ignore_ascii_case("table")) {
        return Err(QueryError::Unsupported(
            "only DROP TABLE is supported".into(),
        ));
    }
    expect_kw(it, "table")?;
    let if_exists = take_if_exists(it)?;
    let (keyspace, table) = next_table_name(it)?;
    end(it, "DROP TABLE")?;
    Ok(Statement::DropTable(DropTable {
        keyspace,
        table,
        if_exists,
    }))
}

fn parse_alter(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "alter")?;
    if !it.peek().is_some_and(|t| t.eq_ignore_ascii_case("table")) {
        return Err(QueryError::Unsupported(
            "only ALTER TABLE is supported".into(),
        ));
    }
    expect_kw(it, "table")?;
    let (keyspace, table) = next_table_name(it)?;
    // Only `ADD` is supported (the schema-compatible alteration).
    if !it.peek().is_some_and(|t| t.eq_ignore_ascii_case("add")) {
        return Err(QueryError::Unsupported(
            "only ALTER TABLE ... ADD <col> <type> is supported".into(),
        ));
    }
    expect_kw(it, "add")?;
    // `ADD (a int, b text)` or `ADD a int [, b text ...]` — accept both shapes.
    let parenthesized = it.peek() == Some("(");
    if parenthesized {
        expect_punct(it, "(")?;
    }
    let mut add_columns = Vec::new();
    loop {
        let name = next_ident(it, "column name")?;
        let type_name = next_ident(it, "column type")?;
        let ty = CqlType::parse(&type_name).ok_or_else(|| {
            QueryError::Unsupported(format!("unsupported column type `{type_name}`"))
        })?;
        add_columns.push((name, ty));
        if it.peek() == Some(",") {
            it.next();
        } else {
            break;
        }
    }
    if parenthesized {
        expect_punct(it, ")")?;
    }
    end(it, "ALTER TABLE")?;
    Ok(Statement::AlterTable(AlterTable {
        keyspace,
        table,
        add_columns,
    }))
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
    let mut clustering_keys: Vec<String> = Vec::new();
    loop {
        // A `PRIMARY KEY (pk [, ck ...])` clause, or a `<name> <type> [PRIMARY KEY]`.
        if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("primary")) {
            expect_kw(it, "primary")?;
            expect_kw(it, "key")?;
            expect_punct(it, "(")?;
            // A composite partition key — `PRIMARY KEY ((a, b), c)` — is not
            // supported; reject the parenthesized form loudly.
            if it.peek() == Some("(") {
                return Err(QueryError::Unsupported(
                    "composite (multi-column) partition keys are not supported yet".into(),
                ));
            }
            let pk = next_ident(it, "primary key column")?;
            // Any further comma-separated columns are clustering keys.
            while it.peek() == Some(",") {
                it.next();
                clustering_keys.push(next_ident(it, "clustering key column")?);
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
    // An optional trailing `WITH ...` clause (e.g. `WITH CLUSTERING ORDER BY
    // (...)`) is accepted and ignored — this subset stores rows in ascending
    // clustering order only.
    if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("with")) {
        while it.next().is_some() {}
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
    // The partition key + every clustering key must be a declared column, and no
    // primary-key column may be named twice.
    let mut seen: Vec<String> = Vec::new();
    for name in std::iter::once(&partition_key).chain(clustering_keys.iter()) {
        if !columns.iter().any(|(n, _)| n.eq_ignore_ascii_case(name)) {
            return Err(QueryError::Unsupported(format!(
                "PRIMARY KEY column `{name}` is not a declared column"
            )));
        }
        if seen.iter().any(|s| s.eq_ignore_ascii_case(name)) {
            return Err(QueryError::Unsupported(format!(
                "PRIMARY KEY column `{name}` declared twice"
            )));
        }
        seen.push(name.clone());
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
        clustering_keys,
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
    let predicates = parse_where(it)?;
    end(it, "SELECT")?;
    Ok(Statement::Select(Select {
        keyspace,
        table,
        projection,
        predicates,
    }))
}

fn parse_update(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "update")?;
    let (keyspace, table) = next_table_name(it)?;
    expect_kw(it, "set")?;
    let mut assignments = Vec::new();
    loop {
        let column = next_ident(it, "assignment column")?;
        expect_punct(it, "=")?;
        let value = next_term(it, "assignment value")?;
        assignments.push((column, value));
        if it.peek() == Some(",") {
            it.next();
        } else {
            break;
        }
    }
    if assignments.is_empty() {
        return Err(QueryError::Unsupported(
            "UPDATE has no SET assignments".into(),
        ));
    }
    expect_kw(it, "where")?;
    let predicates = parse_where(it)?;
    end(it, "UPDATE")?;
    Ok(Statement::Update(Update {
        keyspace,
        table,
        assignments,
        predicates,
    }))
}

fn parse_delete(it: &mut TokenStream<'_>) -> Result<Statement, QueryError> {
    expect_kw(it, "delete")?;
    // This subset deletes whole rows only — no per-column `DELETE a, b FROM ...`.
    if !it.peek().is_some_and(|t| t.eq_ignore_ascii_case("from")) {
        return Err(QueryError::Unsupported(
            "only whole-row DELETE (`DELETE FROM <table> WHERE ...`) is supported".into(),
        ));
    }
    expect_kw(it, "from")?;
    let (keyspace, table) = next_table_name(it)?;
    expect_kw(it, "where")?;
    let predicates = parse_where(it)?;
    end(it, "DELETE")?;
    Ok(Statement::Delete(Delete {
        keyspace,
        table,
        predicates,
    }))
}

/// Parse a `WHERE` clause of one or more `<column> = <term>` equality
/// predicates joined by `AND`. Only equality is supported (no ranges/`IN`).
fn parse_where(it: &mut TokenStream<'_>) -> Result<Vec<Predicate>, QueryError> {
    let mut predicates = Vec::new();
    loop {
        let column = next_ident(it, "predicate column")?;
        expect_punct(it, "=")?;
        let value = next_term(it, "predicate value")?;
        predicates.push(Predicate { column, value });
        if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("and")) {
            it.next();
        } else {
            break;
        }
    }
    Ok(predicates)
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

fn take_if_exists(it: &mut TokenStream<'_>) -> Result<bool, QueryError> {
    if it.peek().is_some_and(|t| t.eq_ignore_ascii_case("if")) {
        expect_kw(it, "if")?;
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

/// Split `s` on unquoted top-level `;` characters, honoring the **same**
/// single-quoted-string escaping rule [`tokenize`]'s quote arm applies
/// (`'...'`, with `''` as an escaped quote) — so a `;` inside a text literal
/// (e.g. `VALUES (1, 'a;b')`) is never mistaken for a `BATCH` member
/// separator. Used only by [`parse_batch`], which needs the raw member
/// substrings (to recursively call [`parse_statement`] on each), not a
/// token list — so this can't just reuse [`tokenize`] outright (it discards
/// whitespace and does not treat `;` as a delimiter at all). Keep this in
/// sync with `tokenize`'s quote handling if that rule ever changes.
fn split_unquoted_semicolons(s: &str) -> Vec<&str> {
    let mut pieces = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut chars = s.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if in_quote {
            if c == '\'' {
                if chars.peek().is_some_and(|&(_, n)| n == '\'') {
                    chars.next(); // `''` inside a string is an escaped quote.
                } else {
                    in_quote = false;
                }
            }
        } else if c == '\'' {
            in_quote = true;
        } else if c == ';' {
            pieces.push(&s[start..i]);
            start = i + 1; // `;` is one ASCII byte.
        }
    }
    pieces.push(&s[start..]);
    pieces
}

/// Build the data-plane (engine) key for a partition (ADR 0023):
/// `partition_token(pk_bytes) || pk_bytes`.
///
/// **No table prefix** — a CQL partition is one CP value whose tablet is scoped to
/// one table (its own engine), so the table is the routing argument, not key bytes.
/// The Murmur3 token (fixed 8 bytes) spreads partitions across the table's ring;
/// `pk_bytes` is the type-canonical partition-key encoding (see
/// [`crate::types::CqlValue::to_key_bytes`]) and follows the token so two partition
/// keys that hash alike stay distinct.
#[must_use]
pub fn data_key(pk_bytes: &[u8]) -> Vec<u8> {
    let mut key = animus_tablet::partition_token(pk_bytes).to_vec();
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
    fn parses_compound_primary_key() {
        let stmt =
            parse_statement("CREATE TABLE t (a int, b int, c text, PRIMARY KEY (a, b))").unwrap();
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(ct.partition_key, "a");
        assert_eq!(ct.clustering_keys, vec!["b"]);
    }

    #[test]
    fn rejects_composite_partition_key() {
        let err =
            parse_statement("CREATE TABLE t (a int, b int, PRIMARY KEY ((a, b)))").unwrap_err();
        assert!(matches!(err, QueryError::Unsupported(_)));
    }

    #[test]
    fn ignores_clustering_order_with_clause() {
        let stmt = parse_statement(
            "CREATE TABLE t (a int, b int, PRIMARY KEY (a, b)) WITH CLUSTERING ORDER BY (b DESC)",
        )
        .unwrap();
        let Statement::CreateTable(ct) = stmt else {
            panic!("expected create table");
        };
        assert_eq!(ct.clustering_keys, vec!["b"]);
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
        assert_eq!(s.predicates.len(), 1);
        assert_eq!(s.predicates[0].column, "id");
        assert_eq!(s.predicates[0].value, Term::Bind);

        let proj = parse_statement("SELECT name, age FROM users WHERE id = 'u1'").unwrap();
        let Statement::Select(s) = proj else {
            panic!("expected select")
        };
        assert_eq!(s.projection, vec!["name", "age"]);
    }

    #[test]
    fn parses_select_with_clustering_predicate() {
        let s = parse_statement("SELECT * FROM t WHERE pk = 'a' AND ck = 3").unwrap();
        let Statement::Select(s) = s else {
            panic!("expected select")
        };
        assert_eq!(s.predicates.len(), 2);
        assert_eq!(s.predicates[1].column, "ck");
        assert_eq!(
            s.predicates[1].value,
            Term::Literal {
                text: "3".into(),
                quoted: false
            }
        );
    }

    #[test]
    fn parses_update() {
        let s = parse_statement("UPDATE users SET name = 'Ada', age = ? WHERE id = 7").unwrap();
        let Statement::Update(u) = s else {
            panic!("expected update")
        };
        assert_eq!(u.assignments.len(), 2);
        assert_eq!(u.assignments[0].0, "name");
        assert_eq!(u.assignments[1].1, Term::Bind);
        assert_eq!(u.predicates[0].column, "id");
    }

    #[test]
    fn parses_delete() {
        let s = parse_statement("DELETE FROM users WHERE id = 7 AND ck = 'x'").unwrap();
        let Statement::Delete(d) = s else {
            panic!("expected delete")
        };
        assert_eq!(d.table, "users");
        assert_eq!(d.predicates.len(), 2);
    }

    #[test]
    fn rejects_unknown_statements() {
        assert!(matches!(
            parse_statement("TRUNCATE t"),
            Err(QueryError::Unsupported(_))
        ));
        assert!(matches!(
            parse_statement("DROP KEYSPACE app"),
            Err(QueryError::Unsupported(_))
        ));
        assert!(matches!(parse_statement("   "), Err(QueryError::Empty)));
    }

    #[test]
    fn parses_drop_table() {
        let s = parse_statement("DROP TABLE app.users").unwrap();
        let Statement::DropTable(d) = s else {
            panic!("expected drop table")
        };
        assert_eq!(d.keyspace.as_deref(), Some("app"));
        assert_eq!(d.table, "users");
        assert!(!d.if_exists);

        let s = parse_statement("DROP TABLE IF EXISTS users").unwrap();
        let Statement::DropTable(d) = s else {
            panic!("expected drop table")
        };
        assert!(d.if_exists);
    }

    #[test]
    fn parses_alter_table_add() {
        let s = parse_statement("ALTER TABLE users ADD nickname text").unwrap();
        let Statement::AlterTable(a) = s else {
            panic!("expected alter table")
        };
        assert_eq!(a.table, "users");
        assert_eq!(a.add_columns, vec![("nickname".into(), CqlType::Text)]);

        // Multi-column ADD, parenthesized.
        let s = parse_statement("ALTER TABLE users ADD (a int, b bigint)").unwrap();
        let Statement::AlterTable(a) = s else {
            panic!("expected alter table")
        };
        assert_eq!(
            a.add_columns,
            vec![("a".into(), CqlType::Int), ("b".into(), CqlType::BigInt)]
        );

        // ALTER ... DROP is unsupported.
        assert!(matches!(
            parse_statement("ALTER TABLE users DROP nickname"),
            Err(QueryError::Unsupported(_))
        ));
    }

    #[test]
    fn parses_batch_of_mutations() {
        let s = parse_statement(
            "BEGIN BATCH \
             INSERT INTO users (id, name) VALUES (1, 'a'); \
             UPDATE users SET name = 'b' WHERE id = 2; \
             DELETE FROM users WHERE id = 3; \
             APPLY BATCH",
        )
        .unwrap();
        let Statement::Batch(b) = s else {
            panic!("expected batch")
        };
        assert_eq!(b.statements.len(), 3);
        assert!(matches!(b.statements[0], Statement::Insert(_)));
        assert!(matches!(b.statements[1], Statement::Update(_)));
        assert!(matches!(b.statements[2], Statement::Delete(_)));
    }

    /// Regression: a BATCH member's own text literal containing a `;` used
    /// to split the member in two (raw `body.split(';')`, bypassing the
    /// quote-aware `tokenize`), wrongly rejecting an otherwise-valid batch.
    #[test]
    fn batch_member_semicolon_inside_a_literal_does_not_split_the_member() {
        let s = parse_statement(
            "BEGIN BATCH \
             INSERT INTO t (id, note) VALUES (1, 'a;b'); \
             APPLY BATCH",
        )
        .unwrap();
        let Statement::Batch(b) = s else {
            panic!("expected batch")
        };
        assert_eq!(b.statements.len(), 1);
        let Statement::Insert(ins) = &b.statements[0] else {
            panic!("expected insert");
        };
        assert_eq!(
            ins.values[1],
            Term::Literal {
                text: "a;b".to_owned(),
                quoted: true,
            }
        );
    }

    #[test]
    fn batch_accepts_unlogged_option() {
        let s = parse_statement("BEGIN UNLOGGED BATCH INSERT INTO t (id) VALUES (1); APPLY BATCH")
            .unwrap();
        assert!(matches!(s, Statement::Batch(_)));
    }

    #[test]
    fn batch_rejects_select_member() {
        assert!(matches!(
            parse_statement("BEGIN BATCH SELECT * FROM t WHERE id = 1; APPLY BATCH"),
            Err(QueryError::Unsupported(_))
        ));
    }

    #[test]
    fn data_key_disambiguates_partition_keys() {
        // Distinct partition keys yield distinct data keys (the table is no longer
        // in the key — tables are separated by per-table tablets, ADR 0023).
        assert_ne!(data_key(b"c"), data_key(b"bc"));
        assert_eq!(data_key(b"c"), data_key(b"c"));
    }
}
