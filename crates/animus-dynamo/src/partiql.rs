//! PartiQL subset (ADR 0071, W-07): a minimal `SELECT` grammar, lowered onto
//! [`crate::wire::Operation::Query`]/[`crate::wire::Operation::Scan`] so
//! `animusd` needs no new data-plane primitive to serve `ExecuteStatement`.
//!
//! This module is **pure and deterministic** — no I/O, no catalog access, no
//! `Env` — matching every other module in this crate below the wire edge.
//! The lexer/parser (§3 of the ADR) is hand-written (no regex/parser-combinator
//! crate); ADR 0071 §3 explains why W-01's `UpdateExpression` string parser
//! (`crate::wire`) was not reused. `lower_select` builds
//! [`animus_item::condition`] types (`Comparator`/`SortKeyCondition`/
//! `ConditionExpression`) directly from the parsed AST rather than
//! round-tripping through `KeyConditionExpression`/`FilterExpression` text —
//! see ADR 0071 §5 for why.
//!
//! Only `SELECT` is implemented in this PR (W-07 PR 2). `INSERT`/`UPDATE`/
//! `DELETE` land in PR 3 — a statement beginning with one of those keywords
//! is rejected here with a clear "not supported yet" [`PartiqlError`].
//!
//! ## Placeholder-only discipline (ADR 0071 §2)
//!
//! Every value in a `WHERE` predicate must be a `?` placeholder, bound
//! **positionally** against the caller's own `Parameters` array — never a
//! literal written into the statement text. The lexer still recognizes a
//! quoted-string or numeric literal token (so a value position naming one
//! gets a specific, actionable error instead of a generic "unexpected
//! token").
//!
//! ## `NextToken` (ADR 0071 §9)
//!
//! [`encode_next_token`]/[`decode_next_token`] mint and verify the opaque,
//! versioned, statement-hash-bound pagination cursor `ExecuteStatement`
//! surfaces in place of `Query`/`Scan`'s transparent `LastEvaluatedKey`.

use std::fmt;

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::condition::{Comparator, ConditionExpression, SortKeyCondition};
use crate::wire::{Operation, PathSegment, Projection, Select, WireError};
use crate::{AttributeValue, Item};

/// A PartiQL parse/lowering failure. Every case in this module renders as a
/// DynamoDB `ValidationException` (`impl From<PartiqlError> for WireError`
/// below) — ADR 0071 §10.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartiqlError {
    pub message: String,
}

impl PartiqlError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for PartiqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for PartiqlError {}

impl From<PartiqlError> for WireError {
    fn from(err: PartiqlError) -> Self {
        WireError::validation(err.message)
    }
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Keyword(Keyword),
    /// A bare or double-quoted identifier — quoting is not retained past the
    /// lexer (ADR 0071 §1: a quoted identifier is never keyword-classified,
    /// but once past the lexer both forms are just an attribute/table name).
    Ident(String),
    Placeholder,
    Comparator(Comparator),
    Comma,
    Dot,
    Star,
    LParen,
    RParen,
    /// A single-quoted string literal — never legal in a value position
    /// (ADR 0071 §2); kept as a token so the parser can name it in an error.
    StringLiteral(String),
    /// A bare numeric literal — same "kept only to name it in an error" role
    /// as [`Self::StringLiteral`].
    NumberLiteral(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Keyword {
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    Order,
    By,
    Asc,
    Desc,
    Between,
    Insert,
    Update,
    Delete,
    Null,
    True,
    False,
}

impl Keyword {
    fn from_ident(s: &str) -> Option<Self> {
        Some(match s.to_ascii_uppercase().as_str() {
            "SELECT" => Keyword::Select,
            "FROM" => Keyword::From,
            "WHERE" => Keyword::Where,
            "AND" => Keyword::And,
            "OR" => Keyword::Or,
            "NOT" => Keyword::Not,
            "ORDER" => Keyword::Order,
            "BY" => Keyword::By,
            "ASC" => Keyword::Asc,
            "DESC" => Keyword::Desc,
            "BETWEEN" => Keyword::Between,
            "INSERT" => Keyword::Insert,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "NULL" => Keyword::Null,
            "TRUE" => Keyword::True,
            "FALSE" => Keyword::False,
            _ => return None,
        })
    }

    fn text(self) -> &'static str {
        match self {
            Keyword::Select => "SELECT",
            Keyword::From => "FROM",
            Keyword::Where => "WHERE",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::Not => "NOT",
            Keyword::Order => "ORDER",
            Keyword::By => "BY",
            Keyword::Asc => "ASC",
            Keyword::Desc => "DESC",
            Keyword::Between => "BETWEEN",
            Keyword::Insert => "INSERT",
            Keyword::Update => "UPDATE",
            Keyword::Delete => "DELETE",
            Keyword::Null => "NULL",
            Keyword::True => "TRUE",
            Keyword::False => "FALSE",
        }
    }
}

fn lex(input: &str) -> Result<Vec<Token>, PartiqlError> {
    let bytes = input.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            _ if c.is_whitespace() => i += 1,
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            '.' => {
                tokens.push(Token::Dot);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '?' => {
                tokens.push(Token::Placeholder);
                i += 1;
            }
            '=' => {
                tokens.push(Token::Comparator(Comparator::Eq));
                i += 1;
            }
            '<' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    tokens.push(Token::Comparator(Comparator::Le));
                    i += 2;
                } else if bytes.get(i + 1) == Some(&b'>') {
                    tokens.push(Token::Comparator(Comparator::Ne));
                    i += 2;
                } else {
                    tokens.push(Token::Comparator(Comparator::Lt));
                    i += 1;
                }
            }
            '>' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    tokens.push(Token::Comparator(Comparator::Ge));
                    i += 2;
                } else {
                    tokens.push(Token::Comparator(Comparator::Gt));
                    i += 1;
                }
            }
            '!' => {
                if bytes.get(i + 1) == Some(&b'=') {
                    tokens.push(Token::Comparator(Comparator::Ne));
                    i += 2;
                } else {
                    return Err(PartiqlError::new(
                        "unexpected character `!` (did you mean `!=`?)",
                    ));
                }
            }
            '"' => {
                let (text, next) = read_quoted(input, i + 1, '"')?;
                if text.is_empty() {
                    return Err(PartiqlError::new("empty quoted identifier `\"\"`"));
                }
                tokens.push(Token::Ident(text));
                i = next;
            }
            '\'' => {
                let (text, next) = read_quoted(input, i + 1, '\'')?;
                tokens.push(Token::StringLiteral(text));
                i = next;
            }
            _ if c == '_' || c.is_ascii_alphabetic() => {
                let start = i;
                while i < bytes.len() {
                    let ch = bytes[i] as char;
                    if ch == '_' || ch.is_ascii_alphanumeric() {
                        i += 1;
                    } else {
                        break;
                    }
                }
                let text = &input[start..i];
                tokens.push(match Keyword::from_ident(text) {
                    Some(kw) => Token::Keyword(kw),
                    None => Token::Ident(text.to_owned()),
                });
            }
            _ if c.is_ascii_digit() => {
                let start = i;
                while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                    i += 1;
                }
                if bytes.get(i) == Some(&b'.') && bytes.get(i + 1).is_some_and(u8::is_ascii_digit) {
                    i += 1;
                    while i < bytes.len() && (bytes[i] as char).is_ascii_digit() {
                        i += 1;
                    }
                }
                tokens.push(Token::NumberLiteral(input[start..i].to_owned()));
            }
            other => {
                return Err(PartiqlError::new(format!(
                    "unexpected character `{other}` in statement"
                )));
            }
        }
    }
    Ok(tokens)
}

/// Read a quoted span starting just past the opening `quote`, up to the next
/// unescaped occurrence of `quote`. No escape sequences are supported (a
/// doubled quote, backslash-escapes, etc.) — an unterminated quote is an
/// error naming the missing closer. Returns the unquoted text and the byte
/// index just past the closing quote.
fn read_quoted(input: &str, from: usize, quote: char) -> Result<(String, usize), PartiqlError> {
    match input[from..].find(quote) {
        Some(rel) => Ok((input[from..from + rel].to_owned(), from + rel + 1)),
        None => Err(PartiqlError::new(format!(
            "unterminated quoted string/identifier (missing closing `{quote}`)"
        ))),
    }
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

/// A parsed `SELECT` statement (ADR 0071 §1's `select_stmt`).
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub table: String,
    pub index: Option<String>,
    pub projection: ProjectionSpec,
    pub where_terms: Vec<WhereTerm>,
    pub order_by: Option<OrderBy>,
    /// Total distinct `?` placeholders the statement uses, in encounter
    /// order — [`lower_select`] requires this to equal `parameters.len()`.
    placeholder_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionSpec {
    All,
    Attrs(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub attr: String,
    pub descending: bool,
}

/// One top-level `WHERE` predicate term (ADR 0071 §1's `predicate`). The
/// `usize`(s) are positional indices into the request's `Parameters` array,
/// assigned left to right as `?`s are encountered while parsing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WhereTerm {
    Compare(String, Comparator, usize),
    Between(String, usize, usize),
    BeginsWith(String, usize),
    Contains(String, usize),
    AttributeExists(String),
    AttributeNotExists(String),
}

impl WhereTerm {
    fn attr(&self) -> &str {
        match self {
            WhereTerm::Compare(a, ..)
            | WhereTerm::Between(a, ..)
            | WhereTerm::BeginsWith(a, ..)
            | WhereTerm::Contains(a, ..)
            | WhereTerm::AttributeExists(a)
            | WhereTerm::AttributeNotExists(a) => a,
        }
    }

    /// Whether this term's shape is one DynamoDB's own `KeyConditionExpression`
    /// grammar allows as a *sort-key* condition (ADR 0071 §4.2): `=`/`<`/
    /// `<=`/`>`/`>=`, `BETWEEN`, `begins_with` — never `<>`, `contains`,
    /// `attribute_exists`/`attribute_not_exists`.
    fn is_sort_key_legal(&self) -> bool {
        match self {
            WhereTerm::Compare(_, cmp, _) => *cmp != Comparator::Ne,
            WhereTerm::Between(..) | WhereTerm::BeginsWith(..) => true,
            WhereTerm::Contains(..)
            | WhereTerm::AttributeExists(..)
            | WhereTerm::AttributeNotExists(..) => false,
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
    placeholder_count: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn eat_keyword(&mut self, kw: Keyword) -> Result<(), PartiqlError> {
        match self.advance() {
            Some(Token::Keyword(k)) if k == kw => Ok(()),
            other => Err(PartiqlError::new(format!(
                "expected `{}`, found {}",
                kw.text(),
                describe(other.as_ref())
            ))),
        }
    }

    fn peek_keyword(&self, kw: Keyword) -> bool {
        matches!(self.peek(), Some(Token::Keyword(k)) if *k == kw)
    }

    fn eat_placeholder(&mut self) -> Result<usize, PartiqlError> {
        match self.advance() {
            Some(Token::Placeholder) => {
                let idx = self.placeholder_count;
                self.placeholder_count += 1;
                Ok(idx)
            }
            Some(Token::StringLiteral(_)) | Some(Token::NumberLiteral(_)) => {
                Err(PartiqlError::new(
                    "literal values are not supported in this statement; \
                     use ? placeholders bound via Parameters",
                ))
            }
            other => Err(PartiqlError::new(format!(
                "expected a `?` placeholder, found {}",
                describe(other.as_ref())
            ))),
        }
    }

    fn eat_ident(&mut self) -> Result<String, PartiqlError> {
        match self.advance() {
            Some(Token::Ident(s)) => Ok(s),
            other => Err(PartiqlError::new(format!(
                "expected an identifier, found {}",
                describe(other.as_ref())
            ))),
        }
    }

    fn parse_select(&mut self) -> Result<SelectStatement, PartiqlError> {
        self.eat_keyword(Keyword::Select)?;
        let projection = self.parse_projection()?;
        self.eat_keyword(Keyword::From)?;
        let table = self.eat_ident()?;
        let index = if matches!(self.peek(), Some(Token::Dot)) {
            self.advance();
            Some(self.eat_ident()?)
        } else {
            None
        };
        let where_terms = if self.peek_keyword(Keyword::Where) {
            self.advance();
            self.parse_where()?
        } else {
            Vec::new()
        };
        let order_by = if self.peek_keyword(Keyword::Order) {
            self.advance();
            self.eat_keyword(Keyword::By)?;
            let attr = self.eat_ident()?;
            let descending = if self.peek_keyword(Keyword::Desc) {
                self.advance();
                true
            } else if self.peek_keyword(Keyword::Asc) {
                self.advance();
                false
            } else {
                false
            };
            Some(OrderBy { attr, descending })
        } else {
            None
        };
        if let Some(tok) = self.peek() {
            return Err(PartiqlError::new(format!(
                "unexpected trailing input after statement, starting at {}",
                describe(Some(tok))
            )));
        }
        Ok(SelectStatement {
            table,
            index,
            projection,
            where_terms,
            order_by,
            placeholder_count: self.placeholder_count,
        })
    }

    fn parse_projection(&mut self) -> Result<ProjectionSpec, PartiqlError> {
        if matches!(self.peek(), Some(Token::Star)) {
            self.advance();
            return Ok(ProjectionSpec::All);
        }
        let mut attrs = vec![self.eat_ident()?];
        while matches!(self.peek(), Some(Token::Comma)) {
            self.advance();
            attrs.push(self.eat_ident()?);
        }
        Ok(ProjectionSpec::Attrs(attrs))
    }

    fn parse_where(&mut self) -> Result<Vec<WhereTerm>, PartiqlError> {
        let mut terms = vec![self.parse_predicate()?];
        loop {
            if self.peek_keyword(Keyword::And) {
                self.advance();
                terms.push(self.parse_predicate()?);
            } else if self.peek_keyword(Keyword::Or) || self.peek_keyword(Keyword::Not) {
                return Err(PartiqlError::new(
                    "OR/NOT and parenthesised grouping are not supported in WHERE \
                     (only a top-level AND conjunction of terms)",
                ));
            } else {
                break;
            }
        }
        Ok(terms)
    }

    fn parse_predicate(&mut self) -> Result<WhereTerm, PartiqlError> {
        // Function forms: `name(args)`. Only recognized when the identifier
        // is immediately followed by `(` — otherwise it's a plain attribute
        // name that happens to share text with a function.
        if let Some(Token::Ident(name)) = self.peek().cloned()
            && matches!(self.tokens.get(self.pos + 1), Some(Token::LParen))
        {
            let lname = name.to_ascii_lowercase();
            if matches!(
                lname.as_str(),
                "begins_with" | "contains" | "attribute_exists" | "attribute_not_exists"
            ) {
                self.advance(); // ident
                self.advance(); // (
                let attr = self.eat_ident()?;
                let term = match lname.as_str() {
                    "attribute_exists" => {
                        self.expect(Token::RParen)?;
                        return Ok(WhereTerm::AttributeExists(attr));
                    }
                    "attribute_not_exists" => {
                        self.expect(Token::RParen)?;
                        return Ok(WhereTerm::AttributeNotExists(attr));
                    }
                    "begins_with" => {
                        self.expect(Token::Comma)?;
                        let idx = self.eat_placeholder()?;
                        WhereTerm::BeginsWith(attr, idx)
                    }
                    "contains" => {
                        self.expect(Token::Comma)?;
                        let idx = self.eat_placeholder()?;
                        WhereTerm::Contains(attr, idx)
                    }
                    _ => unreachable!("filtered by the outer match above"),
                };
                self.expect(Token::RParen)?;
                return Ok(term);
            }
        }

        let attr = self.eat_ident()?;
        if self.peek_keyword(Keyword::Between) {
            self.advance();
            let lo = self.eat_placeholder()?;
            self.eat_keyword(Keyword::And)?;
            let hi = self.eat_placeholder()?;
            return Ok(WhereTerm::Between(attr, lo, hi));
        }
        let cmp = match self.advance() {
            Some(Token::Comparator(c)) => c,
            other => {
                return Err(PartiqlError::new(format!(
                    "expected a comparator or BETWEEN after `{attr}`, found {}",
                    describe(other.as_ref())
                )));
            }
        };
        let idx = self.eat_placeholder()?;
        Ok(WhereTerm::Compare(attr, cmp, idx))
    }

    fn expect(&mut self, want: Token) -> Result<(), PartiqlError> {
        match self.advance() {
            Some(t) if t == want => Ok(()),
            other => Err(PartiqlError::new(format!(
                "expected `{want:?}`, found {}",
                describe(other.as_ref())
            ))),
        }
    }
}

fn describe(tok: Option<&Token>) -> String {
    match tok {
        None => "end of statement".to_owned(),
        Some(Token::Keyword(k)) => format!("`{}`", k.text()),
        Some(Token::Ident(s)) => format!("identifier `{s}`"),
        Some(Token::Placeholder) => "`?`".to_owned(),
        Some(Token::Comparator(_)) => "a comparator".to_owned(),
        Some(Token::Comma) => "`,`".to_owned(),
        Some(Token::Dot) => "`.`".to_owned(),
        Some(Token::Star) => "`*`".to_owned(),
        Some(Token::LParen) => "`(`".to_owned(),
        Some(Token::RParen) => "`)`".to_owned(),
        Some(Token::StringLiteral(s)) => format!("string literal `'{s}'`"),
        Some(Token::NumberLiteral(s)) => format!("number literal `{s}`"),
    }
}

/// The statement's leading keyword, read directly off the trimmed input
/// without a full lex (see [`parse_select_statement`]'s own doc for why).
/// `None` when the input doesn't start with a bare-word keyword at all
/// (whitespace-only, or starting with punctuation/a quote).
fn leading_keyword(input: &str) -> Option<Keyword> {
    let trimmed = input.trim_start();
    let end = trimmed
        .find(|c: char| !(c == '_' || c.is_ascii_alphanumeric()))
        .unwrap_or(trimmed.len());
    Keyword::from_ident(&trimmed[..end])
}

/// Parse a PartiQL statement, requiring it to be a `SELECT` (W-07 PR 2's
/// only supported statement shape — `INSERT`/`UPDATE`/`DELETE` land in PR 3
/// and are rejected here by name rather than falling through to a generic
/// parse error).
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem — malformed syntax, an
/// unsupported statement kind, or a construct ADR 0071 §7 puts permanently
/// out of scope.
pub fn parse_select_statement(input: &str) -> Result<SelectStatement, PartiqlError> {
    // Checked against the raw text, ahead of the full lex: PR 3's `INSERT`
    // statement grammar contains bytes (`{`, `}`) this PR's lexer doesn't
    // recognize at all, so a non-`SELECT` statement must be named *before*
    // lexing the whole input, not after.
    match leading_keyword(input) {
        Some(Keyword::Select) => {}
        Some(kw @ (Keyword::Insert | Keyword::Update | Keyword::Delete)) => {
            return Err(PartiqlError::new(format!(
                "`{}` statements are not supported yet (PartiQL W-07 PR 3)",
                kw.text()
            )));
        }
        _ => {
            return Err(PartiqlError::new(
                "expected a statement to begin with SELECT",
            ));
        }
    }
    let tokens = lex(input)?;
    let mut parser = Parser {
        tokens,
        pos: 0,
        placeholder_count: 0,
    };
    parser.parse_select()
}

// ---------------------------------------------------------------------------
// Lowering (ADR 0071 §4, §5, §6, §8)
// ---------------------------------------------------------------------------

/// Lower a parsed `SELECT` onto [`Operation::Query`] or [`Operation::Scan`]
/// (ADR 0071 §4's key-versus-filter rule).
///
/// `partition_key`/`sort_key` are the target's (base table's, or the named
/// index's) own declared key attribute names — resolved by the caller
/// (`animusd`, which holds the replicated catalog); this function never
/// reads a schema itself.
///
/// # Errors
/// A [`PartiqlError`] when: the placeholder count doesn't match
/// `parameters.len()`; `WHERE` names the partition key with `=` more than
/// once; `ORDER BY` is present but the statement doesn't lower to `Query`,
/// or names something other than the target's own sort key.
#[allow(clippy::too_many_arguments)] // one SELECT's full lowering shape (mirrors animusd::dynamo::run_query's own arity)
pub fn lower_select(
    stmt: &SelectStatement,
    parameters: &[AttributeValue],
    partition_key: &str,
    sort_key: Option<&str>,
    exclusive_start_key: Option<Item>,
    limit: Option<usize>,
    consistent_read: bool,
) -> Result<Operation, PartiqlError> {
    if stmt.placeholder_count != parameters.len() {
        return Err(PartiqlError::new(format!(
            "statement has {} placeholder(s) but Parameters supplied {}",
            stmt.placeholder_count,
            parameters.len()
        )));
    }
    let value_of = |idx: usize| parameters[idx].clone();

    let pk_positions: Vec<usize> = stmt
        .where_terms
        .iter()
        .enumerate()
        .filter(|(_, t)| matches!(t, WhereTerm::Compare(attr, Comparator::Eq, _) if attr == partition_key))
        .map(|(i, _)| i)
        .collect();
    if pk_positions.len() > 1 {
        return Err(PartiqlError::new(format!(
            "WHERE names the partition key `{partition_key}` more than once"
        )));
    }

    let projection = build_projection(&stmt.projection);
    let select = match &stmt.projection {
        ProjectionSpec::Attrs(_) => Select::SpecificAttributes,
        ProjectionSpec::All if stmt.index.is_some() => Select::AllProjectedAttributes,
        ProjectionSpec::All => Select::AllAttributes,
    };

    let mut remaining: Vec<&WhereTerm> = stmt.where_terms.iter().collect();

    match pk_positions.first().copied() {
        Some(pk_pos) => {
            let pk_term = remaining.remove(pk_pos);
            let WhereTerm::Compare(pk_attr, _, pk_idx) = pk_term else {
                unreachable!("pk_positions only selects Compare(_, Eq, _) terms")
            };
            let partition_value = value_of(*pk_idx);

            let sort_pos = sort_key.and_then(|sk| {
                remaining
                    .iter()
                    .position(|t| t.attr() == sk && t.is_sort_key_legal())
            });
            let (sort_attr, sort_condition) = match sort_pos {
                Some(sp) => {
                    let term = remaining.remove(sp);
                    (
                        Some(term.attr().to_owned()),
                        Some(build_sort_condition(term, &value_of)),
                    )
                }
                None => (None, None),
            };

            if let Some(ob) = &stmt.order_by
                && sort_key != Some(ob.attr.as_str())
            {
                return Err(PartiqlError::new(
                    "ORDER BY is only supported on the sort key of a Query-shaped SELECT",
                ));
            }
            let scan_index_forward = !stmt.order_by.as_ref().is_some_and(|ob| ob.descending);

            let filter = build_filter(&remaining, &value_of);

            Ok(Operation::Query {
                table: stmt.table.clone(),
                index: stmt.index.clone(),
                partition_attr: pk_attr.clone(),
                partition_value,
                sort_attr,
                sort_condition,
                limit,
                exclusive_start_key,
                scan_index_forward,
                filter,
                projection,
                select,
                consistent_read,
            })
        }
        None => {
            if stmt.order_by.is_some() {
                return Err(PartiqlError::new(
                    "ORDER BY requires a Query-shaped SELECT (WHERE must pin the \
                     partition key with =)",
                ));
            }
            let filter = build_filter(&remaining, &value_of);
            Ok(Operation::Scan {
                table: stmt.table.clone(),
                index: stmt.index.clone(),
                limit,
                exclusive_start_key,
                filter,
                projection,
                select,
                segment: None,
                consistent_read,
            })
        }
    }
}

fn build_projection(spec: &ProjectionSpec) -> Option<Projection> {
    match spec {
        ProjectionSpec::All => None,
        ProjectionSpec::Attrs(attrs) => Some(Projection(
            attrs
                .iter()
                .map(|a| vec![PathSegment::Field(a.clone())])
                .collect(),
        )),
    }
}

fn build_sort_condition(
    term: &WhereTerm,
    value_of: &impl Fn(usize) -> AttributeValue,
) -> SortKeyCondition {
    match term {
        WhereTerm::Compare(_, cmp, idx) => SortKeyCondition::Compare(*cmp, value_of(*idx)),
        WhereTerm::Between(_, lo, hi) => SortKeyCondition::Between(value_of(*lo), value_of(*hi)),
        WhereTerm::BeginsWith(_, idx) => SortKeyCondition::BeginsWith(value_of(*idx)),
        WhereTerm::Contains(..)
        | WhereTerm::AttributeExists(..)
        | WhereTerm::AttributeNotExists(..) => {
            unreachable!("is_sort_key_legal excludes these shapes")
        }
    }
}

fn build_filter(
    terms: &[&WhereTerm],
    value_of: &impl Fn(usize) -> AttributeValue,
) -> Option<ConditionExpression> {
    let mut iter = terms.iter();
    let first = iter.next()?;
    let mut expr = build_condition_expr(first, value_of);
    for t in iter {
        expr =
            ConditionExpression::And(Box::new(expr), Box::new(build_condition_expr(t, value_of)));
    }
    Some(expr)
}

fn build_condition_expr(
    term: &WhereTerm,
    value_of: &impl Fn(usize) -> AttributeValue,
) -> ConditionExpression {
    match term {
        WhereTerm::Compare(attr, cmp, idx) => {
            ConditionExpression::Compare(attr.clone(), *cmp, value_of(*idx))
        }
        WhereTerm::Between(attr, lo, hi) => {
            ConditionExpression::Between(attr.clone(), value_of(*lo), value_of(*hi))
        }
        WhereTerm::BeginsWith(attr, idx) => {
            ConditionExpression::BeginsWith(attr.clone(), value_of(*idx))
        }
        WhereTerm::Contains(attr, idx) => {
            ConditionExpression::Contains(attr.clone(), value_of(*idx))
        }
        WhereTerm::AttributeExists(attr) => ConditionExpression::AttributeExists(attr.clone()),
        WhereTerm::AttributeNotExists(attr) => {
            ConditionExpression::AttributeNotExists(attr.clone())
        }
    }
}

// ---------------------------------------------------------------------------
// NextToken (ADR 0071 §9)
// ---------------------------------------------------------------------------

const NEXT_TOKEN_VERSION: u64 = 1;

fn statement_hash(statement: &str) -> String {
    crate::wire::hex_encode_lower(&Sha256::digest(statement.as_bytes()))
}

/// Mint an opaque `NextToken` wrapping `last_evaluated_key`, bound to
/// `statement` (ADR 0071 §9) so it can never be replayed against a
/// different statement.
#[must_use]
pub fn encode_next_token(statement: &str, last_evaluated_key: &Item) -> String {
    let mut obj = Map::new();
    obj.insert("v".into(), Value::from(NEXT_TOKEN_VERSION));
    obj.insert("stmt".into(), Value::String(statement_hash(statement)));
    obj.insert("lek".into(), crate::wire::encode_item(last_evaluated_key));
    let json = serde_json::to_vec(&Value::Object(obj)).expect("token envelope serializes");
    crate::wire::base64_encode(&json)
}

/// Decode and verify a `NextToken` minted by [`encode_next_token`] against
/// `statement` — a mismatch (wrong statement, wrong version, malformed
/// bytes) is a [`PartiqlError`] (ADR 0071 §9).
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem.
pub fn decode_next_token(token: &str, statement: &str) -> Result<Item, PartiqlError> {
    let bytes = crate::wire::base64_decode(token)
        .ok_or_else(|| PartiqlError::new("malformed NextToken"))?;
    let value: Value =
        serde_json::from_slice(&bytes).map_err(|_| PartiqlError::new("malformed NextToken"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| PartiqlError::new("malformed NextToken"))?;
    let version = obj
        .get("v")
        .and_then(Value::as_u64)
        .ok_or_else(|| PartiqlError::new("malformed NextToken"))?;
    if version != NEXT_TOKEN_VERSION {
        return Err(PartiqlError::new("unrecognized NextToken version"));
    }
    let stmt_hash = obj
        .get("stmt")
        .and_then(Value::as_str)
        .ok_or_else(|| PartiqlError::new("malformed NextToken"))?;
    if stmt_hash != statement_hash(statement) {
        return Err(PartiqlError::new("NextToken does not match this statement"));
    }
    let lek = obj
        .get("lek")
        .and_then(Value::as_object)
        .ok_or_else(|| PartiqlError::new("malformed NextToken"))?;
    crate::wire::decode_item(lek).map_err(|e| PartiqlError::new(e.message))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> SelectStatement {
        parse_select_statement(text).expect("parses")
    }

    #[test]
    fn lexes_every_token_class() {
        let tokens =
            lex("SELECT * FROM \"t\" WHERE a = ? AND b BETWEEN ? AND ? , . ( ) <> != <= >=")
                .expect("lexes");
        assert!(tokens.contains(&Token::Star));
        assert!(tokens.contains(&Token::Comma));
        assert!(tokens.contains(&Token::Dot));
        assert!(tokens.contains(&Token::LParen));
        assert!(tokens.contains(&Token::RParen));
        assert!(tokens.contains(&Token::Placeholder));
        assert!(tokens.contains(&Token::Comparator(Comparator::Ne)));
        assert!(tokens.contains(&Token::Comparator(Comparator::Le)));
        assert!(tokens.contains(&Token::Comparator(Comparator::Ge)));
    }

    #[test]
    fn quoted_identifier_is_never_a_keyword() {
        let stmt = s("SELECT * FROM \"from\"");
        assert_eq!(stmt.table, "from");
    }

    #[test]
    fn rejects_unterminated_quote() {
        let err = parse_select_statement("SELECT * FROM \"t").unwrap_err();
        assert!(err.message.contains("unterminated"), "{}", err.message);
    }

    #[test]
    fn parses_star_projection() {
        let stmt = s("SELECT * FROM t");
        assert_eq!(stmt.projection, ProjectionSpec::All);
        assert_eq!(stmt.table, "t");
        assert_eq!(stmt.index, None);
    }

    #[test]
    fn parses_attribute_list_projection() {
        let stmt = s("SELECT a, b, c FROM t");
        assert_eq!(
            stmt.projection,
            ProjectionSpec::Attrs(vec!["a".into(), "b".into(), "c".into()])
        );
    }

    #[test]
    fn parses_index_from_clause() {
        let stmt = s("SELECT * FROM \"t\".\"gsi1\"");
        assert_eq!(stmt.table, "t");
        assert_eq!(stmt.index, Some("gsi1".into()));
    }

    #[test]
    fn parses_where_equality_and_placeholder_indices() {
        let stmt = s("SELECT * FROM t WHERE pk = ? AND sk = ?");
        assert_eq!(
            stmt.where_terms,
            vec![
                WhereTerm::Compare("pk".into(), Comparator::Eq, 0),
                WhereTerm::Compare("sk".into(), Comparator::Eq, 1),
            ]
        );
        assert_eq!(stmt.placeholder_count, 2);
    }

    #[test]
    fn parses_between() {
        let stmt = s("SELECT * FROM t WHERE pk = ? AND sk BETWEEN ? AND ?");
        assert_eq!(stmt.where_terms[1], WhereTerm::Between("sk".into(), 1, 2));
    }

    #[test]
    fn parses_begins_with_and_contains_and_attribute_exists() {
        let stmt = s(
            "SELECT * FROM t WHERE begins_with(sk, ?) AND contains(tags, ?) \
             AND attribute_exists(a) AND attribute_not_exists(b)",
        );
        assert_eq!(stmt.where_terms[0], WhereTerm::BeginsWith("sk".into(), 0));
        assert_eq!(stmt.where_terms[1], WhereTerm::Contains("tags".into(), 1));
        assert_eq!(stmt.where_terms[2], WhereTerm::AttributeExists("a".into()));
        assert_eq!(
            stmt.where_terms[3],
            WhereTerm::AttributeNotExists("b".into())
        );
    }

    #[test]
    fn parses_order_by_default_ascending() {
        let stmt = s("SELECT * FROM t WHERE pk = ? ORDER BY sk");
        let ob = stmt.order_by.expect("order by");
        assert_eq!(ob.attr, "sk");
        assert!(!ob.descending);
    }

    #[test]
    fn parses_order_by_desc() {
        let stmt = s("SELECT * FROM t WHERE pk = ? ORDER BY sk DESC");
        assert!(stmt.order_by.unwrap().descending);
    }

    #[test]
    fn rejects_or_in_where() {
        let err = parse_select_statement("SELECT * FROM t WHERE a = ? OR b = ?").unwrap_err();
        assert!(err.message.contains("OR/NOT"), "{}", err.message);
    }

    #[test]
    fn rejects_literal_string_value() {
        let err = parse_select_statement("SELECT * FROM t WHERE pk = 'x'").unwrap_err();
        assert!(err.message.contains("literal values"), "{}", err.message);
    }

    #[test]
    fn rejects_literal_number_value() {
        let err = parse_select_statement("SELECT * FROM t WHERE n > 3").unwrap_err();
        assert!(err.message.contains("literal values"), "{}", err.message);
    }

    #[test]
    fn rejects_insert_update_delete_with_named_error() {
        for (text, name) in [
            ("INSERT INTO t VALUE {'pk':?}", "INSERT"),
            ("UPDATE t SET a = ? WHERE pk = ?", "UPDATE"),
            ("DELETE FROM t WHERE pk = ?", "DELETE"),
        ] {
            let err = parse_select_statement(text).unwrap_err();
            assert!(err.message.contains(name), "{}: {}", text, err.message);
            assert!(err.message.contains("PR 3"), "{}: {}", text, err.message);
        }
    }

    #[test]
    fn rejects_trailing_garbage() {
        let err = parse_select_statement("SELECT * FROM t EXTRA").unwrap_err();
        assert!(err.message.contains("trailing"), "{}", err.message);
    }

    #[test]
    fn rejects_non_select_garbage() {
        let err = parse_select_statement("BOGUS").unwrap_err();
        assert!(err.message.contains("SELECT"), "{}", err.message);
    }

    fn av_s(s: &str) -> AttributeValue {
        AttributeValue::S(s.to_owned())
    }

    #[test]
    fn lowers_partition_equality_to_query() {
        let stmt = s("SELECT * FROM t WHERE pk = ?");
        let params = vec![av_s("v1")];
        let op = lower_select(&stmt, &params, "pk", None, None, None, false).expect("lowers");
        match op {
            Operation::Query {
                partition_attr,
                partition_value,
                sort_attr,
                sort_condition,
                filter,
                ..
            } => {
                assert_eq!(partition_attr, "pk");
                assert_eq!(partition_value, av_s("v1"));
                assert_eq!(sort_attr, None);
                assert_eq!(sort_condition, None);
                assert_eq!(filter, None);
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn lowers_no_partition_equality_to_scan_with_filter() {
        let stmt = s("SELECT * FROM t WHERE other = ?");
        let params = vec![av_s("v1")];
        let op = lower_select(&stmt, &params, "pk", None, None, None, false).expect("lowers");
        match op {
            Operation::Scan { filter, .. } => {
                assert_eq!(
                    filter,
                    Some(ConditionExpression::Compare(
                        "other".into(),
                        Comparator::Eq,
                        av_s("v1")
                    ))
                );
            }
            other => panic!("expected Scan, got {other:?}"),
        }
    }

    #[test]
    fn lowers_sort_condition_and_leaves_extra_terms_as_filter() {
        let stmt = s("SELECT * FROM t WHERE pk = ? AND sk > ? AND extra = ?");
        let params = vec![av_s("p"), av_s("s"), av_s("e")];
        let op = lower_select(&stmt, &params, "pk", Some("sk"), None, None, false).expect("lowers");
        match op {
            Operation::Query {
                sort_attr,
                sort_condition,
                filter,
                ..
            } => {
                assert_eq!(sort_attr, Some("sk".into()));
                assert_eq!(
                    sort_condition,
                    Some(SortKeyCondition::Compare(Comparator::Gt, av_s("s")))
                );
                assert_eq!(
                    filter,
                    Some(ConditionExpression::Compare(
                        "extra".into(),
                        Comparator::Eq,
                        av_s("e")
                    ))
                );
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn sort_key_ne_never_consumed_as_key_condition() {
        // `<>` is not a legal KeyConditionExpression comparator — it must
        // fall through to the filter even though it names the sort key.
        let stmt = s("SELECT * FROM t WHERE pk = ? AND sk <> ?");
        let params = vec![av_s("p"), av_s("s")];
        let op = lower_select(&stmt, &params, "pk", Some("sk"), None, None, false).expect("lowers");
        match op {
            Operation::Query {
                sort_condition,
                filter,
                ..
            } => {
                assert_eq!(sort_condition, None);
                assert!(filter.is_some());
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn rejects_two_partition_key_equalities() {
        let stmt = s("SELECT * FROM t WHERE pk = ? AND pk = ?");
        let params = vec![av_s("a"), av_s("b")];
        let err = lower_select(&stmt, &params, "pk", None, None, None, false).unwrap_err();
        assert!(err.message.contains("more than once"), "{}", err.message);
    }

    #[test]
    fn rejects_placeholder_count_mismatch() {
        let stmt = s("SELECT * FROM t WHERE pk = ?");
        let params: Vec<AttributeValue> = vec![];
        let err = lower_select(&stmt, &params, "pk", None, None, None, false).unwrap_err();
        assert!(err.message.contains("placeholder"), "{}", err.message);
    }

    #[test]
    fn order_by_on_scan_lowering_is_rejected() {
        let stmt = s("SELECT * FROM t WHERE other = ? ORDER BY sk");
        let params = vec![av_s("v")];
        let err = lower_select(&stmt, &params, "pk", Some("sk"), None, None, false).unwrap_err();
        assert!(err.message.contains("ORDER BY"), "{}", err.message);
    }

    #[test]
    fn order_by_on_non_sort_key_is_rejected() {
        let stmt = s("SELECT * FROM t WHERE pk = ? ORDER BY nope");
        let params = vec![av_s("v")];
        let err = lower_select(&stmt, &params, "pk", Some("sk"), None, None, false).unwrap_err();
        assert!(err.message.contains("ORDER BY"), "{}", err.message);
    }

    #[test]
    fn order_by_desc_sets_scan_index_forward_false() {
        let stmt = s("SELECT * FROM t WHERE pk = ? ORDER BY sk DESC");
        let params = vec![av_s("v")];
        let op = lower_select(&stmt, &params, "pk", Some("sk"), None, None, false).expect("lowers");
        match op {
            Operation::Query {
                scan_index_forward, ..
            } => assert!(!scan_index_forward),
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn projection_list_sets_specific_attributes_select() {
        let stmt = s("SELECT a, b FROM t WHERE pk = ?");
        let params = vec![av_s("v")];
        let op = lower_select(&stmt, &params, "pk", None, None, None, false).expect("lowers");
        match op {
            Operation::Query {
                projection, select, ..
            } => {
                assert_eq!(select, Select::SpecificAttributes);
                assert_eq!(
                    projection,
                    Some(Projection(vec![
                        vec![PathSegment::Field("a".into())],
                        vec![PathSegment::Field("b".into())],
                    ]))
                );
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn star_projection_on_index_selects_all_projected_attributes() {
        let stmt = s("SELECT * FROM \"t\".\"gsi1\" WHERE pk = ?");
        let params = vec![av_s("v")];
        let op = lower_select(&stmt, &params, "pk", None, None, None, false).expect("lowers");
        match op {
            Operation::Query { select, index, .. } => {
                assert_eq!(select, Select::AllProjectedAttributes);
                assert_eq!(index, Some("gsi1".into()));
            }
            other => panic!("expected Query, got {other:?}"),
        }
    }

    #[test]
    fn next_token_round_trips() {
        let mut item = Item::new();
        item.insert("pk".into(), av_s("v1"));
        let stmt = "SELECT * FROM t WHERE pk = ?";
        let tok = encode_next_token(stmt, &item);
        let decoded = decode_next_token(&tok, stmt).expect("decodes");
        assert_eq!(decoded, item);
    }

    #[test]
    fn next_token_rejects_different_statement() {
        let mut item = Item::new();
        item.insert("pk".into(), av_s("v1"));
        let tok = encode_next_token("SELECT * FROM t WHERE pk = ?", &item);
        let err = decode_next_token(&tok, "SELECT * FROM t WHERE pk = ? AND sk = ?").unwrap_err();
        assert!(err.message.contains("does not match"), "{}", err.message);
    }

    #[test]
    fn next_token_rejects_malformed_input() {
        let err = decode_next_token("not-base64!!", "SELECT * FROM t").unwrap_err();
        assert!(err.message.contains("malformed"), "{}", err.message);
    }

    #[test]
    fn next_token_rejects_unknown_version() {
        let mut obj = Map::new();
        obj.insert("v".into(), Value::from(99u64));
        obj.insert(
            "stmt".into(),
            Value::String(statement_hash("SELECT * FROM t")),
        );
        obj.insert("lek".into(), Value::Object(Map::new()));
        let json = serde_json::to_vec(&Value::Object(obj)).unwrap();
        let tok = crate::wire::base64_encode(&json);
        let err = decode_next_token(&tok, "SELECT * FROM t").unwrap_err();
        assert!(err.message.contains("version"), "{}", err.message);
    }
}
