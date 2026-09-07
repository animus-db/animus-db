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
//! `INSERT`/`UPDATE`/`DELETE` (W-07 PR 3) share the same lexer and the same
//! "types, not string decoders" discipline: [`lower_insert`] builds an
//! [`Item`] directly from the parsed document (placeholder leaves only, ADR
//! 0071 §2 — a nested `{..}`/`[..]` literal *structure* is legal, but every
//! leaf value must still be a `?`); [`lower_update`] builds
//! [`crate::wire::UpdateAction`] `SET` actions directly from the parsed
//! assignment list; both, plus [`lower_delete`], resolve `UPDATE`/`DELETE`'s
//! required `WHERE` clause through the identical exact-match key-vs-filter
//! logic (ADR 0071 §5) — the full primary key as `=` terms, everything else
//! folded into a `ConditionExpression` filter. [`parse_statement`] is the
//! entry point that dispatches on the statement's own leading keyword to one
//! of the four parsers, returning a [`Statement`].
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

use crate::capacity::{ReturnConsumedCapacity, ReturnItemCollectionMetrics};
use crate::condition::{Comparator, ConditionExpression, SortKeyCondition};
use crate::wire::{
    Operation, PathSegment, Projection, ReturnValues, Select, UpdateAction, UpdateExpr,
    UpdateReturnValues, WireError,
};
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
    Colon,
    Star,
    LParen,
    RParen,
    /// `{` — opens an `INSERT` document, or a nested document value.
    LBrace,
    /// `}` — closes one.
    RBrace,
    /// `[` — opens a list value.
    LBracket,
    /// `]` — closes one.
    RBracket,
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
    Into,
    Value,
    Update,
    Set,
    Delete,
    Returning,
    All,
    Old,
    New,
    On,
    Conflict,
    Do,
    Nothing,
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
            "INTO" => Keyword::Into,
            "VALUE" => Keyword::Value,
            "UPDATE" => Keyword::Update,
            "SET" => Keyword::Set,
            "DELETE" => Keyword::Delete,
            "RETURNING" => Keyword::Returning,
            "ALL" => Keyword::All,
            "OLD" => Keyword::Old,
            "NEW" => Keyword::New,
            "ON" => Keyword::On,
            "CONFLICT" => Keyword::Conflict,
            "DO" => Keyword::Do,
            "NOTHING" => Keyword::Nothing,
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
            Keyword::Into => "INTO",
            Keyword::Value => "VALUE",
            Keyword::Update => "UPDATE",
            Keyword::Set => "SET",
            Keyword::Delete => "DELETE",
            Keyword::Returning => "RETURNING",
            Keyword::All => "ALL",
            Keyword::Old => "OLD",
            Keyword::New => "NEW",
            Keyword::On => "ON",
            Keyword::Conflict => "CONFLICT",
            Keyword::Do => "DO",
            Keyword::Nothing => "NOTHING",
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
            ':' => {
                tokens.push(Token::Colon);
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
            '{' => {
                tokens.push(Token::LBrace);
                i += 1;
            }
            '}' => {
                tokens.push(Token::RBrace);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
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

/// A parsed statement — one of the four shapes ADR 0071 §1 pins.
/// [`parse_statement`] dispatches to the right variant by leading keyword.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
}

impl Statement {
    /// The statement's target table name — every one of the four shapes
    /// names exactly one.
    #[must_use]
    pub fn table(&self) -> &str {
        match self {
            Statement::Select(s) => &s.table,
            Statement::Insert(s) => &s.table,
            Statement::Update(s) => &s.table,
            Statement::Delete(s) => &s.table,
        }
    }
}

/// A parsed `INSERT` statement (ADR 0071 §1's `insert_stmt`, W-07 PR 3).
#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    /// The `VALUE` document's top-level pairs, in statement order.
    pub document: Vec<(String, ValueAst)>,
    /// `ON CONFLICT DO NOTHING` — swallow a duplicate-key failure as a
    /// silent no-op instead of raising `DuplicateItemException` (ADR 0071's
    /// lowering table).
    pub on_conflict_do_nothing: bool,
    placeholder_count: usize,
}

/// A parsed `UPDATE` statement (ADR 0071 §1's `update_stmt`, W-07 PR 3).
#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    /// `SET` assignments, in statement order — each lowers to one `SET`
    /// [`UpdateAction`].
    pub assignments: Vec<(String, ValueAst)>,
    pub where_terms: Vec<WhereTerm>,
    /// `RETURNING ALL OLD *` / `RETURNING ALL NEW *`, if given.
    pub returning: Option<ReturningMode>,
    placeholder_count: usize,
}

/// A parsed `DELETE` statement (ADR 0071 §1's `delete_stmt`, W-07 PR 3).
#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_terms: Vec<WhereTerm>,
    /// `RETURNING ALL OLD *` only — `ALL NEW *` is rejected at parse time
    /// (a `DELETE` has no new image).
    pub returning: Option<ReturningMode>,
    placeholder_count: usize,
}

/// `RETURNING ALL OLD *` / `RETURNING ALL NEW *` (ADR 0071's PR 3 lowering
/// table — a conservative extension past ADR 0071 §7's original "deferred
/// past PR 3" note; see this PR's own ADR amendment). Every DynamoDB
/// PartiQL `RETURNING` clause names `ALL` explicitly — there is no `MODIFIED
/// OLD`/`MODIFIED NEW` subset in this adapter's grammar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturningMode {
    AllOld,
    AllNew,
}

/// A value in `INSERT`'s document, or an `UPDATE` assignment's
/// right-hand side (ADR 0071 §2's placeholder-only discipline, widened to a
/// recursive document/list *structure*): every leaf is a `?`, but the
/// structure around it — nested `{..}` documents and `[..]` lists — may be
/// written directly in the statement text. A literal scalar (a quoted
/// string, a bare number, `true`/`false`/`NULL`) in any leaf position is
/// rejected exactly like a `WHERE` term's literal (ADR 0071 §2).
#[derive(Debug, Clone, PartialEq)]
pub enum ValueAst {
    /// A `?` placeholder — the positional index into `Parameters`.
    Placeholder(usize),
    /// A nested `{ 'key': value, .. }` document.
    Document(Vec<(String, ValueAst)>),
    /// A `[ value, .. ]` list.
    List(Vec<ValueAst>),
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

    /// A quoted document key (ADR 0071 §1's `pair := string ":" value`) — a
    /// single-quoted string literal, never a bare/double-quoted `ident`.
    fn eat_string_key(&mut self) -> Result<String, PartiqlError> {
        match self.advance() {
            Some(Token::StringLiteral(s)) => Ok(s),
            other => Err(PartiqlError::new(format!(
                "expected a quoted document key (e.g. 'pk'), found {}",
                describe(other.as_ref())
            ))),
        }
    }

    /// `{ [ 'key': value ("," 'key': value)* ] }` — an `INSERT` document, or
    /// a nested document value.
    fn parse_document(&mut self) -> Result<Vec<(String, ValueAst)>, PartiqlError> {
        self.expect(Token::LBrace)?;
        let mut pairs = Vec::new();
        if !matches!(self.peek(), Some(Token::RBrace)) {
            loop {
                let key = self.eat_string_key()?;
                self.expect(Token::Colon)?;
                let value = self.parse_value()?;
                pairs.push((key, value));
                if matches!(self.peek(), Some(Token::Comma)) {
                    self.advance();
                    continue;
                }
                break;
            }
        }
        self.expect(Token::RBrace)?;
        Ok(pairs)
    }

    /// `value := "?" | document | "[" [ value ("," value)* ] "]"` (ADR 0071
    /// §2, widened per this PR's own amendment — see [`ValueAst`]'s doc). A
    /// literal scalar token in a value position is named explicitly, same
    /// discipline as [`Self::eat_placeholder`].
    fn parse_value(&mut self) -> Result<ValueAst, PartiqlError> {
        match self.peek().cloned() {
            Some(Token::Placeholder) => {
                self.advance();
                let idx = self.placeholder_count;
                self.placeholder_count += 1;
                Ok(ValueAst::Placeholder(idx))
            }
            Some(Token::LBrace) => Ok(ValueAst::Document(self.parse_document()?)),
            Some(Token::LBracket) => {
                self.advance();
                let mut items = Vec::new();
                if !matches!(self.peek(), Some(Token::RBracket)) {
                    loop {
                        items.push(self.parse_value()?);
                        if matches!(self.peek(), Some(Token::Comma)) {
                            self.advance();
                            continue;
                        }
                        break;
                    }
                }
                self.expect(Token::RBracket)?;
                Ok(ValueAst::List(items))
            }
            Some(Token::StringLiteral(_))
            | Some(Token::NumberLiteral(_))
            | Some(Token::Keyword(Keyword::True | Keyword::False | Keyword::Null)) => {
                Err(PartiqlError::new(
                    "literal values are not supported in this statement; \
                     use ? placeholders bound via Parameters",
                ))
            }
            other => Err(PartiqlError::new(format!(
                "expected a value (`?`, a nested document, or a list), found {}",
                describe(other.as_ref())
            ))),
        }
    }

    /// `"INSERT" "INTO" ident "VALUE" document [ "ON" "CONFLICT" "DO" "NOTHING" ]`.
    fn parse_insert(&mut self) -> Result<InsertStatement, PartiqlError> {
        self.eat_keyword(Keyword::Insert)?;
        self.eat_keyword(Keyword::Into)?;
        let table = self.eat_ident()?;
        self.eat_keyword(Keyword::Value)?;
        let document = self.parse_document()?;
        let on_conflict_do_nothing = if self.peek_keyword(Keyword::On) {
            self.advance();
            self.eat_keyword(Keyword::Conflict)?;
            self.eat_keyword(Keyword::Do)?;
            self.eat_keyword(Keyword::Nothing)?;
            true
        } else {
            false
        };
        if let Some(tok) = self.peek() {
            return Err(PartiqlError::new(format!(
                "unexpected trailing input after statement, starting at {}",
                describe(Some(tok))
            )));
        }
        Ok(InsertStatement {
            table,
            document,
            on_conflict_do_nothing,
            placeholder_count: self.placeholder_count,
        })
    }

    /// `ident "=" value`.
    fn parse_assignment(&mut self) -> Result<(String, ValueAst), PartiqlError> {
        let attr = self.eat_ident()?;
        self.expect(Token::Comparator(Comparator::Eq))?;
        let value = self.parse_value()?;
        Ok((attr, value))
    }

    /// `[ "RETURNING" "ALL" ( "OLD" | "NEW" ) "*" ]` (this PR's own amendment
    /// to ADR 0071 §7's original "deferred past PR 3" note).
    fn parse_returning_clause(&mut self) -> Result<Option<ReturningMode>, PartiqlError> {
        if !self.peek_keyword(Keyword::Returning) {
            return Ok(None);
        }
        self.advance();
        self.eat_keyword(Keyword::All)?;
        let mode = match self.advance() {
            Some(Token::Keyword(Keyword::Old)) => ReturningMode::AllOld,
            Some(Token::Keyword(Keyword::New)) => ReturningMode::AllNew,
            other => {
                return Err(PartiqlError::new(format!(
                    "expected OLD or NEW after RETURNING ALL, found {}",
                    describe(other.as_ref())
                )));
            }
        };
        self.expect(Token::Star)?;
        Ok(Some(mode))
    }

    /// `"UPDATE" ident "SET" assignment ("," assignment)* where_clause
    /// [ returning_clause ]` — `WHERE` required (ADR 0071 §1).
    fn parse_update(&mut self) -> Result<UpdateStatement, PartiqlError> {
        self.eat_keyword(Keyword::Update)?;
        let table = self.eat_ident()?;
        self.eat_keyword(Keyword::Set)?;
        let mut assignments = vec![self.parse_assignment()?];
        while matches!(self.peek(), Some(Token::Comma)) {
            self.advance();
            assignments.push(self.parse_assignment()?);
        }
        self.eat_keyword(Keyword::Where)?;
        let where_terms = self.parse_where()?;
        let returning = self.parse_returning_clause()?;
        if let Some(tok) = self.peek() {
            return Err(PartiqlError::new(format!(
                "unexpected trailing input after statement, starting at {}",
                describe(Some(tok))
            )));
        }
        Ok(UpdateStatement {
            table,
            assignments,
            where_terms,
            returning,
            placeholder_count: self.placeholder_count,
        })
    }

    /// `"DELETE" "FROM" ident where_clause [ returning_clause ]` — `WHERE`
    /// required (ADR 0071 §1). `RETURNING ALL NEW *` is rejected here — a
    /// `DELETE` has no new image.
    fn parse_delete(&mut self) -> Result<DeleteStatement, PartiqlError> {
        self.eat_keyword(Keyword::Delete)?;
        self.eat_keyword(Keyword::From)?;
        let table = self.eat_ident()?;
        self.eat_keyword(Keyword::Where)?;
        let where_terms = self.parse_where()?;
        let returning = self.parse_returning_clause()?;
        if returning == Some(ReturningMode::AllNew) {
            return Err(PartiqlError::new(
                "RETURNING ALL NEW * is not supported on DELETE (there is no new image; \
                 use RETURNING ALL OLD *)",
            ));
        }
        if let Some(tok) = self.peek() {
            return Err(PartiqlError::new(format!(
                "unexpected trailing input after statement, starting at {}",
                describe(Some(tok))
            )));
        }
        Ok(DeleteStatement {
            table,
            where_terms,
            returning,
            placeholder_count: self.placeholder_count,
        })
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
        Some(Token::Colon) => "`:`".to_owned(),
        Some(Token::Star) => "`*`".to_owned(),
        Some(Token::LParen) => "`(`".to_owned(),
        Some(Token::RParen) => "`)`".to_owned(),
        Some(Token::LBrace) => "`{`".to_owned(),
        Some(Token::RBrace) => "`}`".to_owned(),
        Some(Token::LBracket) => "`[`".to_owned(),
        Some(Token::RBracket) => "`]`".to_owned(),
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

/// Build a fresh [`Parser`] over `input`, first checking (against the raw
/// text, ahead of the full lex — some statement grammars use bytes, e.g.
/// `{`/`}`, that only make sense once the leading keyword is known) that
/// `input` begins with `want`.
fn parser_for(input: &str, want: Keyword) -> Result<Parser, PartiqlError> {
    match leading_keyword(input) {
        Some(k) if k == want => {}
        _ => {
            return Err(PartiqlError::new(format!(
                "expected a statement to begin with {}",
                want.text()
            )));
        }
    }
    let tokens = lex(input)?;
    Ok(Parser {
        tokens,
        pos: 0,
        placeholder_count: 0,
    })
}

/// Parse a PartiQL statement, requiring it to be a `SELECT`.
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem — malformed syntax, an
/// unsupported statement kind, or a construct ADR 0071 §7 puts permanently
/// out of scope.
pub fn parse_select_statement(input: &str) -> Result<SelectStatement, PartiqlError> {
    parser_for(input, Keyword::Select)?.parse_select()
}

/// Parse a PartiQL statement, requiring it to be an `INSERT` (ADR 0071 §1's
/// `insert_stmt`, W-07 PR 3).
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem.
pub fn parse_insert_statement(input: &str) -> Result<InsertStatement, PartiqlError> {
    parser_for(input, Keyword::Insert)?.parse_insert()
}

/// Parse a PartiQL statement, requiring it to be an `UPDATE` (ADR 0071 §1's
/// `update_stmt`, W-07 PR 3).
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem.
pub fn parse_update_statement(input: &str) -> Result<UpdateStatement, PartiqlError> {
    parser_for(input, Keyword::Update)?.parse_update()
}

/// Parse a PartiQL statement, requiring it to be a `DELETE` (ADR 0071 §1's
/// `delete_stmt`, W-07 PR 3).
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem.
pub fn parse_delete_statement(input: &str) -> Result<DeleteStatement, PartiqlError> {
    parser_for(input, Keyword::Delete)?.parse_delete()
}

/// Parse a PartiQL statement of any of the four supported shapes (ADR 0071
/// §1), dispatching on the statement's own leading keyword — the entry
/// point `animusd::dynamo::execute_statement` uses.
///
/// # Errors
/// A [`PartiqlError`] naming the specific problem — malformed syntax, an
/// unrecognized leading keyword, or a construct ADR 0071 §7 puts
/// permanently out of scope.
pub fn parse_statement(input: &str) -> Result<Statement, PartiqlError> {
    match leading_keyword(input) {
        Some(Keyword::Select) => parse_select_statement(input).map(Statement::Select),
        Some(Keyword::Insert) => parse_insert_statement(input).map(Statement::Insert),
        Some(Keyword::Update) => parse_update_statement(input).map(Statement::Update),
        Some(Keyword::Delete) => parse_delete_statement(input).map(Statement::Delete),
        _ => Err(PartiqlError::new(
            "expected a statement to begin with SELECT, INSERT, UPDATE, or DELETE",
        )),
    }
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
// Lowering: INSERT/UPDATE/DELETE (ADR 0071 §5, W-07 PR 3)
// ---------------------------------------------------------------------------

/// Resolve a [`ValueAst`] into a real [`AttributeValue`], recursively —
/// every `Placeholder` leaf resolves against `parameters` (the same
/// positional binding [`lower_select`]'s WHERE terms use); `Document`/`List`
/// rebuild the equivalent `M`/`L` structure.
fn lower_value_ast(value: &ValueAst, parameters: &[AttributeValue]) -> AttributeValue {
    match value {
        ValueAst::Placeholder(idx) => parameters[*idx].clone(),
        ValueAst::Document(pairs) => AttributeValue::M(
            pairs
                .iter()
                .map(|(k, v)| (k.clone(), lower_value_ast(v, parameters)))
                .collect(),
        ),
        ValueAst::List(items) => AttributeValue::L(
            items
                .iter()
                .map(|v| lower_value_ast(v, parameters))
                .collect(),
        ),
    }
}

/// The shared `UPDATE`/`DELETE` `WHERE`-to-key lowering (ADR 0071 §5): the
/// full primary key, both attrs present as `=` terms — exactly one term
/// naming `partition_key` with `=`, and (if `sort_key` is `Some`) exactly
/// one more naming it with `=` too. Every other term is AND-folded into a
/// `ConditionExpression` filter (`None` when nothing remains), applied as
/// the underlying `UpdateItem`/`DeleteItem`'s own `condition` — unlike
/// [`lower_select`]'s key-vs-filter rule (ADR 0071 §4), a mutation target is
/// always an *exact* key, never a partial one: a `WHERE` that only narrows
/// (missing the sort key, or a comparator other than `=`) is a
/// [`PartiqlError`], never silently widened into a scan-and-mutate.
fn lower_exact_key_where(
    where_terms: &[WhereTerm],
    parameters: &[AttributeValue],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<(Item, Option<ConditionExpression>), PartiqlError> {
    let value_of = |idx: usize| parameters[idx].clone();

    let mut remaining: Vec<&WhereTerm> = where_terms.iter().collect();
    let mut key = Item::new();

    let pk_positions: Vec<usize> = remaining
        .iter()
        .enumerate()
        .filter(|(_, t)| {
            matches!(t, WhereTerm::Compare(attr, Comparator::Eq, _) if attr == partition_key)
        })
        .map(|(i, _)| i)
        .collect();
    match pk_positions.len() {
        0 => {
            return Err(PartiqlError::new(format!(
                "WHERE must pin the partition key `{partition_key}` with = \
                 (a mutation targets exactly one item, never a range)"
            )));
        }
        1 => {}
        _ => {
            return Err(PartiqlError::new(format!(
                "WHERE names the partition key `{partition_key}` more than once"
            )));
        }
    }
    let pk_term = remaining.remove(pk_positions[0]);
    let WhereTerm::Compare(_, _, pk_idx) = pk_term else {
        unreachable!("pk_positions only selects Compare(_, Eq, _) terms")
    };
    key.insert(partition_key.to_string(), value_of(*pk_idx));

    if let Some(sk) = sort_key {
        let sk_positions: Vec<usize> = remaining
            .iter()
            .enumerate()
            .filter(|(_, t)| matches!(t, WhereTerm::Compare(attr, Comparator::Eq, _) if attr == sk))
            .map(|(i, _)| i)
            .collect();
        match sk_positions.len() {
            0 => {
                return Err(PartiqlError::new(format!(
                    "WHERE must also pin the sort key `{sk}` with = \
                     (a mutation targets exactly one item, never a range)"
                )));
            }
            1 => {}
            _ => {
                return Err(PartiqlError::new(format!(
                    "WHERE names the sort key `{sk}` more than once"
                )));
            }
        }
        let sk_term = remaining.remove(sk_positions[0]);
        let WhereTerm::Compare(_, _, sk_idx) = sk_term else {
            unreachable!("sk_positions only selects Compare(_, Eq, _) terms")
        };
        key.insert(sk.to_string(), value_of(*sk_idx));
    }

    let filter = build_filter(&remaining, &value_of);
    Ok((key, filter))
}

/// Lower a parsed `INSERT` onto [`Operation::PutItem`] (ADR 0071's lowering
/// table): `item` from the document; `condition` is
/// `attribute_not_exists(pk)` (`AND attribute_not_exists(sk)` for a
/// composite key) unless `ON CONFLICT DO NOTHING` was given — the caller
/// (`animusd::dynamo::execute_statement`) maps a `ConditionalCheckFailedException`
/// from running this op into `DuplicateItemException`, or swallows it
/// silently when `on_conflict_do_nothing` is set (ADR 0071's lowering
/// table). `partition_key`/`sort_key` are the target's own declared key
/// attribute names, resolved by the caller — this function never reads a
/// schema.
///
/// # Errors
/// A [`PartiqlError`] when the placeholder count doesn't match
/// `parameters.len()`.
pub fn lower_insert(
    stmt: &InsertStatement,
    parameters: &[AttributeValue],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<Operation, PartiqlError> {
    if stmt.placeholder_count != parameters.len() {
        return Err(PartiqlError::new(format!(
            "statement has {} placeholder(s) but Parameters supplied {}",
            stmt.placeholder_count,
            parameters.len()
        )));
    }
    let item: Item = stmt
        .document
        .iter()
        .map(|(k, v)| (k.clone(), lower_value_ast(v, parameters)))
        .collect();
    let mut condition = ConditionExpression::AttributeNotExists(partition_key.to_string());
    if let Some(sk) = sort_key {
        condition = ConditionExpression::And(
            Box::new(condition),
            Box::new(ConditionExpression::AttributeNotExists(sk.to_string())),
        );
    }
    Ok(Operation::PutItem {
        table: stmt.table.clone(),
        item,
        condition: Some(condition),
        return_values: ReturnValues::None,
        capacity: ReturnConsumedCapacity::None,
        metrics: ReturnItemCollectionMetrics::None,
    })
}

/// Lower a parsed `UPDATE` onto [`Operation::UpdateItem`] (ADR 0071's
/// lowering table): `key` from the required exact-match `WHERE`
/// ([`lower_exact_key_where`]); `actions` one `SET` [`UpdateAction`] per
/// assignment; `condition` is `attribute_exists(pk)` AND-folded with any
/// non-key `WHERE` terms (the item must already exist — DynamoDB's own
/// `UPDATE` semantics, surfaced as `ConditionalCheckFailedException` on a
/// missing item); `return_values` from `RETURNING`.
///
/// # Errors
/// A [`PartiqlError`] when the placeholder count doesn't match
/// `parameters.len()`, or `WHERE` doesn't resolve to an exact-match key (see
/// [`lower_exact_key_where`]).
pub fn lower_update(
    stmt: &UpdateStatement,
    parameters: &[AttributeValue],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<Operation, PartiqlError> {
    if stmt.placeholder_count != parameters.len() {
        return Err(PartiqlError::new(format!(
            "statement has {} placeholder(s) but Parameters supplied {}",
            stmt.placeholder_count,
            parameters.len()
        )));
    }
    let (key, filter) =
        lower_exact_key_where(&stmt.where_terms, parameters, partition_key, sort_key)?;

    let mut condition = ConditionExpression::AttributeExists(partition_key.to_string());
    if let Some(f) = filter {
        condition = ConditionExpression::And(Box::new(condition), Box::new(f));
    }

    let actions = stmt
        .assignments
        .iter()
        .map(|(attr, v)| {
            UpdateAction::Set(
                vec![PathSegment::Field(attr.clone())],
                UpdateExpr::value(lower_value_ast(v, parameters)),
            )
        })
        .collect();

    let return_values = match stmt.returning {
        None => UpdateReturnValues::None,
        Some(ReturningMode::AllOld) => UpdateReturnValues::AllOld,
        Some(ReturningMode::AllNew) => UpdateReturnValues::AllNew,
    };

    Ok(Operation::UpdateItem {
        table: stmt.table.clone(),
        key,
        actions,
        condition: Some(condition),
        return_values,
        capacity: ReturnConsumedCapacity::None,
        metrics: ReturnItemCollectionMetrics::None,
    })
}

/// Lower a parsed `DELETE` onto [`Operation::DeleteItem`] (ADR 0071's
/// lowering table): `key` from the required exact-match `WHERE`
/// ([`lower_exact_key_where`]); `condition` is the AND-fold of any non-key
/// `WHERE` terms (`None` when there are none) — unlike `UPDATE`, there is
/// **no** implicit existence condition, matching plain `DeleteItem`'s own
/// AWS semantics (deleting an absent key is a silent no-op success, not an
/// error); `return_values` from `RETURNING` (`ALL NEW *` is rejected at
/// parse time, so only `None`/`AllOld` ever reach here).
///
/// # Errors
/// A [`PartiqlError`] when the placeholder count doesn't match
/// `parameters.len()`, or `WHERE` doesn't resolve to an exact-match key (see
/// [`lower_exact_key_where`]).
pub fn lower_delete(
    stmt: &DeleteStatement,
    parameters: &[AttributeValue],
    partition_key: &str,
    sort_key: Option<&str>,
) -> Result<Operation, PartiqlError> {
    if stmt.placeholder_count != parameters.len() {
        return Err(PartiqlError::new(format!(
            "statement has {} placeholder(s) but Parameters supplied {}",
            stmt.placeholder_count,
            parameters.len()
        )));
    }
    let (key, filter) =
        lower_exact_key_where(&stmt.where_terms, parameters, partition_key, sort_key)?;

    let return_values = match stmt.returning {
        None => ReturnValues::None,
        Some(ReturningMode::AllOld) => ReturnValues::AllOld,
        Some(ReturningMode::AllNew) => {
            unreachable!("RETURNING ALL NEW * on DELETE is rejected at parse time")
        }
    };

    Ok(Operation::DeleteItem {
        table: stmt.table.clone(),
        key,
        condition: filter,
        return_values,
        capacity: ReturnConsumedCapacity::None,
        metrics: ReturnItemCollectionMetrics::None,
    })
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
    fn parse_select_statement_rejects_insert_update_delete() {
        // `parse_select_statement` is the SELECT-only entry point — a
        // statement beginning with another keyword is still rejected here,
        // even though PR 3 supports it through `parse_statement`/the
        // dedicated `parse_{insert,update,delete}_statement` functions.
        for text in [
            "INSERT INTO t VALUE {'pk':?}",
            "UPDATE t SET a = ? WHERE pk = ?",
            "DELETE FROM t WHERE pk = ?",
        ] {
            let err = parse_select_statement(text).unwrap_err();
            assert!(err.message.contains("SELECT"), "{}: {}", text, err.message);
        }
    }

    #[test]
    fn parse_statement_dispatches_on_leading_keyword() {
        assert!(matches!(
            parse_statement("SELECT * FROM t").unwrap(),
            Statement::Select(_)
        ));
        assert!(matches!(
            parse_statement("INSERT INTO t VALUE {'pk':?}").unwrap(),
            Statement::Insert(_)
        ));
        assert!(matches!(
            parse_statement("UPDATE t SET a = ? WHERE pk = ?").unwrap(),
            Statement::Update(_)
        ));
        assert!(matches!(
            parse_statement("DELETE FROM t WHERE pk = ?").unwrap(),
            Statement::Delete(_)
        ));
    }

    #[test]
    fn parse_statement_rejects_unrecognized_leading_keyword() {
        let err = parse_statement("BOGUS").unwrap_err();
        assert!(err.message.contains("SELECT"), "{}", err.message);
        assert!(err.message.contains("INSERT"), "{}", err.message);
        assert!(err.message.contains("UPDATE"), "{}", err.message);
        assert!(err.message.contains("DELETE"), "{}", err.message);
    }

    #[test]
    fn statement_table_resolves_for_every_kind() {
        assert_eq!(parse_statement("SELECT * FROM t").unwrap().table(), "t");
        assert_eq!(
            parse_statement("INSERT INTO t VALUE {'pk':?}")
                .unwrap()
                .table(),
            "t"
        );
        assert_eq!(
            parse_statement("UPDATE t SET a = ? WHERE pk = ?")
                .unwrap()
                .table(),
            "t"
        );
        assert_eq!(
            parse_statement("DELETE FROM t WHERE pk = ?")
                .unwrap()
                .table(),
            "t"
        );
    }

    // -- INSERT -------------------------------------------------------------

    #[test]
    fn parses_insert_document() {
        let stmt = parse_insert_statement("INSERT INTO \"t\" VALUE {'pk': ?, 'a': ?}").unwrap();
        assert_eq!(stmt.table, "t");
        assert_eq!(
            stmt.document,
            vec![
                ("pk".to_string(), ValueAst::Placeholder(0)),
                ("a".to_string(), ValueAst::Placeholder(1)),
            ]
        );
        assert!(!stmt.on_conflict_do_nothing);
    }

    #[test]
    fn parses_insert_nested_document_and_list() {
        let stmt = parse_insert_statement(
            "INSERT INTO t VALUE {'pk': ?, 'nested': {'x': ?}, 'tags': [?, ?]}",
        )
        .unwrap();
        assert_eq!(
            stmt.document[1],
            (
                "nested".to_string(),
                ValueAst::Document(vec![("x".to_string(), ValueAst::Placeholder(1))])
            )
        );
        assert_eq!(
            stmt.document[2],
            (
                "tags".to_string(),
                ValueAst::List(vec![ValueAst::Placeholder(2), ValueAst::Placeholder(3)])
            )
        );
    }

    #[test]
    fn parses_insert_empty_document() {
        let stmt = parse_insert_statement("INSERT INTO t VALUE {}").unwrap();
        assert!(stmt.document.is_empty());
    }

    #[test]
    fn parses_insert_on_conflict_do_nothing() {
        let stmt =
            parse_insert_statement("INSERT INTO t VALUE {'pk': ?} ON CONFLICT DO NOTHING").unwrap();
        assert!(stmt.on_conflict_do_nothing);
    }

    #[test]
    fn rejects_literal_in_document() {
        let err = parse_insert_statement("INSERT INTO t VALUE {'pk': 'x'}").unwrap_err();
        assert!(err.message.contains("literal values"), "{}", err.message);
    }

    #[test]
    fn rejects_document_key_that_is_not_a_quoted_string() {
        let err = parse_insert_statement("INSERT INTO t VALUE {pk: ?}").unwrap_err();
        assert!(
            err.message.contains("quoted document key"),
            "{}",
            err.message
        );
    }

    #[test]
    fn insert_requires_into_and_value_keywords() {
        assert!(parse_insert_statement("INSERT t VALUE {'pk':?}").is_err());
        assert!(parse_insert_statement("INSERT INTO t {'pk':?}").is_err());
    }

    // -- UPDATE ---------------------------------------------------------

    #[test]
    fn parses_update_set_and_where() {
        let stmt = parse_update_statement("UPDATE t SET a = ?, b = ? WHERE pk = ?").unwrap();
        assert_eq!(stmt.table, "t");
        assert_eq!(
            stmt.assignments,
            vec![
                ("a".to_string(), ValueAst::Placeholder(0)),
                ("b".to_string(), ValueAst::Placeholder(1)),
            ]
        );
        assert_eq!(
            stmt.where_terms,
            vec![WhereTerm::Compare("pk".into(), Comparator::Eq, 2)]
        );
        assert_eq!(stmt.returning, None);
    }

    #[test]
    fn update_requires_where_clause() {
        assert!(parse_update_statement("UPDATE t SET a = ?").is_err());
    }

    #[test]
    fn parses_update_returning_all_new() {
        let stmt =
            parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL NEW *").unwrap();
        assert_eq!(stmt.returning, Some(ReturningMode::AllNew));
    }

    #[test]
    fn parses_update_returning_all_old() {
        let stmt =
            parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL OLD *").unwrap();
        assert_eq!(stmt.returning, Some(ReturningMode::AllOld));
    }

    #[test]
    fn rejects_unknown_returning_mode() {
        let err = parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL BOGUS *")
            .unwrap_err();
        assert!(err.message.contains("OLD or NEW"), "{}", err.message);
    }

    #[test]
    fn rejects_returning_missing_star() {
        let err = parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL OLD")
            .unwrap_err();
        assert!(
            err.message.contains("`*`") || err.message.contains("Star"),
            "{}",
            err.message
        );
    }

    // -- DELETE ---------------------------------------------------------

    #[test]
    fn parses_delete_where() {
        let stmt = parse_delete_statement("DELETE FROM t WHERE pk = ?").unwrap();
        assert_eq!(stmt.table, "t");
        assert_eq!(
            stmt.where_terms,
            vec![WhereTerm::Compare("pk".into(), Comparator::Eq, 0)]
        );
        assert_eq!(stmt.returning, None);
    }

    #[test]
    fn delete_requires_where_clause() {
        assert!(parse_delete_statement("DELETE FROM t").is_err());
    }

    #[test]
    fn parses_delete_returning_all_old() {
        let stmt =
            parse_delete_statement("DELETE FROM t WHERE pk = ? RETURNING ALL OLD *").unwrap();
        assert_eq!(stmt.returning, Some(ReturningMode::AllOld));
    }

    #[test]
    fn rejects_delete_returning_all_new() {
        let err =
            parse_delete_statement("DELETE FROM t WHERE pk = ? RETURNING ALL NEW *").unwrap_err();
        assert!(err.message.contains("no new image"), "{}", err.message);
    }

    // -- Lowering: INSERT -------------------------------------------------

    #[test]
    fn lowers_insert_simple_key() {
        let stmt = parse_insert_statement("INSERT INTO t VALUE {'pk': ?, 'a': ?}").unwrap();
        let params = vec![av_s("k1"), av_s("v1")];
        let op = lower_insert(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::PutItem {
                table,
                item,
                condition,
                return_values,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(item.get("pk"), Some(&av_s("k1")));
                assert_eq!(item.get("a"), Some(&av_s("v1")));
                assert_eq!(
                    condition,
                    Some(ConditionExpression::AttributeNotExists("pk".into()))
                );
                assert_eq!(return_values, ReturnValues::None);
            }
            other => panic!("expected PutItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_insert_composite_key_condition() {
        let stmt = parse_insert_statement("INSERT INTO t VALUE {'pk': ?, 'sk': ?}").unwrap();
        let params = vec![av_s("k1"), av_s("s1")];
        let op = lower_insert(&stmt, &params, "pk", Some("sk")).expect("lowers");
        match op {
            Operation::PutItem { condition, .. } => {
                assert_eq!(
                    condition,
                    Some(ConditionExpression::And(
                        Box::new(ConditionExpression::AttributeNotExists("pk".into())),
                        Box::new(ConditionExpression::AttributeNotExists("sk".into())),
                    ))
                );
            }
            other => panic!("expected PutItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_insert_nested_document_and_list_values() {
        let stmt =
            parse_insert_statement("INSERT INTO t VALUE {'pk': ?, 'n': {'x': ?}, 'l': [?, ?]}")
                .unwrap();
        let params = vec![av_s("k1"), av_s("nx"), av_s("l0"), av_s("l1")];
        let op = lower_insert(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::PutItem { item, .. } => {
                let mut nested = Item::new();
                nested.insert("x".into(), av_s("nx"));
                assert_eq!(item.get("n"), Some(&AttributeValue::M(nested)));
                assert_eq!(
                    item.get("l"),
                    Some(&AttributeValue::L(vec![av_s("l0"), av_s("l1")]))
                );
            }
            other => panic!("expected PutItem, got {other:?}"),
        }
    }

    #[test]
    fn insert_rejects_placeholder_count_mismatch() {
        let stmt = parse_insert_statement("INSERT INTO t VALUE {'pk': ?}").unwrap();
        let params: Vec<AttributeValue> = vec![];
        let err = lower_insert(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("placeholder"), "{}", err.message);
    }

    // -- Lowering: UPDATE -------------------------------------------------

    #[test]
    fn lowers_update_simple_key() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE pk = ?").unwrap();
        let params = vec![av_s("v1"), av_s("k1")];
        let op = lower_update(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::UpdateItem {
                table,
                key,
                actions,
                condition,
                return_values,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(key.get("pk"), Some(&av_s("k1")));
                assert_eq!(
                    actions,
                    vec![UpdateAction::Set(
                        vec![PathSegment::Field("a".into())],
                        UpdateExpr::value(av_s("v1")),
                    )]
                );
                assert_eq!(
                    condition,
                    Some(ConditionExpression::AttributeExists("pk".into()))
                );
                assert_eq!(return_values, UpdateReturnValues::None);
            }
            other => panic!("expected UpdateItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_update_composite_key() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE pk = ? AND sk = ?").unwrap();
        let params = vec![av_s("v1"), av_s("k1"), av_s("s1")];
        let op = lower_update(&stmt, &params, "pk", Some("sk")).expect("lowers");
        match op {
            Operation::UpdateItem { key, .. } => {
                assert_eq!(key.get("pk"), Some(&av_s("k1")));
                assert_eq!(key.get("sk"), Some(&av_s("s1")));
            }
            other => panic!("expected UpdateItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_update_non_key_where_term_into_condition() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE pk = ? AND other = ?").unwrap();
        let params = vec![av_s("v1"), av_s("k1"), av_s("o1")];
        let op = lower_update(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::UpdateItem { condition, .. } => {
                assert_eq!(
                    condition,
                    Some(ConditionExpression::And(
                        Box::new(ConditionExpression::AttributeExists("pk".into())),
                        Box::new(ConditionExpression::Compare(
                            "other".into(),
                            Comparator::Eq,
                            av_s("o1")
                        )),
                    ))
                );
            }
            other => panic!("expected UpdateItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_update_returning_modes() {
        let params = vec![av_s("v1"), av_s("k1")];
        let old =
            parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL OLD *").unwrap();
        assert!(matches!(
            lower_update(&old, &params, "pk", None).unwrap(),
            Operation::UpdateItem {
                return_values: UpdateReturnValues::AllOld,
                ..
            }
        ));
        let new =
            parse_update_statement("UPDATE t SET a = ? WHERE pk = ? RETURNING ALL NEW *").unwrap();
        assert!(matches!(
            lower_update(&new, &params, "pk", None).unwrap(),
            Operation::UpdateItem {
                return_values: UpdateReturnValues::AllNew,
                ..
            }
        ));
    }

    #[test]
    fn update_rejects_missing_key_term() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE other = ?").unwrap();
        let params = vec![av_s("v1"), av_s("o1")];
        let err = lower_update(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("partition key"), "{}", err.message);
    }

    #[test]
    fn update_rejects_composite_key_missing_sort_term() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE pk = ?").unwrap();
        let params = vec![av_s("v1"), av_s("k1")];
        let err = lower_update(&stmt, &params, "pk", Some("sk")).unwrap_err();
        assert!(err.message.contains("sort key"), "{}", err.message);
    }

    #[test]
    fn update_rejects_placeholder_count_mismatch() {
        let stmt = parse_update_statement("UPDATE t SET a = ? WHERE pk = ?").unwrap();
        let params = vec![av_s("v1")];
        let err = lower_update(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("placeholder"), "{}", err.message);
    }

    // -- Lowering: DELETE -------------------------------------------------

    #[test]
    fn lowers_delete_simple_key_no_implicit_condition() {
        let stmt = parse_delete_statement("DELETE FROM t WHERE pk = ?").unwrap();
        let params = vec![av_s("k1")];
        let op = lower_delete(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::DeleteItem {
                table,
                key,
                condition,
                return_values,
                ..
            } => {
                assert_eq!(table, "t");
                assert_eq!(key.get("pk"), Some(&av_s("k1")));
                assert_eq!(condition, None);
                assert_eq!(return_values, ReturnValues::None);
            }
            other => panic!("expected DeleteItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_delete_non_key_where_term_into_condition() {
        let stmt = parse_delete_statement("DELETE FROM t WHERE pk = ? AND other = ?").unwrap();
        let params = vec![av_s("k1"), av_s("o1")];
        let op = lower_delete(&stmt, &params, "pk", None).expect("lowers");
        match op {
            Operation::DeleteItem { condition, .. } => {
                assert_eq!(
                    condition,
                    Some(ConditionExpression::Compare(
                        "other".into(),
                        Comparator::Eq,
                        av_s("o1")
                    ))
                );
            }
            other => panic!("expected DeleteItem, got {other:?}"),
        }
    }

    #[test]
    fn lowers_delete_returning_all_old() {
        let stmt =
            parse_delete_statement("DELETE FROM t WHERE pk = ? RETURNING ALL OLD *").unwrap();
        let params = vec![av_s("k1")];
        let op = lower_delete(&stmt, &params, "pk", None).expect("lowers");
        assert!(matches!(
            op,
            Operation::DeleteItem {
                return_values: ReturnValues::AllOld,
                ..
            }
        ));
    }

    #[test]
    fn delete_rejects_non_key_only_where() {
        // No term names the partition key at all — every term is a filter,
        // which is exactly the "no key term" rejection (never widened into
        // a scan-and-delete).
        let stmt = parse_delete_statement("DELETE FROM t WHERE other = ?").unwrap();
        let params = vec![av_s("o1")];
        let err = lower_delete(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("partition key"), "{}", err.message);
    }

    #[test]
    fn delete_rejects_two_partition_key_equalities() {
        let stmt = parse_delete_statement("DELETE FROM t WHERE pk = ? AND pk = ?").unwrap();
        let params = vec![av_s("a"), av_s("b")];
        let err = lower_delete(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("more than once"), "{}", err.message);
    }

    #[test]
    fn delete_rejects_placeholder_count_mismatch() {
        let stmt = parse_delete_statement("DELETE FROM t WHERE pk = ?").unwrap();
        let params: Vec<AttributeValue> = vec![];
        let err = lower_delete(&stmt, &params, "pk", None).unwrap_err();
        assert!(err.message.contains("placeholder"), "{}", err.message);
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
