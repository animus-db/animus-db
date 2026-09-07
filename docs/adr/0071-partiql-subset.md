# ADR 0071 — PartiQL subset (W-07)

- **Status:** Accepted
- **Date:** 2026-09-07
- **Origin:** roadmap W-07 ("PartiQL: `ExecuteStatement`,
  `BatchExecuteStatement`, `ExecuteTransaction`") — sized honestly there as
  XL and split into five PRs; this ADR is PR 1.
- **Amends:** none. **Depends on:** ADR 0006 (wire adapter), ADR 0018 (2PC
  transactions, for `ExecuteTransaction`), ADR 0054 step 1 (`animus-item`'s
  `condition`/key-encoding types this reuses), ADR 0063 (order-preserving
  `N` key encoding — why value literals must never be interpolated as text),
  ADR 0065/0066 (capacity/throttle and authz classification).

## Context

DynamoDB's PartiQL surface (`ExecuteStatement`, `BatchExecuteStatement`,
`ExecuteTransaction`) lets a client write SQL-shaped `SELECT`/`INSERT`/
`UPDATE`/`DELETE` statements instead of building `Query`/`PutItem`/
`UpdateItem`/`DeleteItem`/`TransactWriteItems` request shapes by hand. Some
SDKs and most of the AWS CLI's interactive tooling default to it. This
adapter has none of it today — every PartiQL grep across code, docs, and
`website/` in preparation for this ADR came back empty.

The honest sizing concern from the roadmap holds: a real SQL-ish parser, a
new error surface, and — the part most likely to go quietly wrong — a
WHERE-to-key-bound-or-filter compiler that must not diverge from what
`KeyConditionExpression`/`ConditionExpression` already mean for `Query`/
`Scan`/conditional writes. This ADR's job is to pin a subset narrow enough
to build and verify in five reviewable PRs, and specifically to pin the two
decisions a parser can get wrong in ways that are expensive later: what
literal syntax is even legal (§2), and how a `WHERE` clause is split between
"this pins a key" and "this is a filter" (§4).

This crate (`animus-dynamo`) already depends on `animus-control` (for the
replicated schema catalog types) and on `animus-item` (`AttributeValue`,
`Item`, `TableSchema`, and `condition::{Comparator, SortKeyCondition,
ConditionExpression}`) — the layering PartiQL needs already exists; no new
crate dependency is introduced by this feature.

## Decision

### 1. Grammar — the exact supported subset, all five PRs

One EBNF-ish grammar, built incrementally: PR 2 implements `select_stmt`
only; PR 3 adds `insert_stmt`/`update_stmt`/`delete_stmt`; PR 4 reuses
`select_stmt`/`insert_stmt`/... unchanged inside `BatchExecuteStatement`
(one statement per batch entry, no new grammar); PR 5 reuses all four
inside `ExecuteTransaction`. A statement not matching this grammar, or
matching it but naming a feature explicitly out of scope (§7), is a
`ValidationException` naming what was rejected and why — never a silent
best-effort parse.

```
statement      := select_stmt | insert_stmt | update_stmt | delete_stmt

select_stmt    := "SELECT" projection "FROM" from_clause
                   [ where_clause ] [ order_by_clause ]
projection     := "*" | ident ("," ident)*
from_clause    := ident [ "." ident ]              ; table ["." index]
where_clause   := "WHERE" predicate ("AND" predicate)*
order_by_clause:= "ORDER" "BY" ident [ "ASC" | "DESC" ]

insert_stmt    := "INSERT" "INTO" ident "VALUE" document
                   [ "ON CONFLICT DO NOTHING" ]     ; PR 3
document       := "{" [ pair ("," pair)* ] "}"
pair           := string ":" value

update_stmt    := "UPDATE" ident "SET" assignment ("," assignment)*
                   where_clause                     ; PR 3, WHERE required
assignment     := ident "=" value

delete_stmt    := "DELETE" "FROM" ident where_clause ; PR 3, WHERE required

predicate      := ident comparator "?"
                 | ident "BETWEEN" "?" "AND" "?"
                 | "begins_with" "(" ident "," "?" ")"
                 | "contains" "(" ident "," "?" ")"
                 | "attribute_exists" "(" ident ")"
                 | "attribute_not_exists" "(" ident ")"
comparator     := "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="

ident          := bare_ident | '"' quoted_ident '"'
bare_ident     := [A-Za-z_][A-Za-z0-9_]*
value          := "?"                               ; PR 3 — see §2
```

Notably absent, permanently (§7): joins, subqueries, arbitrary function
calls beyond the three the condition evaluator already has
(`begins_with`/`contains`/`attribute_exists`/`attribute_not_exists`), a
`LIMIT` keyword (DynamoDB has no such keyword — page size is the request's
own `Limit` field, exactly as it is for `Query`/`Scan`), `OR`/`NOT`/
parenthesised grouping in a `WHERE` clause (top level is a **conjunction
only** — see §7.1 for why this is deliberately narrower than
`ConditionExpression`'s own grammar), and nested document paths in a
projection or `WHERE` term (`a.b`, `a[0]`) — every `ident` here names a
top-level attribute only.

### 2. Placeholder-only discipline — `?` is the only value syntax

Every value that participates in a `WHERE` predicate, an `INSERT` document,
or an `UPDATE` assignment **must** be a `?` placeholder, bound positionally
against the request's `Parameters` array. A literal written directly into
the statement text — a quoted string (`WHERE pk = 'x'`), a bare number
(`WHERE n > 3`), `true`/`false`/`NULL` — is rejected as
`ValidationException: literal values are not supported in this statement; \
use ? placeholders bound via Parameters` at parse time, even though the
grammar's lexer recognizes these tokens (so the error names the literal,
rather than falling through to a generic "unexpected token").

This is stricter than AWS's own PartiQL, which does allow inline literals.
Two reasons, both load-bearing enough to accept the incompatibility:

1. **Injection discipline.** The moment a value can appear as statement
   text, the boundary between "this byte sequence is code" and "this byte
   sequence is data" lives inside a hand-written lexer instead of at the
   `Parameters` array's own JSON typing. Every other write path in this
   adapter (`PutItem`'s `Item`, `UpdateItem`'s `ExpressionAttributeValues`,
   `Query`'s `KeyConditionExpression` placeholders) already keeps that
   boundary at the JSON/wire layer, never inside expression text; PartiQL
   placeholder-only keeps the same invariant instead of adding the one
   surface in this adapter where a string is parsed for its *value*, not
   just its *shape*.
2. **ADR 0063's number encoding.** A stored `N` key sorts by the
   order-preserving `numkey` byte layout, not by decimal text. Every other
   path that turns a client-supplied number into an `AttributeValue::N`
   does it through typed JSON decoding (`serde_json::Value::as_f64`/string
   number forms), never through parsing digits out of free-form text. A
   PartiQL lexer that parsed `3.5e10` or `-0` or a very-long-digit-run
   number literal out of raw statement text would need to reproduce that
   decoding exactly or risk a value that round-trips differently than the
   same number sent through `Parameters` — a second, easy-to-drift copy of
   a rule ADR 0063 already centralized at one choke point
  (`AttributeValue::key_bytes`). Placeholder-only means every bound value
  flows through the *same* JSON `AttributeValue` decoder every other
  operation already uses (`decode_attribute_value`), so there is only ever
  one number-decoding path in this crate.

`Parameters` is decoded exactly like any other `AttributeValue` array (the
same `decode_attribute_value` every other operation uses); a `?`'s
positional index into that array is resolved at lowering time, not parse
time (parsing only counts placeholders and records their order). A
placeholder count mismatch — a statement with `N` question marks but a
`Parameters` array of a different length — is a `ValidationException`
naming both counts. AWS's own behavior here is unspecified in the docs
available; this is the conservative reading (fail closed on a mismatch
rather than guess which extra parameter was meant to bind where).

### 3. Lexer/tokenizer: hand-written, not reused from W-01

The roadmap flagged reusing W-01's `UpdateExpression` tokenizer
(`animus_dynamo::wire`'s string-expression parser for
`UpdateExpression`/`ConditionExpression`/`KeyConditionExpression`, which
lives in `wire.rs` since it needs `ExpressionAttributeNames`/
`ExpressionAttributeValues` resolution) as a candidate to generalize.
Having read it closely: **it does not generalize, and PartiQL gets its own
small hand-written lexer instead.** Three reasons:

- W-01's tokenizer is not really a tokenizer — it is a family of
  recursive-descent functions (`decode_predicate_or`/`_and`/`_not`/`_expr`,
  `find_top_level`, `split_comparator`, `func_arg`) that scan **already
  fully-formed expression strings** for keyword boundaries at
  paren-depth-zero, immediately resolving `:value`/`#name` placeholders
  against the request's `ExpressionAttributeValues`/`Names` maps as they
  go. PartiQL's surface syntax is different enough (double-quoted
  identifiers, a `FROM "table"."index"` clause, `SELECT`/`ORDER BY`
  keywords none of that grammar has) that "reuse" would mean either
  bolting PartiQL-only cases onto a parser whose whole shape assumes
  DynamoDB expression grammar, or re-deriving the same left-to-right
  scanning primitives (`find_top_level`, `split_comparator`, `func_arg`)
  under new names anyway — no real code is shared either way.
- Placeholder resolution is structurally different: W-01 resolves a
  `:name`/`#name` **string key** against a JSON map at parse time; PartiQl
  resolves a **positional** `?` against an array, and only once the
  surrounding `WHERE`/`SET` clause has been split into key-vs-filter terms
  (§4) — an ordering W-01's parser has no equivalent step for.
- What *is* reused, deliberately, is one layer up: the **types** `?`
  ultimately produces — `Comparator`, `SortKeyCondition`,
  `ConditionExpression` (all `animus-item::condition`) — are built
  directly by PartiQL's own lowering, the same types W-01's string parser
  also ultimately builds. See §5 for why this is "reuse the evaluator,
  not the string decoder" rather than "reuse nothing."

`crates/animus-dynamo/src/partiql.rs` therefore owns a small,
deterministic, hand-written lexer (no regex crate, no parser-combinator
crate, no dependency addition — matching root `CLAUDE.md`'s determinism
posture and this ADR's own explicit **no new crate** rule from the roadmap
sizing) producing a flat token stream (keyword/ident/quoted-ident/`?`/
comparator/punctuation), and a recursive-descent parser over the grammar in
§1 producing a typed AST (`SelectStatement` for PR 2; `InsertStatement`/
`UpdateStatement`/`DeleteStatement` for PR 3) — never a partial/generic
expression AST shared with `wire.rs`'s.

### 4. The key-versus-filter decision rule

A `SELECT`'s `WHERE` clause (§1: a flat top-level conjunction of predicate
terms, no `OR`/parens) lowers against **one** target: the base table, or —
when `FROM "table"."index"` names one — that secondary index. The target's
own partition-key attribute name (`hash_attribute` for an index,
`TableSchema::partition_key` for the base table) and optional sort-key
attribute name (`sort_attribute`/`TableSchema::sort_key`) are resolved by
the caller (`animusd`, which holds the replicated catalog) and handed to
`lower_select` as plain `&str`s — `animus-dynamo::partiql` itself never
reads `Metadata`, keeping the parser pure and catalog-free like every other
module in this crate.

The rule, applied once those two names are known:

1. Scan the WHERE clause's top-level terms for **one** term of the exact
   shape `<partition-key-attr> = ?`. If none exists, the statement lowers
   to `Operation::Scan` — **every** WHERE term (including one that happens
   to compare the partition or sort key with a non-equality operator)
   becomes part of the `FilterExpression`. This mirrors `Scan`'s own
   contract exactly: `Scan` has no `KeyConditionExpression`, index or not.
2. If exactly one such term exists, it is consumed as the key equality —
   the statement lowers to `Operation::Query`. Then scan the *remaining*
   terms for **one** term whose attribute is the target's declared
   sort-key attribute and whose comparator is one
   `SortKeyCondition` supports (`=`, `<`, `<=`, `>`, `>=`, `BETWEEN`,
   `begins_with`) — real DynamoDB's own `KeyConditionExpression`
   restriction, so `<>`/`contains`/`attribute_exists` on the sort key never
   qualifies even though they are legal *filter* terms. At most one such
   term is consumed (DynamoDB itself allows only one sort-key condition);
   if more than one term matches, the **first** encountered (left to
   right) is taken as the sort condition and every other one — including a
   second sort-key term — falls through to the filter, unchanged from
   how it would evaluate as a filter anyway (conservative: never silently
   drops a clause, always still applies it, just after the key range
   rather than as part of it).
3. **Two** or more terms of the exact partition-key-equality shape is a
   `ValidationException` ("`WHERE` names the partition key more than
   once") — DynamoDB's own `KeyConditionExpression` grammar has no way to
   AND two equalities on the same key either, and silently taking the
   first would hide a very likely client bug (a copy-paste WHERE clause).
4. Every term not consumed by steps 1–3 is AND-joined (in original
   statement order) into a `FilterExpression`; zero remaining terms means
   no `FilterExpression` at all (`None`, not an empty string) — matching
   how `decode_predicate` treats an absent field.

This is a **narrower** version of what real DynamoDB PartiQL's own planner
does (AWS does not publicly document its own decision procedure in enough
detail to match byte-for-byte, so — per this task's own "where unsure,
choose the conservative reading" instruction — this ADR pins the narrowest
rule that is still useful: an explicit, syntactic, single top-level
equality term, never an inferred-from-multiple-clauses key bound). A
`SELECT` that could, with a cleverer planner, still resolve to a `Query`
but doesn't match this rule (e.g. an `OR`ed pair of partition-key
equalities, or a key equality nested under a not-yet-supported grouping)
falls through to a `Scan` — always *correct*, just not always *optimal* —
never an error and never a wrong answer.

### 5. Reuse: types, not string decoders

`lower_select`'s WHERE-term lowering builds `animus_item::condition` values
**directly** from PartiQL's own typed AST — `Comparator::{Eq,Ne,Lt,Le,Gt,
Ge}` for a plain comparator term, `SortKeyCondition::{Compare,Between,
BeginsWith}` for a consumed sort-key term, `ConditionExpression::{Compare,
Between,BeginsWith,Contains,AttributeExists,AttributeNotExists}` for every
filter term (AND-folded with `ConditionExpression::And`) — rather than
re-serializing the AST into a `KeyConditionExpression`/`FilterExpression`
string and re-parsing it through `wire.rs`'s string decoder
(`decode_sort_condition`/`decode_predicate`). This is deliberate: PartiQL
has already done the parsing (§3); round-tripping through text a second
time would mean re-deriving quoting/escaping rules for attribute names
purely to throw them away again one function call later, and would make
every future `ConditionExpression` variant need updating in two decoders
that are supposed to agree instead of one. What genuinely is reused is
every *evaluator* downstream of that point unchanged — `SortKeyCondition::
matches`/`matches_raw`, `ConditionExpression::evaluate`, and the entire
`animusd::dynamo::run_query`/`run_scan`/`run_index_query`/`run_index_scan`
execution path, which never learns a `Query`/`Scan` it is asked to run
originated from a `SELECT` rather than a client-built request.

`INSERT`/`UPDATE`/`DELETE` (PR 3) reuse the identical discipline: an
`INSERT`'s `document` lowers straight to an `Item` (the same
`decode_attribute_value` typed decode, never a hand-rolled JSON-in-SQL
parser); an `UPDATE`'s `assignment` list lowers to `Vec<UpdateAction>`
(`animus_item::update`'s data model, built directly — the same "types, not
string decoder" reasoning as above, W-01's `UpdateExpression` string parser
not reused for the identical reasons §3 gives); both `UPDATE`/`DELETE`'s
required `WHERE` clause lowers through the *same* `lower_select`-adjacent
term-to-key logic (a `PutItem`/`UpdateItem`/`DeleteItem` needs a full key —
partition, and sort if the table has one — not a `Query`'s partial key
condition, so `WHERE` must resolve to an exact-match key, both attrs
present as `=` terms with no lingering unconsumed terms; anything else is
`ValidationException`, since DynamoDB's own PartiQL mutation statements
target exactly one item, never a range).

### 6. Projection

`SELECT *` sets no `ProjectionExpression` (the response's `Select` is
`AllAttributes`, or `AllProjectedAttributes` when `FROM` names an index —
mirroring `decode_select`'s own default logic exactly). `SELECT a, b, c`
builds a `Projection` naming those top-level attributes directly
(`animus_item::PathSegment::Field`, no nested-path parsing needed since
§1's grammar restricts a projection item to a bare/quoted `ident`) and sets
`Select::SpecificAttributes`. No `ProjectionExpression` string round-trip —
same "types, not text" reasoning as §5.

### 7. Out of scope (permanent, not just "not yet")

- **Joins, subqueries.** DynamoDB PartiQL has neither; nothing to defer.
- **Functions beyond the three the condition evaluator already has** —
  `begins_with`/`contains`/`attribute_exists`/`attribute_not_exists`.
  `attribute_type`/`size` (which `ConditionExpression` *does* support for
  conditional writes) are deliberately excluded from `WHERE` — real
  DynamoDB's PartiQL `WHERE` grammar does not document them as legal
  either, and the conservative reading excludes rather than guesses.
- **`RETURNING` beyond what DynamoDB's own PartiQL supports** — AWS's
  `RETURNING ALL OLD *` / `RETURNING ALL NEW *` on `INSERT`/`UPDATE`/
  `DELETE` is deferred past PR 3 entirely (not attempted this wave); PR 3's
  mutations answer the bare DynamoDB PartiQL default (no item echoed),
  matching `ReturnValues: NONE`.
- **No `LIMIT` keyword.** DynamoDB has none; page size is the request's own
  `Limit` field (§1), exactly like `Query`/`Scan`.
- **`OR`/`NOT`/parenthesised grouping in `WHERE`** (§1, §4.4) — deferred
  indefinitely, not just to a later PR; revisit only if a real workload
  needs it, since every `WHERE` shape this excludes still has a correct
  (if not always optimal) `Scan`-with-filter fallback under §4's rule for
  the cases it does parse, and grouping doesn't reach even that fallback
  today (a parse-time `ValidationException`, not a wrong answer).
- **Nested document paths** (`a.b`, `a[0]`) anywhere in this statement
  grammar — projection, `WHERE`, `SET`. `INSERT`'s whole-document value is
  the one place nesting already exists uncompromised (§1's `document`
  grammar is recursive), since an item's own attribute values are already
  arbitrarily nested regardless of how PartiQL names them.

## Lowering table

| Statement shape | `Operation` variant | Key fields |
|---|---|---|
| `SELECT` whose `WHERE` has a partition-key equality term (§4.2) | `Query` | `partition_attr`/`partition_value` from the consumed term; `sort_attr`/`sort_condition` from the consumed sort term, if any; `filter` from the remaining terms; `index` from `FROM "t"."i"`; `projection`/`select` per §6; `scan_index_forward` from `ORDER BY` (§8); `limit`/`exclusive_start_key` from the request's own `Limit`/decoded `NextToken` (§9); `consistent_read` from the request |
| `SELECT` with no such term | `Scan` | `filter` from every WHERE term; `index` from `FROM`; `projection`/`select`/`limit`/`exclusive_start_key`/`consistent_read` as above; `segment` always `None` (PartiQL has no parallel-scan syntax) |
| `INSERT INTO "t" VALUE {..}` (PR 3) | `PutItem` | `item` from the document; `condition` is `AttributeNotExists(pk)` (and, for a composite key, `AND AttributeNotExists(sk)`) unless `ON CONFLICT DO NOTHING` is given, in which case a condition failure is swallowed rather than raised — AWS's own default `INSERT` semantics (fail if the item exists) vs. the opt-out |
| `UPDATE "t" SET a = ? [, ..] WHERE pk = ? [AND sk = ?]` (PR 3) | `UpdateItem` | `key` from the required exact-match `WHERE` (§5); `actions` one `SET` `UpdateAction` per assignment |
| `DELETE FROM "t" WHERE pk = ? [AND sk = ?]` (PR 3) | `DeleteItem` | `key` from the required exact-match `WHERE` |
| `BatchExecuteStatement` (PR 4) | one `Operation` per statement, run independently (no cross-statement atomicity — mirrors `BatchWriteItem`/`BatchGetItem`'s own "per-request, not transactional" contract) | — |
| `ExecuteTransaction` (PR 5) | `TransactWriteItems` (mutations) / `TransactGetItems` (an all-`SELECT` transaction) | Each statement becomes one `TransactAction`/`TransactGet`; a transaction mixing `SELECT` with a mutation is a `ValidationException` (matching AWS: a transaction is all-reads or all-writes) |

### 8. `ORDER BY`

Legal only when the statement lowers to `Query` (§4) and the named
attribute is exactly the target's own declared sort-key attribute — naming
anything else, or appearing on a `Scan` lowering, is a `ValidationException`
("`ORDER BY` is only supported on the sort key of a `Query`-shaped
`SELECT`"). `ASC` (or the keyword omitted, the default) sets
`scan_index_forward: true`; `DESC` sets it `false` — `Query`'s own existing
field, unchanged semantics.

### 9. Pagination — `NextToken` encoding

Real DynamoDB's `ExecuteStatement` response carries an opaque `NextToken`,
not `Query`/`Scan`'s transparent `LastEvaluatedKey` item map. This adapter
mints its own opaque token rather than exposing the underlying
`LastEvaluatedKey` directly, so a `NextToken` a client received from one
`ExecuteStatement` call can never be handed back into a *different*
statement (a footgun DynamoDB's own opaque token also closes) and so the
encoding has room to change later without a wire compatibility promise
(root `CLAUDE.md`: no back-compat guaranteed between revisions anyway, but
an opaque token is the honest shape regardless).

Encoding: `NextToken` is base64 (standard alphabet, no padding stripped) of
a small versioned JSON envelope:

```json
{"v": 1, "stmt": "<lowercase-hex SHA-256 of the exact statement text>", "lek": { ..DynamoDB-JSON-encoded LastEvaluatedKey item.. }}
```

- `v` — a format version (`1` today); an unrecognized version is
  `ValidationException: unrecognized NextToken version` rather than a
  silent misparse.
- `stmt` — `sha256(statement.as_bytes())` hex, the same "hash the decoded,
  canonical form" pattern `transact_write_fingerprint` already uses
  elsewhere in this crate (`wire.rs`), except hashing the **raw statement
  string** here (there is no decoded/canonical AST form worth hashing
  instead — two byte-identical statement strings always parse identically,
  since the parser is pure and deterministic). A `NextToken` presented
  alongside a *different* `Statement` string (even one that is
  semantically equivalent after whitespace/case differences) fails this
  hash check and is rejected — `ValidationException: NextToken does not
  match this statement` — deliberately conservative (§2's "when unsure,
  fail closed" posture) rather than attempting statement-equivalence
  detection.
- `lek` — the prior page's `LastEvaluatedKey`, DynamoDB-JSON-encoded
  exactly like `Query`/`Scan`'s own wire shape (`encode_item`), decoded
  back into the `Item` `lower_select` hands `Operation::Query`/`Scan` as
  `exclusive_start_key`.

`Parameters` are deliberately **not** part of the hash. A retried page
request with the same statement text but (accidentally or deliberately)
different bound values still resumes from the same cursor position — that
mirrors DynamoDB's own behavior (parameters are not part of what a real
`NextToken` binds to, from what AWS's public docs describe) and keeps the
token small. A missing/malformed `NextToken` — not valid base64, not valid
JSON, missing a field — is `ValidationException: malformed NextToken`.

### 10. Error surface

Every rejection in this module is `ValidationException` with a message
naming the specific problem (never a bare "invalid statement") —
consistent with every other decode-time rejection in `wire.rs`. No new
DynamoDB exception type is introduced; PartiQL reuses the adapter's
existing `WireError` shape (`impl From<PartiqlError> for WireError`,
mirroring `From<ConditionError>`/`From<UpdateError>` immediately above it
in `wire.rs`).

### 11. Consumed-capacity and throttling classification

Per ADR 0066 Decision 1's classification table: `ExecuteStatement` running
a `SELECT` is `OpClass::Read`; PR 3's `INSERT`/`UPDATE`/`DELETE` are
`OpClass::Write`. `BatchExecuteStatement` (PR 4) and `ExecuteTransaction`
(PR 5) take the class of their own members at the per-table authorization
check — the same "check each of its own tables inside its own handler"
shape `BatchWriteItem`/`TransactWriteItems` already have in
`animusd::authz`, since a batch/transaction's table set (and read-vs-write
mix) is only known after parsing every statement inside it, not at
`classify`'s single-variant dispatch. `classify`'s own top-level entry for
all three `Operation` variants is `OpClass::Read` for `ExecuteStatement`
(PR 2's only supported statement shape) — revisited in PR 3 once a
mutation shape exists to classify (see that PR's own amendment to this
ADR, which is also where a non-`SELECT` `Operation::ExecuteStatement` value
first becomes constructible; PR 2 has no such value to classify, since
non-`SELECT` statement text is rejected at parse time before an
`Operation` is even built).

`Query`/`Scan` do not currently compute a `ConsumedCapacity` at all — a
pre-existing, independent gap this ADR does not attempt to close.
`ExecuteStatement`'s `ConsumedCapacity` therefore mirrors that same gap
(omitted from the response) rather than inventing a capacity computation
`Query`/`Scan` themselves don't have; `ReturnConsumedCapacity` is decoded
and accepted (never rejected) but currently has nothing to report either
way. Revisit together with `Query`/`Scan`'s own capacity gap, not as a
PartiQL-specific fix.

## What each PR delivers

1. **This ADR.**
2. **`ExecuteStatement`, `SELECT` only** — the parser/lexer/AST for
   `select_stmt`, `lower_select`, the wire decode/dispatch/authz/website
   updates, unit + end-to-end tests.
3. **`INSERT`/`UPDATE`/`DELETE`** inside `ExecuteStatement` — the
   remaining three statement shapes, `OpClass::Write` classification for
   them, tests.
4. **`BatchExecuteStatement`** — up to 25 statements (AWS's own cap; this
   adapter enforces the same), each independently lowered and run, no
   cross-statement atomicity, per-statement success/failure reporting
   mirroring `BatchWriteItem`'s `UnprocessedItems` shape.
5. **`ExecuteTransaction`** — up to 25 statements (AWS's own transaction
   cap, matching `TransactWriteItems`' existing limit already enforced in
   this adapter), lowered into one `TransactWriteItems`/`TransactGetItems`
   call, reusing ADR 0018's existing 2PC machinery unchanged.

## Consequences

- A new, fully self-contained module (`animus-dynamo::partiql`) with no
  behavior change to any existing operation — `Query`/`Scan`/`PutItem`/
  `UpdateItem`/`DeleteItem` decode and execute exactly as before; PartiQL
  is a new client-facing surface lowered onto them, never a modification of
  them.
- One new `Operation` variant (`ExecuteStatement`) to classify at every
  exhaustive match site the root `CLAUDE.md` lesson already calls out —
  `Operation::table()` (returns `None`; the table is only known after
  parsing the statement, so it joins `BatchGetItem`/`BatchWriteItem`'s
  existing "resolved inside its own handler" group), `authz::classify`,
  `authz::authorize_op` (also joins the multi-table/table-late no-op
  group, checked once the statement is parsed and its table known,
  before any read/write runs), and `wire::decode_request`'s dispatch.
- `website/compatibility.html` gains an `ExecuteStatement` row (Supported,
  `SELECT` only, from PR 2 onward) once PR 2 lands; PR 3–5 extend its own
  caveat text as each statement shape/batch/transaction form lands.

## Testing

Unit tests in `partiql.rs` itself: lexer (every token class, quoted vs.
bare identifiers, `?` counting), parser (every grammar production in §1,
every rejection in §7), every row of the lowering table, every
`ValidationException` case named in this ADR (literal-value rejection,
placeholder count mismatch, `ORDER BY` on a non-sort-key or on a `Scan`
lowering, two partition-key-equality terms, malformed/mismatched
`NextToken`). End-to-end coverage lives in a new
`crates/animusd/tests/dynamo_partiql.rs`, over the real wire, following
this crate's existing `ProdEnv` cluster fixture + poll-not-one-shot
conventions (root `CLAUDE.md` Testing section).

## Alternatives considered

- **Reuse W-01's string-expression tokenizer wholesale.** Rejected — see
  §3; the two grammars don't share enough shape for it to be a real
  reduction in code, and the placeholder-resolution model differs
  structurally (string-keyed vs. positional).
- **Allow inline literals, matching real AWS PartiQL exactly.** Rejected —
  see §2; the injection-discipline and ADR-0063-number-encoding arguments
  both outweigh exact syntactic compatibility with AWS's own grammar,
  which nothing in this adapter has ever promised byte-for-byte (every
  other operation's own subset already narrows AWS's grammar in various
  documented ways).
- **A general boolean `WHERE` grammar (`OR`/`NOT`/parens) from the start.**
  Rejected for this wave — see §7; every excluded shape still has a
  correct, if not optimal, path once support does land (nothing this ADR
  pins would need to change to add it later, since the key-vs-filter rule
  in §4 already treats "any term not consumed as a key predicate" as a
  filter, and a richer boolean tree is just a richer filter to fold in).
- **Route the parsed WHERE back through `wire.rs`'s string decoders**
  (`decode_predicate`/`decode_sort_condition`) by re-serializing to
  `KeyConditionExpression`/`FilterExpression` text plus a synthetic
  `ExpressionAttributeNames`/`Values` map. Rejected — see §5; doubles the
  parsing work for no shared code, and reintroduces exactly the
  attribute-name quoting/escaping surface PartiQL's own parser already
  resolved once.

## 2026-09-07 amendment — as-built, PR 2

PR 2 landed as designed, with two small clarifications the grammar/design
sections above stated as intent but are worth pinning as fact now that
they're implemented and tested:

- **The leading-keyword check runs on raw text, ahead of the full lex.**
  `parse_select_statement` peeks the statement's first bare word (skipping
  leading whitespace) to decide `SELECT` vs. `INSERT`/`UPDATE`/`DELETE`
  vs. garbage **before** tokenizing the rest of the statement — not after,
  as an earlier draft of the implementation did. The reason is mechanical,
  not aesthetic: `INSERT`'s own PR 3 grammar (§1's `document`) uses `{`/`}`
  bytes this PR's lexer has no token for at all, so lexing a real `INSERT`
  statement whole (to inspect token 0 and reject it) would itself fail
  with an unrelated "unexpected character `{`" error instead of the
  intended "not supported yet" message. Caught by
  `rejects_insert_update_delete_with_named_error`'s unit test on first
  run.
- **`lower_select`'s `partition_key`/`sort_key` parameters are exactly the
  target's own attribute *names*, as plain `&str`** — no `IndexDef`/
  `TableSchema` type crosses into `animus-dynamo::partiql` at all (the
  crate does depend on `animus-control` already, for unrelated reasons,
  but this module deliberately doesn't use it — keeping the "resolved by
  the caller, this module reads no schema" boundary a type-level fact, not
  just a documented convention). `animusd::dynamo::execute_statement`
  resolves those names once — from `TableSchema` for a base-table
  `SELECT`, from the named index's `IndexDef.hash_attribute`/
  `sort_attribute` (with the identical `NoSuchIndex` rejection
  `run_index_query`/`run_index_scan` already give) for an indexed one —
  before calling `lower_select`.

Gate results (2026-09-07): `fmt`/`clippy -D warnings`/`build --workspace
--all-targets`/`test -p animus-dynamo -p animus-item`/`test -p animusd
--test dynamo_partiql` (×3)/`test --workspace`/`cargo deny check` all
green; no new crate dependency added (the roadmap's own "hand-write it, no
parser crate" instruction held). See the PR 2 commit for the exact gate
log and the `dynamo_partiql.rs` end-to-end coverage list.

## 2026-09-07 amendment — as-built, PR 3

PR 3 landed `INSERT`/`UPDATE`/`DELETE`. Grammar, lowering, and error mapping
as built — plus three deliberate departures from what PR 1 (above) pinned
for this PR, each a conservative call made and recorded here rather than
silently diverging from the original text.

### Grammar as built

```
insert_stmt    := "INSERT" "INTO" ident "VALUE" document
                   [ "ON" "CONFLICT" "DO" "NOTHING" ]
document       := "{" [ pair ("," pair)* ] "}"
pair           := string ":" value

update_stmt    := "UPDATE" ident "SET" assignment ("," assignment)*
                   where_clause [ returning_clause ]
assignment     := ident "=" value

delete_stmt    := "DELETE" "FROM" ident where_clause [ returning_clause ]

returning_clause := "RETURNING" "ALL" ( "OLD" | "NEW" ) "*"

value          := "?" | document | list
list           := "[" [ value ("," value)* ] "]"
```

`where_clause`/`predicate` are unchanged from §1 (shared verbatim with
`select_stmt`). No `REMOVE` clause on `UPDATE` — §1's own `update_stmt`
production (pinned in PR 1, unchanged since) never had one; this PR
implements exactly what was pinned, not the wider `UpdateExpression`
subset (`REMOVE`/`ADD`/`DELETE` actions) real DynamoDB PartiQL's `UPDATE`
supports. `UPDATE`'s `assignment` targets a bare top-level attribute only
(no nested path), matching §1 and §7's "no nested document paths outside
`INSERT`'s own document" rule.

### Departure 1 — `value` is recursive (document/list *structure*), not the
literal `"?"` §1 pinned

§1's original grammar (PR 1) pinned `value := "?"` — a document pair's
right-hand side could only ever be a bare placeholder, with any nested
structure arriving exclusively through what that one placeholder's own
`Parameters` entry decodes to (an `AttributeValue::M`/`L` can already be
arbitrarily nested — `decode_attribute_value` doesn't care how many levels
deep it's called from). Built instead: `value` is recursive —
`"?" | document | list` — so `INSERT INTO t VALUE {'pk': ?, 'tags': [?, ?]}`
can be written directly, with the `{..}`/`[..]` *structure* in the
statement text and every *leaf* still a `?`. The placeholder-only
discipline (§2) is unchanged in substance: no scalar literal (a quoted
string, a bare number, `true`/`false`/`NULL`) is ever legal in a value
position, checked at exactly the same point a top-level `?` was — only the
*shape* around the placeholders can now be written in the statement. This
is a usability call: a client with a nested item shape would otherwise
have to pre-flatten every `M`/`L` value into a single `Parameters` entry
computed client-side before sending the request, which defeats a large
part of PartiQL's own appeal (writing the item shape in the statement,
the way a real DynamoDB PartiQL client does) for no injection-safety
benefit the stricter grammar didn't already have without it. Every leaf is
still exactly one `Parameters` slot resolved through the identical
positional binding §2 already established; `lower_insert`/`lower_update`
resolve the whole tree recursively (`lower_value_ast`) rather than
re-deriving a second decode path.

### Departure 2 — `RETURNING` is implemented, not deferred

§7 (PR 1) named `RETURNING` "deferred past PR 3 entirely (not attempted
this wave)". Built instead: `RETURNING ALL OLD *` (`UPDATE`/`DELETE`) and
`RETURNING ALL NEW *` (`UPDATE` only — `DELETE` has no new image, rejected
at parse time with a named error) are implemented in this PR. The reason
for shipping it now rather than honoring the original deferral: it costs
essentially nothing beyond what this PR already has to build. `PutItem`/
`UpdateItem`/`DeleteItem` already carry `ReturnValues`/`UpdateReturnValues`
fields and already know how to build the `Attributes` response an echo
needs (`wire::write_response`/`update_response`) — mapping a parsed
`RETURNING` clause onto those pre-existing fields is a few lines of
lowering, not a new mechanism, and the PR 3 lowering table this ADR
already commits to (`PutItem`/`UpdateItem`/`DeleteItem`, all three of
which already have these fields) makes the wiring close to free. Deferring
it would have meant either shipping `INSERT`/`UPDATE`/`DELETE` with no way
to see what a `RETURNING`-shaped real DynamoDB PartiQL client actually
sends (a client SDK that always includes the clause would get a parse
error on every single mutation), or silently ignoring the clause (worse —
a client asking for the deleted item's image and silently getting nothing
back is a correctness surprise, not a scope cut). `execute_statement`'s
`ExecuteStatement` response reshapes the underlying op's `Attributes`
field into `Items` (`[]` when absent, a one-element array when present) —
mirroring a `SELECT` `ExecuteStatement`'s own always-present `Items` field,
never omitted either way.

### Departure 3 — none on the WHERE/key-vs-filter or error-mapping fronts

Everything else in §5's lowering table, and §4's shared key-vs-filter
machinery (reused here as `lower_exact_key_where`, the mutation-specific
sibling requiring an *exact* key rather than `lower_select`'s
partial-key-or-scan rule), landed exactly as pinned — no further
departures to record.

### Error mapping (as built)

| Failure | `WireError` code | Source |
|---|---|---|
| `INSERT` at an existing key, no `ON CONFLICT DO NOTHING` | `DuplicateItemException` (new constructor, `wire.rs`) | `execute_statement` maps the underlying `PutItem`'s `ConditionalCheckFailedException` |
| `INSERT` at an existing key, `ON CONFLICT DO NOTHING` given | — (silent success, empty `Items`) | same underlying failure, swallowed by `execute_statement` |
| `UPDATE`/`DELETE` — literal value in `WHERE`/document/assignment | `ValidationException` | `partiql.rs` parse time (§2) |
| `UPDATE`/`DELETE` — placeholder count mismatch | `ValidationException` | `lower_insert`/`lower_update`/`lower_delete` |
| `UPDATE`/`DELETE` — `WHERE` missing the partition key, or a composite table's sort key | `ValidationException` | `lower_exact_key_where` |
| `UPDATE`/`DELETE` — `WHERE` names a key attribute with `=` more than once | `ValidationException` | `lower_exact_key_where` |
| `DELETE ... RETURNING ALL NEW *` | `ValidationException` | parse time (`Parser::parse_delete`) — no new image on a delete |
| `UPDATE` of a key that doesn't exist | `ConditionalCheckFailedException` | the underlying `UpdateItem`'s implicit `attribute_exists(pk)` |
| `UPDATE`/`DELETE` — a non-key `WHERE` term (the lowered `ConditionExpression`) evaluates false | `ConditionalCheckFailedException` | the underlying `UpdateItem`/`DeleteItem` |
| `DELETE` of a key that doesn't exist, no other `WHERE` term | — (silent success, empty `Items`) | plain `DeleteItem` semantics — no implicit existence condition |
| any mutation against a throttled table | `ProvisionedThroughputExceededException` | inherited unmodified from the underlying `PutItem`/`UpdateItem`/`DeleteItem` write path (ADR 0065) |

### Classification (`authz`)

`authz::classify`'s `Operation::ExecuteStatement` row cannot see inside
`statement` — it is opaque, unparsed text at that layer, and `classify`
takes only the `Operation` value, never a parser. It stays `OpClass::Read`
unconditionally for **every** statement kind (verified directly:
`every_operation_classifies_per_adr_0066_decision_1` now has four
`ExecuteStatement` cases — one per statement kind — all asserting the
identical `OpClass::Read`, including the three mutation shapes). This is
not a security gap: `authorize_op` is a deliberate no-op for
`ExecuteStatement` (unchanged since PR 2, for the identical "table unknown
before parse" reason), and the real per-statement enforcement happens
inside `execute_statement` itself once `statement` is parsed — a `SELECT`
gets an explicit `authz::authorize(.., OpClass::Read, ..)` call (PR 2,
unchanged); an `INSERT`/`UPDATE`/`DELETE` lowers onto a genuine
`Operation::PutItem`/`UpdateItem`/`DeleteItem` and runs through
`run_operation`, whose own `authorize_op` call classifies **that**
concrete, now-parsed operation — `OpClass::Write`, the same row every
client-built `PutItem`/`UpdateItem`/`DeleteItem` request already uses —
before any write executes. No `authz.rs` code needed to change to make
this correct; only its `ExecuteStatement` row's doc comment and the
classification test's coverage were extended, to make the "why" explicit
rather than implicit.

### Write-path reuse: real `run_operation`, not a parallel path

`execute_statement`'s `INSERT`/`UPDATE`/`DELETE` branches call
`run_operation(ctx, principal, op)` directly with the lowered
`Operation::PutItem`/`UpdateItem`/`DeleteItem` — the exact dispatcher a
client-built request of that shape already goes through — rather than
reimplementing any slice of the write path. This means conditions, LSI/GSI
index maintenance, DynamoDB Streams change records, and per-table
throttling are inherited with zero new write-path code; a bug fix to
`PutItem`/`UpdateItem`/`DeleteItem` fixes the PartiQL path for free, and
there is no second place index/stream/throttle logic can drift.
`run_operation` is `async` and calls `execute_statement` for its own
`ExecuteStatement` arm, so the reverse call from `execute_statement`'s
mutation branches back into `run_operation` is a genuine mutual
recursion — resolved with `Box::pin(run_operation(..)).await` at the one
recursive call site (the standard Rust technique for breaking a
directly/mutually recursive `async fn`'s otherwise-infinite generated
`Future` type; `run_operation`'s own call into `execute_statement`, one
level up, stays a plain unboxed `.await` — only one edge of the cycle
needs boxing to make the type finite).

### Testing

Unit tests in `partiql.rs`: every new grammar production (`INSERT`/
`UPDATE`/`DELETE`, `ON CONFLICT DO NOTHING`, nested document/list values,
`RETURNING` both modes on `UPDATE`, `RETURNING ALL OLD *` on `DELETE`),
every new rejection (literal in a document/assignment value, a
non-quoted-string document key, `RETURNING ALL NEW *` on `DELETE`, an
unknown `RETURNING` mode, a missing `*`), and every lowering row (`INSERT`
simple/composite-key condition, nested document/list values, `UPDATE`
simple/composite key, a non-key `WHERE` term folded into the condition,
both `RETURNING` modes, `DELETE` with no implicit condition, a non-key
`WHERE` term as `DELETE`'s own condition, `RETURNING ALL OLD *`, the
missing-partition-key/missing-sort-key/two-equalities/placeholder-mismatch
errors for both `UPDATE` and `DELETE`). End-to-end in
`crates/animusd/tests/dynamo_partiql.rs` (25 tests total, up from 13):
`INSERT` then `SELECT` sees it; a duplicate `INSERT` gives
`DuplicateItemException` and leaves the original item unchanged; `ON
CONFLICT DO NOTHING` swallows a duplicate silently; `UPDATE ... RETURNING
ALL NEW *` on an existing item; `UPDATE` of a missing item fails; a
non-key `WHERE` term as `UPDATE`'s condition, both met and unmet; `DELETE
... RETURNING ALL OLD *` (and the item is actually gone afterward);
`DELETE` of a missing key is a silent success; a GSI-projected attribute
(`cat`) updated via PartiQL `UPDATE` is visible through a `SELECT` on the
index (index maintenance inherited, not reimplemented); a
`WHERE`-missing-partition-key `UPDATE` and a `WHERE`-missing-sort-key
`DELETE` are both `ValidationException`; `RETURNING ALL NEW *` on `DELETE`
is rejected; and a `PROVISIONED`-billing table throttles a PartiQL
`INSERT` exactly like a client-built `PutItem` would (throttling
inherited). A stream-enabled table recording a PartiQL write, and
`BatchExecuteStatement`'s own per-statement outcome shape, are left to
PR 4 (batch) rather than duplicated here — the throttling and index-
maintenance tests above already establish that PR 3's writes ride the
identical leader-evaluated funnel every other write path already has
Streams/throttle coverage for elsewhere in this crate's own test suite,
so a third redundant proof here was judged not worth its own fixture cost
within this PR's scope.

Gate results (2026-09-07, PR 3): `fmt`/`clippy -D warnings`/`build
--workspace --all-targets`/`test -p animus-dynamo -p animus-item`/`test -p
animusd --test dynamo_partiql` (×3, 25/25 each run) all green; no new crate
dependency added. Given this sandbox's own limited disk allowance
(~11 GB), the remaining per-crate/per-binary gates ran individually rather
than as one `cargo test --workspace` sweep — see the PR 3 commit message
for the exact per-crate log.
