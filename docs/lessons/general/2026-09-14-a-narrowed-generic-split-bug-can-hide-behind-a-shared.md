# A "narrowed generic split" bug can hide behind a shared prelude too, not just a shared dispatcher (issue #842)

The five PartiQL entry points in `animusd::dynamo` (`execute_statement`,
`execute_transaction`, `execute_one_batch_statement`, and their two
`<E, R>`-generic siblings) each ran `table_known` (existence) *before*
`authz::authorize`/`authorize_op`/`authorize_each_table` (authorization) —
the exact reverse of `run_operation`'s own prelude order (`authorize_op`
always runs before dispatch) and of `run_transact`/`run_transact_get`'s own
authz-before-`resolve_key` order. A `Principal::Scoped` (ADR 0066)
principal denied access to a table therefore got a *different* error for a
table that exists (`AccessDeniedException`, once the real handler's own
authz check ran) than for one that never existed (`ResourceNotFoundException`,
from the pre-authz existence short-circuit) — a table-enumeration oracle
none of the native single-op paths have, since those always authorize
first. The fix is a small, mechanical reorder at all five sites: parse →
resolve the table name(s) → authorize (using `OpClass::Read`/`Write`
resolved from the statement's own kind, needing no lowered `Operation` at
all — `Policy::allows` only ever consults `OpClass`/table, never the
operation's own name) → `table_known`. **The general form**: a
"do X once, up front, before dispatching to per-kind handlers" prelude
(existence checks, internal-table rejection, authorization) must be
audited for *order* whenever a new prelude step is added, not just for
presence — the existing root `CLAUDE.md` lesson about a missed relay
allowlist entry is one instance of "a new variant needs the same gate every
sibling variant has"; this is the same category one level up: a new
top-level entry point (PartiQL) reimplementing a shared prelude by hand,
inline, rather than delegating to the one place that already gets the
order right, is exactly how the two preludes drift out of sync with each
other. Grep every site that computes `table_known`/an existence check
ahead of an `authz::`/`Policy::allows` call whenever adding a new
authenticated entry point, and compare its order against `run_operation`'s
own canonical prelude, not just against its nearest sibling (which can
carry the identical bug).
