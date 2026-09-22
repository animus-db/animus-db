# Two closed-set allowlists guarding the same wire hop drift apart — derive both from one shared table

**When a value must survive the same kind of hop twice (once per code
path), don't give each path its own copy of the closed set that decides
what survives — a maintainer who fixes one copy has no structural reason
to remember the other.**

## The incident

`animusd`'s forwarded write path has two `KindWrite*` hops that carry a
leader-minted `WireError`'s own `code` back to a non-leader-connected
client: the singular `KindWriteItem` hop (`dynamo.rs::encode_relayed_error`/
`decode_relayed_error`, a whole-request `wire-error:<code>:<message>`
marker string in `ClientResponse::Error`) and the batched `KindWriteBatch`
hop (`kind_write_outcome_to_reply`/`wire_error_from_batch_rejected`, a
per-item typed reply slot). Both exist for the identical reason — an
unmarked or unrecognized code degrades to a bare 500 `InternalServerError`,
so a typed refusal minted at a *remote* leader must come back with its own
code — and both were built the same way: a closed-set `match code { ..
known codes .. => .., _ => internal(..) }`, one per hop, written by
different changes.

The two lists then drifted in both directions:

- `"ServiceUnavailable"` went into `decode_relayed_error` with issue #994
  (PR #1017), and had to be added to `wire_error_from_batch_rejected`
  separately afterwards when issue #996's regression task found the batch
  hop lacked it (see `docs/lessons/orchestration/2026-09-21-a-sibling-loop-
  copied-before-a-fix-lands-on-the-original-needs-the-fix-ported-at-merge-
  time.md`).
- `"ProvisionedThroughputExceededException"` was in
  `wire_error_from_batch_rejected` from the day the batch hop shipped (its
  per-item throttle shedding needed it immediately) and was never ported
  back to `decode_relayed_error`. The singular hop's own CLAUDE.md entry
  even documented the gap ("deliberately NOT added there ... a
  pre-existing, separate defect ... out of this fix's own scope") — a
  correctly labeled TODO that sat unfixed until issue #1035 filed it as a
  bug: a throttle refusal minted at a remote leader reached a
  non-leader-connected client as a 500, while the identical request against
  the leader's own node correctly returned 400, so the same request got two
  different error contracts depending on which node served it.

## Why the gates didn't catch it

Every existing throttle-forwarding regression test — the real-socket
`dynamo_throttling.rs` scenarios and their later `SimCluster` conversion,
`sim_cluster_dynamo_throttle.rs::a_forwarded_write_is_throttled_on_the_
leader` — used a **plain, unconditioned** `PutItem`. On a plain table with
no GSI/LSI/stream, that shape takes `dynamo.rs::fast_marker_write`
(`dispatch_item_op`'s fast-arm gate: no `ConditionExpression`,
`ReturnValues::None`, no images-carrying table), whose own forwarded error
channel (`map_throttleable_error`, a bare unmarked string) never touches
`decode_relayed_error`'s allowlist at all. So the bug's own hop was never
exercised by any test that already existed — not because the tests were
wrong, but because the single most common write shape (an unconditioned
`Put`) structurally cannot reach the evaluated-at-leader path the bug
lives on. Only a `PutItem`/`UpdateItem`/`DeleteItem` carrying a
`ConditionExpression` (or a non-`NONE` `ReturnValues`, or an indexed
table) is evaluated at `kind_write_item_at_leader`, and nothing forced a
throttle-forwarding test to use that shape until this fix's own
regressions did.

## The fix

Replaced both independent `match` blocks with one shared function,
`dynamo.rs::relayable_wire_error`, holding a single `RELAYABLE_WIRE_ERROR_
CODES` table — the union of every code either hop's own serve arm can
mint. `decode_relayed_error` and `wire_error_from_batch_rejected` both
call it now; neither keeps its own list. An inert extra entry (a code
neither hop currently mints) costs nothing; a missing one is exactly the
silent, placement-dependent 500 this issue was. A `both_forwarded_hops_
share_one_allowlist` unit test iterates the shared table and asserts both
hops round-trip every code in it (and both degrade an unknown code to
`InternalServerError` identically), so a future addition to one hop's
serve arm that needs a new code is caught by this test the moment it's
added to the table but not to whatever mints it — or vice versa.

## The general rule

When two different code paths must each recognize the same closed set of
values crossing the same *kind* of hop (a marker string, a typed reply
variant, a serialized enum discriminant) — even if the two paths were
built at different times, by different changes, and structurally cannot
share a type — **give them one shared table/function to both call,
not two independently-maintained copies of the same match arms.** The
sibling-loop lesson above (`2026-09-21-...`) already covers "a fix to one
copy must be ported to the structural twin at merge time"; this is the
same failure shape one level up — the fix here is to remove the second
copy entirely rather than remember to keep two copies in sync forever.
Two structural checks are cheap to add once you've found one such pair:

1. **Grep for the other allowlist before declaring the fix done.** A
   closed-set `match … => Some(literal) … _ => None` (or the equivalent
   `unwrap_or_else(|| internal(..))` fallback) guarding a wire hop is easy
   to find with one `grep -n "=> internal\|=> WireError::"` pass over the
   file that owns the first one — the same file, in this case, since both
   allowlists lived in `dynamo.rs` all along.
2. **Once found, merge into one function with a test that iterates its
   own exposed constant against every consumer**, rather than trusting a
   code comment ("mirroring `decode_relayed_error`'s identical allowlist
   entry", which `wire_error_from_batch_rejected`'s own doc comment
   already said, verbatim, right next to the code that then drifted
   anyway) to keep two lists in sync by convention.

**Sub-lesson — test the path the bug is on, not the operation name.** Every
pre-existing test for "a forwarded write is throttled" used the most
natural-looking `PutItem`, and every one of them happened to take the one
code path unaffected by this bug. When a defect is scoped to a specific
*internal* routing branch (here: evaluated-at-leader vs. the fast marker
arm) rather than to an operation as a whole, a regression test must name
and force that exact branch (a `ConditionExpression`, here) — matching the
operation's name or its common-case shape is not enough, and it is worth
grepping the dispatcher's own branch condition (`dispatch_item_op`'s fast-
arm gate, in this case) to confirm the test's request body actually lands
on the intended side before trusting a green result.
