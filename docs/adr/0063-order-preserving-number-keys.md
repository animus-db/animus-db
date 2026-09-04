# ADR 0063 — Order-preserving key encoding for `N` (number) key attributes

- **Status:** Accepted
- **Date:** 2026-09-04
- **Amends:** [ADR 0022](0022-hash-ring-partitioning.md), [ADR
  0023](0023-table-scoped-tablets.md) — both mark the data-plane key layout
  a frozen on-disk format, "do not change without a data migration"; this
  ADR is that change, recorded per their own instruction, with no migration
  attempted (see Consequences)
- **Origin:** `docs/roadmap.md`'s W-03 ("Order-preserving encoding for `N`
  sort keys")

## Context

`AttributeValue::key_bytes()` (`crates/animus-dynamo/src/lib.rs:84`) is the
single choke point every data-plane key layout ADR 0022/0023 defined routes
an attribute value through: base sort keys (`storage_key`), GSI/LSI hash and
sort key-path components (`index.rs`), and — via `escape(pk.key_bytes())` at
the `animusd` edge — partition keys feeding `partition_token`. Today it
writes `N` as **raw decimal text**:

```rust
AttributeValue::N(n) => n.clone().into_bytes(),
```

Text bytes do not sort the way DynamoDB numbers order. `"15" < "9"`
lexicographically, and `"-5" > "-10"` (a leading `-` is byte `0x2D`, and the
digit that follows compares as text, so more negative numbers with an equal
or greater number of digits sort *later*, not earlier). This makes every
`N`-keyed **stored key order** wrong across magnitudes and signs.

That bug has two independent faces, and only one of them is fixed today:

- **The in-memory filter is already correct.** `SortKeyCondition::matches`
  compares `N` operands numerically (`compare_numeric`, backing `Between`/
  `Compare`), and `matches_raw` reinterprets a scanned key's raw bytes as
  the condition's declared type before delegating to `matches` — this
  closed issue #373 (`sk BETWEEN 5 AND 15` wrongly excluding `sk = 9`).
  Range/`BETWEEN` filtering on `N` is correct end to end today (base table,
  GSI, LSI), and stays correct after this ADR (see Scope below).
- **Result *order* is not, and nothing has fixed it.** A `Query`'s
  `ScanIndexForward` contract promises the returned page walks the sort key
  in ascending (or, reversed, descending) order. Because the *stored* key is
  still the raw-text encoding, the scan the filter runs over already visits
  rows in byte order, not numeric order — `9` is returned after `15`, not
  before it, in an ascending scan over a partition that mixes digit counts,
  and a negative-vs-positive or differently-signed mix is wrong the same
  way. The filter's correctness (which rows are *included*) says nothing
  about the order those rows arrive in; this is the residual gap
  `crates/animus-dynamo/CLAUDE.md`'s own `key_bytes` entry already names as
  "out of scope [for issue #373] — it needs its own ADR."

This ADR is that ADR. The decision: replace the `N` key encoding with a
canonical, order-preserving, self-delimiting byte layout applied at
`key_bytes()`, so bytewise order of stored keys equals DynamoDB numeric
order — the same guarantee `S` and `B` already have (their `key_bytes` is
already order-preserving: UTF-8 byte order for `S`, raw byte order for `B`,
both matching DynamoDB's own comparator for those types), extended to `N`.

DynamoDB numbers carry up to 38 digits of precision, exponent range
`-130..+125`; the encoding below is sized to that range explicitly rather
than left open-ended.

## Decision

### Canonicalization

Before encoding, a decimal string is reduced to a canonical form:

- Strip the sign, recording it separately (`negative` / `zero` / `positive`
  — three-way, not a boolean, because zero has no meaningful sign and is
  encoded as one distinguished value regardless of how it was written:
  `"0"`, `"-0"`, `"0.00"` all canonicalize identically).
- Strip leading and trailing zeros from the digit run, then re-express the
  value as a **decimal exponent** plus a digit run with no leading or
  trailing zeros (i.e. scientific-notation-shaped: one canonical digit run,
  one canonical exponent, for any input magnitude or decimal-point
  placement). Two decimal strings that denote the same numeric value (e.g.
  `"1.50"` and `"1.5"`, or `"150"` and `"1.5e2"` if exponent notation is
  ever accepted as an `N` literal) canonicalize to the identical digit run
  and exponent — a load-bearing property, since two `N` values DynamoDB
  considers equal must encode to identical bytes.

### Layout

One leading byte classifies the sign:

| Byte | Meaning |
|------|---------|
| `0x02` | negative |
| `0x03` | zero |
| `0x04` | positive |

`0x00` and `0x01` are deliberately left unused by this classification, for
two reasons at once: (1) so an encoded `N` value, when it appears **escaped**
as a mid-key component (`escape()` doubles every `0x00` to `0x00 0x01` and
terminates with `0x00 0x00` — ADR 0023's key-layout doc), can never produce
a byte sequence that collides with `escape`'s own terminator/doubling
vocabulary; (2) so the encoding, when it appears **unescaped** as the
trailing component of a base key (the position `storage_key`'s sort key
occupies today), is never a prefix of another value's encoding — no encoded
`N` value can itself start with `0x00`/`0x01`, so no ambiguity is introduced
at the one position in the key layout where `key_bytes()`'s raw output is
used unescaped.

After the sign byte: the value's decimal **exponent**, as a fixed-width
big-endian offset-binary byte (the true exponent plus a bias, chosen so the
biased value is always non-negative across DynamoDB's documented `-130..
+125` exponent range) — this is what makes bytewise order track magnitude
directly, rather than requiring a decoder to count digits first. Then the
**digit run**, packed one byte per digit, each byte holding `digit + 1`
(range `1..=10`) so no digit byte is ever `0x00`, terminated by a `0x00`
byte.

**Negative numbers invert.** For a negative value, the exponent byte and
every digit byte (but not the leading sign byte, which stays `0x02`) are
bitwise-inverted, and the terminator becomes `0xFF` instead of `0x00`. This
is what makes bytewise order agree with numeric order on the negative side:
among two negative numbers, the one with the **larger magnitude** must sort
**first** (`-100 < -5`), which is the opposite of how their un-inverted
magnitude bytes would compare — inversion flips that comparison for free,
without a separate negative-number code path in the comparator (plain
`memcmp` over the whole encoded value works for every sign combination,
because the three sign-byte values themselves already sort
`negative(0x02) < zero(0x03) < positive(0x04)`, matching numeric order at
the top level).

**Why the terminator is load-bearing, not decorative.** Two digit runs
where one is a proper prefix of the other (e.g. `12` vs `120`, after
canonicalization these differ in exponent too, but the shape recurs
wherever unequal-length digit runs must compare) need a tie-breaker once the
shorter run's bytes are exhausted. For positive numbers, the shorter
(prefix) run must sort **before** the longer one; a `0x00` terminator —
lower than every digit byte (`1..=10`, i.e. `≥1`) — guarantees exactly that
by construction. For negative numbers the inequality reverses (a shorter
inverted-magnitude run must sort **after** the longer one, by the same
larger-magnitude-sorts-first rule above), and the inverted terminator
`0xFF` — higher than every inverted digit byte — guarantees that side too.
This is why the terminator's value is tied to sign rather than fixed: it is
not merely "self-delimiting," it is the mechanism that makes an unequal
digit-run-length comparison land the same direction the equal-length case
already does from the exponent/digit bytes alone.

**Decoding, not reinterpreting.** The encoding is deliberately
decoding-friendly rather than a black box: `matches_raw` (`condition.rs`)
decodes the stored bytes back to a canonical decimal string (undo the sign
class, un-bias the exponent, un-invert and un-offset each digit byte for a
negative value, stop at the terminator) rather than reinterpreting them as
UTF-8 text the way it does for `S`. This is a real, but narrow and
contained, change to that function's own contract — see Testing below for
what has to move with it.

### What this ADR deliberately does not pin down

The byte values above (which sign byte is which, which offsets/biases are
used, exact terminator values) are the *shape* of the encoding, not a
promise that an implementation must reproduce this document's illustrative
byte choices verbatim, beyond the ordering properties they exist to
guarantee. **The differential property test is the source of truth for
correctness**, not this prose: for all pairs `(a, b)` of DynamoDB-valid
decimal strings,

```
encode(a).cmp(&encode(b)) == BigDecimal::from_str(a).cmp(&BigDecimal::from_str(b))
```

and `decode(encode(a))` parses back to a value that compares numerically
equal to `a` (not necessarily byte-identical text — `"1.50"` decoding to
`"1.5"` is fine; DynamoDB itself does not promise textual round-trip). Both
properties are checked over the same `bigdecimal` crate `condition.rs`'s
existing `decimal_differential_tests` already depends on
(`crates/animus-dynamo/Cargo.toml`, already a `[dev-dependencies]` entry —
no new dependency).

### Scope

`key_bytes()` is the only call site this ADR changes, but that one choke
point reaches every consumer that keys off an `N` value:

- **Base sort keys** (`storage_key`) — the motivating case (`ScanIndexForward`
  on an `N` sort key).
- **GSI/LSI hash and sort components** (`index.rs`'s `gsi_row_key`/
  `lsi_row_key`/the various prefix helpers) — an `N` GSI/LSI hash or sort
  attribute gets the same order-preserving layout, both where it is
  `escape()`d mid-key (an `isort`/`alt_sort` component) and where it sits
  unescaped at the tail (a base or LSI row's trailing `sk`/`base_sk`).
- **Partition keys.** `key_bytes()` also feeds `partition_token`'s input
  (`escape(pk.key_bytes())` at the `animusd` edge, per ADR 0022's layout).
  An `N` partition key's **token input changes**, which changes **which
  tablet a row hashes to** — a strictly bigger blast radius than sort-key
  ordering alone, called out explicitly here because it is easy to miss:
  this is not just a re-sort of existing rows within their current tablet,
  it is a re-placement of rows across the table's whole hash ring. This is
  accepted, not mitigated: root `CLAUDE.md`'s "No back-compat until further
  notice" already establishes that there are no migration paths or
  wire/on-disk-format compatibility guarantees between revisions, and that a
  cheap compat measure is never owed as a promise. A table with `N`-typed
  keys created before this change and read after it is expected to be
  re-created, exactly as any other on-disk format break in this codebase
  is handled today.
- **Streams and backup/restore carry these bytes verbatim, and therefore
  follow automatically, with no separate encoding decision of their own.**
  `ChangeRecord.base_sk` (ADR 0042) stores whatever `key_bytes()` produced
  at write time — once `key_bytes()` changes, every new change record's
  `base_sk` is in the new layout with no code change in the streams path
  itself. The same is true of backup/restore's captured objects (ADR
  0059) — a BASE/LSI/FOOTPRINT object is a byte-for-byte capture of stored
  rows, so it inherits the new key layout the moment capture reads
  post-change data, with no encoding logic of its own to update. Because
  neither subsystem has its own `N`-specific encoding to change, the
  regression coverage for this ADR (see Testing) includes explicit
  assertions that each of these consumers reads the *same* bytes the write
  path produced, rather than assuming "no code changed there" implies "no
  test needed there."
- **`animus-tablet`'s `escape`/`partition_token` are unchanged.** The
  roadmap entry that named this gap said the encoding would be "mirrored
  byte-for-byte in `animus-tablet`." On inspection, `animus-tablet` holds
  **no `N`-specific encoding today** — `escape` and `partition_token`
  operate on opaque byte strings and know nothing about DynamoDB's type
  system; the sign/exponent/digit-run scheme above is a
  `crates/animus-dynamo`-only concept, applied to a value **before** it is
  handed to `escape`/`partition_token`, never inside them. There is
  therefore nothing in `animus-tablet` to mirror, and this ADR records
  that finding rather than adding a mirrored implementation that has no
  consumer: an implementing change must re-verify by grep (as this ADR's
  own drafting did — `AttributeValue::N`, `decimal`, `numeric`, `digit`
  found nothing in `crates/animus-tablet/src/lib.rs`) before assuming
  otherwise, since a future refactor could in principle push a numeric
  concept down into that crate and make the "mirror" real.

### What this ADR does not do

`SortKeyCondition` range predicates on `N` (`Between`, `Gt`/`Lt`/etc.) still
scan the whole partition/index sub-range and filter in memory — this ADR
makes that filter's *input order* correct, but does not change the scan
itself to derive tight engine-level bounds from the condition's own operands
now that a numeric comparator's byte range would finally correspond to its
numeric range. That is a real, valuable, and now-*possible* optimization —
before this change, `N`'s stored order had no relationship to its numeric
order, so no byte-range bound could ever be derived from a numeric
predicate at all — but it is an optional follow-up, not part of this
decision. `matches`/`matches_raw` stay the authoritative filter regardless
of whether a future change narrows the scan that feeds them.

## Consequences

- **`ScanIndexForward` becomes correct for `N` sort keys**, matching what
  the filter has already guaranteed for `BETWEEN`/comparator predicates
  since issue #373: a `Query`'s returned page order finally agrees with
  DynamoDB's numeric ordering across magnitudes and signs, closing the gap
  `crates/animus-dynamo/CLAUDE.md`'s `key_bytes` entry names.
- **This is an on-disk/wire format break for every `N`-keyed table**,
  exactly as ADR 0022/0023 warned changing this layout would be. No
  migration is provided or owed (root `CLAUDE.md`); an existing table with
  an `N` partition or sort key (or `N` GSI/LSI key attribute) must be
  re-created after upgrading past this change to read/write correctly —
  its old rows are keyed under the old raw-text layout and will not compare
  correctly against new writes in the new layout within the same table.
- **`N` partition keys re-place rows across the table's ring.** Because the
  token input changes, an `N`-partitioned table's rows redistribute across
  tablets after this change — a strictly larger effect than the sort-key
  case, and one more reason a pre-existing `N`-keyed table cannot simply be
  left in place across the upgrade.
- **`matches_raw` now decodes instead of reinterpreting UTF-8** — a
  behavior change to a function every native scan path
  (`crates/animus-dynamo/CLAUDE.md`: "every production call site" of the
  raw-bytes-off-a-scan shape) already routes through, so its correctness
  is now load-bearing for every `N`-keyed base/GSI/LSI read, not merely a
  documented simplification.
- **A future range-scan tightening for `N` predicates becomes possible**
  (see "What this ADR does not do") but stays unscheduled follow-up, not a
  consequence this ADR claims credit for.
- **`animus-tablet` stays exactly as simple as it is today** — this
  decision is fully contained in `animus-dynamo`'s own `key_bytes` choke
  point; no new coupling to the partitioning primitives is introduced.

## Testing / verification

- **A new differential property test**, next to `condition.rs`'s existing
  `decimal_differential_tests`, checking the two properties stated above
  (`encode(a).cmp(encode(b)) == BigDecimal(a).cmp(BigDecimal(b))` for all
  pairs; `decode(encode(a))` numerically equal to `a`) over the same
  up-to-38-significant-digit generator that suite already uses, extended to
  cover the full `-130..+125` exponent range and the zero/negative/positive
  sign classes explicitly.
- **`matches_raw_reinterprets_bytes_by_the_conditions_own_declared_type`**
  (`condition.rs`, the existing test asserting `matches_raw`'s current
  UTF-8-reinterpretation behavior) is rewritten for the new decode-based
  contract — its name and intent (raw scanned bytes, no type tag, must
  still evaluate correctly against a declared-type condition) survive; its
  mechanism does not.
- **Explicit regression coverage for every consumer named in Scope**,
  proving each reads the *same* bytes the write path produced rather than
  assuming no separate encoding step implies no separate test is needed:
  a base-table `N` sort-key `ScanIndexForward` ordering test across mixed
  magnitudes and signs; the equivalent for a GSI and for an LSI; a streams
  regression asserting a `ChangeRecord.base_sk` captured after this change
  decodes to the same value the write that produced it used; and a
  backup/restore regression asserting a captured object's key bytes, once
  restored, read back correctly through the new decode path.

## Alternatives considered

**Leave `key_bytes` unchanged; derive an order-preserving encoding only at
the scan-range-computation site, keeping stored keys as raw text.**
Rejected: the stored key order is what defines the engine's own scan order,
and no range-computation trick at the read edge can make an
already-byte-ordered-wrong keyspace return rows in a different order without
either a full in-memory sort (defeating the point of a range scan) or
reading the whole partition and re-ordering — exactly the "scan the
partition and filter" cost the roadmap gap explicitly calls out as the thing
a tight scan bound (the noted future optimization) is meant to avoid, not
reintroduce as the permanent shape.

**Fix ordering by changing only `ScanIndexForward`'s read path to sort the
already-fetched page in memory, without touching `key_bytes`.** Rejected:
this "fixes" only the case where a whole partition's worth of matching rows
fits in one page: an `N` sort key with a `Limit` that pages across a
boundary would return each page correctly sorted internally but the overall
sequence across pages still would not be a global numeric order, since the
*scan* itself still visits rows in byte order. An order-preserving key is
the only fix that makes the on-disk order and the numeric order the same
thing, which is what makes windowed pagination correct by construction
rather than by a per-call patch.
