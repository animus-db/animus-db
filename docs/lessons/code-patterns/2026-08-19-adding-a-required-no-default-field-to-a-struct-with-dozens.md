# Adding a required (no-default) field to a struct with dozens of literal construction sites: let the compiler enumerate them, don't grep-and-hope (2026-08-19, ADR 0052's `RoleAddrs::console` port).

**Adding a required (no-default) field to a struct with dozens of literal
construction sites: let the compiler enumerate them, don't grep-and-hope
(2026-08-19, ADR 0052's `RoleAddrs::console` port).** `RoleAddrs` (the
per-node listener-address struct, ADR 0047's `intra` port set the
no-`#[serde(default)]` precedent this field followed) has ~60 literal
`RoleAddrs { .. }` construction sites across `animusd`'s `src/` and
`tests/` — a grep for `RoleAddrs {` finds most of them, but a grep for the
*stride arithmetic* (`6 * i`, `free_addrs(n * 6)`, hardcoded offsets like a
hand-computed `addrs[18]` for node index 3) is exactly the kind of
multi-shape, easy-to-undercount search the root `CLAUDE.md`'s "grep every
gating match site" lesson already warns about — and this field additionally
needed the *stride itself* to change (6 → 7), not just one new field
line, so a per-site fix also had to renumber every sibling offset in the
same literal. The reliable sequencing: add the field to the struct
definition **first** (with no default), then run `cargo build -p animusd
--all-targets` and fix every `error[E0063]: missing field` site the
compiler actually reports — repeating until clean. This is exhaustive by
construction (a missed site is a compile error, not a silent gap) where a
grep pass can only ever be "probably complete." A generic per-site fixup
script (regex over the fixed six-field block shape, deriving the seventh
field's expression from the sixth's) handled ~30 of the ~32 remaining test
files in one pass; the two genuine outliers — a hand-computed hardcoded
multi-node offset block, and the struct's own `generate`/`generate_split`
functions building the stride formula directly — still needed a human
read, which the compiler-driven approach surfaced as compile errors as
reliably as everything else, rather than as something a grep could have
silently missed entirely.
