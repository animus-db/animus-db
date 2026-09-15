# Removing a stride field is the mirror of adding one (above) and needs the identical compiler-driven discipline — but a regex-based mass fixup script for the ~50 call sites carries a distinct, silent-corruption risk the compiler does NOT catch for free: a field-name-keyed regex (`^(admin|intra|console):`) matches the first colon of an unrelated `admin::SomeType { .. }` module path exactly as readily as a `RoleAddrs { admin: p(3), .. }` struct-literal field

**Removing a stride field is the mirror of adding one (above) and needs the
identical compiler-driven discipline — but a regex-based mass fixup script
for the ~50 call sites carries a distinct, silent-corruption risk the
compiler does NOT catch for free: a field-name-keyed regex
(`^(admin|intra|console):`) matches the first colon of an unrelated
`admin::SomeType { .. }` module path exactly as readily as a
`RoleAddrs { admin: p(3), .. }` struct-literal field** (2026-08-22, ADR
0053's CQL-port removal, `RoleAddrs::cql` deleted, the 7→6-port stride).
The script's per-line regex couldn't distinguish "a field named `admin`
inside a `RoleAddrs` literal" from "the start of the path `admin::CpTxnView`
used as an ordinary expression" — both are `admin:` followed by more text
at the start of a line — so it rewrote several `console::TableSummary { .. }`/
`admin::CpRaftView { .. }`/`dynamo::txn_resolve_awaited(..)` call sites into
`console: :TableSummary { .. }` (space-then-colon), a token sequence that
parses as "the struct field `console`, with its value starting with an
unexpected bare `:`". This *did* immediately fail `cargo build`, with a
parse error at the corrupted line — so the mistake was never silent in the
sense of shipping unnoticed — but the error site is generic enough
(`expected one of \`!\`, \`.\`, \`::\`, ...\`, found \`:\``) that it does not
self-explain "your last mass-edit script mangled unrelated `Type::path`
expressions"; diagnosing it means recognizing the shape, not just reading
the compiler's own words. **General rule: a scripted mass edit keyed on
`NAME:` (a struct-field shape) must exclude or specifically detect
`NAME::` (a path shape) — grep the touched files for `\bNAME: :` (or any
of your field names) as a mechanical post-check before trusting a clean
build, and always run the full build (not just the files you *meant* to
touch) immediately after any regex-driven multi-file edit, since the
script's blast radius is whatever text matches its pattern, not whatever
you intended it to match.**
