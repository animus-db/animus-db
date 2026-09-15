# Enforcing a decode-time invariant surfaces every JSON-body-building call site, not just the ones under `crates/*/tests` (roadmap W-11)

Landing the rejection W-05's own lesson above deliberately deferred —
`CreateTable`/`UpdateTable` must reject a key attribute (base or index)
with no `AttributeDefinitions` entry, and reject an entry that names no
key attribute at all — found that "the fixtures" is a wider set than the
obvious `crates/animusd/tests/*.rs` grep suggests. Two call sites outside
that tree built genuinely incomplete bodies and would have started
`400`-ing in production the moment the strict decoder shipped:

- **`animusd::lib.rs`'s `ConsoleBackend::add_gsi`/`create_table`** (the
  console's own Add-GSI and create-table forms) send an
  `AttributeDefinitions` entry only for the base table's partition/sort key
  — an index-only key attribute's type was left **deliberately absent**
  pre-W-11 (`console::CreateTableRequest`'s own doc explains why: neither
  `CreateGsiRequest`/`CreateLsiRequest` nor the create-table form collects
  one). That was a legitimate design choice against the *lenient* decoder;
  against the *strict* one it is simply broken — every console-added index
  needs a `"S"`-defaulted `AttributeDefinitions` entry now, so the type
  these two request builders record for an index key attribute changes
  from `None` to `Some("S")`, a real, observable behavior change (fixed in
  `console_table_config.rs::add_and_drop_gsi_round_trip` and
  `console_create_table.rs::create_full_table_declares_everything_exactly`,
  whose whole point had been to pin the *old*, `None`-recording behavior).
- **`animusd::src::dashboard_browser.js`'s `submitAddIndexForm`** (the
  *operator* dashboard's own "Add index" form — a different surface from
  the console app above, ADR 0052's "Naming, deliberately addressed") sent
  no `AttributeDefinitions` at all for its GSI's key(s). No Rust test
  exercises this path (it POSTs JSON built entirely client-side), so
  nothing in the crate's own test suite would ever have caught the break —
  it was found only by grepping every `KeySchema`/`GlobalSecondaryIndexUpdates`
  occurrence across `crates/animusd/src/*.js` too, not just `*.rs`.

The general form: a decode-time invariant enforced for the first time
doesn't just need every *test* fixture swept — it needs every **caller**
that builds the wire JSON by hand, including a same-crate but
non-`tests/`-tree admin surface and any client-side JS that talks straight
to the wire without a Rust test in between. `grep`ping only `crates/*/tests`
(the obvious first pass, and literally what W-05's own residual note
above named as done) would have shipped two silent regressions.
