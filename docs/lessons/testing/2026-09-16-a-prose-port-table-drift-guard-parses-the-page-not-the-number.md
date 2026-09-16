# A prose port-table drift guard should parse the page's own markup against the real generator, not hardcode the numbers it expects (issue #850)

`website/architecture.html`/`install.html`/`how-it-works.html` documented a
seven-port-per-node layout with a fictitious "reserved" `+3` slot, three
pages independently copied from one another; the real code
(`ClusterConfig::generate`, `crates/animusd/src/config.rs`) has assigned six
consecutive ports since ADR 0053 dropped CQL. Every listener from `admin`
onward was documented one offset higher than what actually binds, and the
`animusd join --seed` walkthrough pointed at the console port instead of the
intra port it claimed to be. Nothing tied the English prose to the Rust
struct, so the drift was invisible to every gate until a human read both
side by side.

**The guard this issue added
(`crates/animusd/tests/website_ports.rs`) parses the two pages' own `<td>+N</td>`
table markup with a tiny hand-rolled scanner — no `regex` dependency added
just for this — and compares the extracted `(offset, port)` pairs against
`ClusterConfig::generate(3, host, 7100)`'s real node-0 addresses in the same
`internal, client, dynamo, admin, intra, console` order the code assigns
them.** That is the generalizable shape: a documented table of values
derived from a struct/function should be asserted *by parsing the
documentation's own markup*, not by hardcoding "expect 7103" in the test —
hardcoding the expected numbers a second time just relocates the copy/paste
risk into the test file itself, where a future port stride change would
need the same manual edit the doc page needs and could silently go stale
identically. Deriving `expected` from a live call to the real generator
means a future stride change (an eighth port, a reordered role) breaks this
test the moment `generate` changes, forcing the doc update in the same PR
instead of a fourth drifted page discovered later.

**Validate a drift guard by proving it actually guards**, not just that it
passes on the already-fixed page: `git show origin/main:<path>` the
pre-fix content into place (backing up the fixed version first), rerun the
built test binary directly (no need to rebuild — a `cargo test --no-run`
binary can be invoked standalone against swapped-in fixture content), watch
it fail with the drifted numbers, then restore the fix and confirm green
again. A test that was never run red is a test whose failure mode is
unverified.
