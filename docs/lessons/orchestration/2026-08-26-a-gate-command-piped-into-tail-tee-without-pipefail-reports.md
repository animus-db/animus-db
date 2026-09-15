# A gate command piped into `tail`/`tee` without `pipefail` reports the pipe's *last* command's exit status, so the gate can fail while the wrapper exits 0 (2026-08-26, ADR 0059 Train 1 PR② merge).

**A gate command piped into `tail`/`tee` without `pipefail` reports the
pipe's *last* command's exit status, so the gate can fail while the
wrapper exits 0 (2026-08-26, ADR 0059 Train 1 PR② merge).** A
`cargo check --workspace --all-targets 2>&1 | tail -3` "validation" of a
merge-conflict resolution exited 0 while `cargo check` itself had failed
with E0061 — the compile error was real (main had added new
`run_node_with_streams_quiesce_and_split_mode` call sites in
`split_build.rs`/`split_lifecycle.rs` while the branch being merged had
widened that signature; textual auto-merge sees no conflict in files
only one side changed), and CI caught what the local wrapper claimed to
have checked. Two rules: (a) any pipeline whose exit status gates a
push runs under `set -o pipefail` (or checks `PIPESTATUS[0]`); (b) a
merge of a moved `main` into a branch that changed a function signature
gets its gate run on the *merged* tree specifically because the
dangerous call sites are the ones only `main` has — the same
missed-allowlist class as the "grep every gating match site" rule, at
merge time.
QA (2026-08-25, website mobile pass)** — in this harness,
`chromium --headless=new --window-size=390,H --screenshot=...` lays the
page out at the default ~800px viewport and then *crops* the PNG to
390px, which is visually indistinguishable from the page overflowing
(text "clipped" mid-glyph at the right edge). A mobile layout was
wrongly diagnosed as broken twice this way while the DOM was fine.
**Rule:** for any viewport-dependent check, drive the browser with
Playwright (`/opt/node22/lib/node_modules/playwright`, executablePath
`/opt/pw-browsers/chromium`) and set `viewport: {width, height}` on the
page, asserting `document.documentElement.scrollWidth` and
element `getBoundingClientRect()` from inside the page; keep raw
`--screenshot` for fixed-width artboards only. Note `scrollWidth`
alone can also pass while content is cut (an `overflow: hidden`
ancestor eats the evidence) — pair it with a bounding-rect sweep for
elements extending past the viewport that are not inside a deliberate
`overflow-x: auto` container.
