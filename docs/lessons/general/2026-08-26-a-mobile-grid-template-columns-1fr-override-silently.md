# A mobile `grid-template-columns: 1fr` override silently reintroduces the desktop overflow it was meant to fix (website responsive pass, ADR 0056 skin)

The site's desktop grids (`.hero-grid`, `.split`, `.foot-grid`) were all
written correctly, as `minmax(0, 1fr) minmax(0, 1fr)` — the standard
CSS-grid guard against a track's "automatic minimum size" defaulting to the
*min-content* width of whatever's inside it. But their `@media` overrides for
the mobile single-column layout were written as plain `grid-template-columns:
1fr;`, dropping the `minmax(0, ...)` the desktop rule had. Nothing looked
wrong reading the CSS in isolation — a single `1fr` column obviously fills
100% of the container, so it read as strictly *simpler* than the two-column
desktop version, not weaker.

It broke exactly where a grid item contained an unbreakable line: a
`.plate .cl` terminal/diff line has `white-space: pre`, and one such line
(`- endpoint: dynamodb.eu-west-1.amazonaws.com`) was longer than the mobile
viewport. Its nested `overflow-x: auto` container did make *it* scroll
locally as intended — but the plain-`1fr` grid track one level up still sized
itself to that line's min-content width first, so the grid item's own box
(not just its content) rendered wider than the viewport and the page itself
gained horizontal scroll. A descendant's own `overflow: auto` does **not**
rescue an ancestor grid/flex track's auto-min-size calculation; only
`minmax(0, ...)` (or `min-width: 0` on the item) on the track/item that is
actually oversized does that — matching what the desktop rule already relied
on for the exact same content.

General rule: **`minmax(0, 1fr)` is not a two-column-only idiom** — carry it
into every breakpoint override of a `grid-template-columns` declaration,
including the single-column ones, whenever any descendant might contain
unbreakable content (`white-space: nowrap`/`pre`, a long token, a wide inline
code/terminal line). A quick audit technique: `grep grid-template-columns` the
stylesheet and check that every bare `1fr`/`Npx` track (not already wrapped in
`minmax(0, ...)` or `min(...)`) has one — a bare `repeat(N, 1fr)` mobile
override is the same bug with more columns. Verifying "no viewport overflow"
by reading the CSS's collapse breakpoints is not enough; render at the target
width and check `document.documentElement.scrollWidth`, because this class of
bug is invisible in the source and only appears against real content.

- **A Playwright `waitUntil: 'networkidle'` goto against the `website/`
  pages can hang indefinitely in this sandbox (2026-08-26, mobile spacing
  pass)** — the pages pull Google Fonts over HTTPS through the proxy; most
  of the time that request resolves quickly, but occasionally it (or some
  other cross-origin request) stays pending and `networkidle` never fires
  because it waits for a quiet network, not a deadline. A batch script
  driving many pages back-to-back (an 11-page x 5-width overflow matrix)
  looked like it was making progress — earlier `ok` lines kept appearing in
  the output — right up until it silently wedged on one page, with no error
  and no timeout to report. Two fixes, both worth doing together: pass an
  explicit `timeout` on every `goto` (Playwright's default is generous but
  finite; the real risk is a bare `networkidle` promise with no timeout
  argument at all in a batch loop), and for anything that doesn't need the
  fonts to actually render (an overflow/scrollWidth check, not a screenshot)
  `page.route('**://fonts.*/**', route => route.abort())` before `goto` and
  use `waitUntil: 'load'` instead — it removes the flaky external dependency
  entirely and the check runs in a fraction of the time. Keep `networkidle`
  (with a timeout) for visual screenshots where you want the real fonts
  loaded, and keep it consistent between a before/after pair — a pixel-diff
  comparing a real-fonts render against a fallback-fonts render reports
  every glyph as changed, which reads as a layout regression that isn't one.
