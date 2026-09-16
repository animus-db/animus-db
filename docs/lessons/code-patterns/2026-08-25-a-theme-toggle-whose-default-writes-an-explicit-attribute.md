# A theme toggle whose default writes an explicit attribute defeats a static dark-mode QA render unless the toggle script is neutralized first (2026-08-25, website rebuild on the Ledger system).

**A theme toggle whose default writes an explicit attribute defeats a
static dark-mode QA render unless the toggle script is neutralized first
(2026-08-25, website rebuild on the Ledger system).** `site.js` applies
`stored()` on every load — and once "light" (not "system") is the
documented default, that call sets `data-theme="light"` on `<html>`
explicitly rather than leaving the attribute absent. A QA render that
hand-edits a scratch copy of a page to add `data-theme="dark"` and then
loads it with the page's own script still attached gets silently
overwritten back to light before first paint — the screenshot comes out
byte-identical to the light render (same file size, confirmed before
looking closer), which reads as "dark mode is broken" when the actual bug
is the QA method fighting the app's own initialization order. The fix is
to strip the theme script from the scratch copy for a static-render check
(the CSS is what's under test, not the runtime toggle), not to debug the
CSS. **General rule**: before concluding a themed page's dark styling is
broken from a scripted/automated screenshot, check whether the page's own
JS re-applies a *default* state on load — a `checked`/`data-*`/class
toggle with a persisted-with-fallback default will clobber any attribute
a test harness sets by editing the static HTML, and the fallback is easy
to miss precisely because it used to be a no-op ("system" removed the
attribute) before the default changed to an explicit value.
