# Headless-Chromium verification of a static page (the `website/` marketing site, or any HTML rendered headlessly): inject an inline `<script>` into a *copy* of the page and read the result back out of `--dump-dom`.

**Headless-Chromium verification of a static page (the `website/`
marketing site, or any HTML rendered headlessly): inject an inline
`<script>` into a *copy* of the page and read the result back out of
`--dump-dom`.** `--dump-dom` and `--virtual-time-budget` work fine; what
does *not* work is `--evaluate-on-load-file`, which **silently no-ops** —
the script never runs, the dumped DOM is the unmodified page, and there
is no error, so a check that "passes" may never have executed. Verify a
flag's effect once against a page whose title the script is supposed to
change before trusting it as a gate. The working recipe: copy the page
to a scratch dir, splice a `<script>` before `</head>` that writes the
measurement into `document.title` (e.g. `clientWidth`/`scrollWidth` for a
horizontal-overflow check), then
`chrome --headless --window-size=W,H --virtual-time-budget=2500
--dump-dom URL | grep -o '<title>[^<]*</title>'`. For anything needing
real interaction (clicking a theme switch, a mobile nav) or a
`prefers-color-scheme` override, drive the same pre-installed Chromium
through Playwright instead (`node` at `/opt/node22/bin/node`; the
package at `/opt/node22/lib/node_modules/playwright` is **not** on the
default module path, and `NODE_PATH` does not affect ESM resolution, so
import the absolute `/opt/node22/lib/node_modules/playwright/index.mjs`
or run from inside that directory), launching with `executablePath:
'/opt/pw-browsers/chromium-<rev>/chrome-linux/chrome'` and
`args: ['--no-sandbox', '--disable-gpu']`.
