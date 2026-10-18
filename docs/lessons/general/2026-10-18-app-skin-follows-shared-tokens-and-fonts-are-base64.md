# The consoles' skin follows the shared tokens; their fonts are base64 in `fonts.css`

(2026-10-18, website redesign applied to the dashboard and console.)

- `crates/animusd/src/fonts.css` carries each face as a base64 `data:` URI,
  because the consoles serve `include_str!` strings. A new face means:
  copy the woff2 into `crates/animusd/src/fonts/`, base64 it into a new
  `@font-face` (keep `font-stretch` for a width-axis face such as Archivo),
  and delete the old woff2 once nothing names it. The crate cannot reach
  `website/` assets at runtime.
- The tokens revision re-valued `--font-ui` and `--code-bg`, but any rule in
  the app skins that hard-coded `background: var(--text)` (primary buttons,
  active segmented options) stayed a black block. Grep the skins for
  `var(--text)` used as a background whenever the palette rule changes.
- Playwright: the `playwright` python module is not installed; use
  `/opt/node-tools/node_modules/playwright` from node. The dashboard defaults
  to the light theme regardless of `prefers-color-scheme`, so a dark
  screenshot must click `[data-theme-choice=dark]`.
- Do not `pkill -f` a pattern that also appears in your own shell command line.
