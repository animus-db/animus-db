# Sourcing a real Google Fonts variable-font `.woff2` for embedding (ADR 0056's 2026-08-31 amendment, the Space Grotesk/Martian Mono → Work Sans/JetBrains Mono font swap)

The Google Fonts CSS2 API (`https://fonts.googleapis.com/css2?family=...`)
returns a **static** per-weight face by default, even for a family that
does ship a variable font — `family=Work+Sans:wght@400;500;600;700`
returns four separate weight-scoped `@font-face` blocks pointing at four
different `.woff2` files, not one file covering the range. To get the
single-file-per-family property this ADR's embedding strategy depends on
(`crates/animusd/src/fonts.css`'s own doc comment: "one file per family
covering the whole weight range," not one per weight), the request needs
the **range** syntax instead — `wght@100..900` (two dots, a closed
interval), which the API recognizes as "give me the variable axis" and
returns one `@font-face` per **script subset** (vietnamese/latin-ext/latin/
…), each still spanning the declared weight range via a single
`font-weight: 100 900;` line and a single `.woff2` url — the **latin**
subset block (usually the last one in the response, `unicode-range:
U+0000-00FF, …`) is the one to keep; the others cover scripts this
codebase's Latin-only convention (ADR 0056's own "Latin subset" framing)
has never needed. A response's `font-weight` range is also the fastest way
to confirm you got the variable build rather than a static one — a static
per-weight response never has two numbers on that line. Second gotcha:
the API 403s/serves a stripped legacy response to an unrecognized or
absent `User-Agent` (its browser-sniffing decides which font format —
woff2 vs. ttf — and which API shape to serve); a plain modern-browser UA
string (`Mozilla/5.0 ... Chrome/128.0.0.0 Safari/537.36`) on both the CSS
fetch and the follow-up `fonts.gstatic.com/.../*.woff2` binary fetch is
required — a request with no UA, or a generic `curl/*` one, can silently
get a different (or empty) result. Once you have the CSS response, the
actual binary is one more plain `curl` of the `url(...)` inside the
`latin` block's `src:`; `base64 -w0` the result straight into the
`data:font/woff2;base64,...` `@font-face` src exactly as the existing
fonts.css comment already documents for regeneration. One real
consequence worth knowing before assuming "same shape, similar size":
different families' variable-font builds are not size-comparable just
because they're both "one file, full weight range" — Work Sans's and
JetBrains Mono's latin variable builds came back meaningfully larger than
Space Grotesk's/Martian Mono's (~50 KB and ~40 KB raw vs. ~22 KB and
~24 KB), pushing the consoles' total embedded-font base64 from ~61 KB to
~118 KB — not a sign anything went wrong, just a property of the
specific typefaces, and not something to "fix" by falling back to a
per-weight static embed (which would cost far more, not less).
