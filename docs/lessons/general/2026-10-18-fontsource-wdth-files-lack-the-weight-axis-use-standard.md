# Fontsource `-wdth-` woff2 vs `-standard-`: pick the file with both axes

When self-hosting a two-axis variable font (Archivo wdth+wght) from `npm pack @fontsource-variable/<name>`,
the files named `*-latin-wdth-normal.woff2` carry only the width axis; `*-latin-standard-normal.woff2` carries
both width and weight (the package's `standard.css` declares `font-weight: 100 900; font-stretch: 62% 125%` for it).
Declare both ranges in the `@font-face`, then `font-stretch: 75%` and `font-weight: 800` apply. Verify in a browser
screenshot: a missing axis silently renders the normal-width cut, which looks "almost right" at a glance.

Also: the shared `tokens.css` has a byte-identity test against the dashboard copy, so edit one generator
(or one file) and copy, then run `cargo test -p animusd --lib tokens_css_matches_website_copy`.
