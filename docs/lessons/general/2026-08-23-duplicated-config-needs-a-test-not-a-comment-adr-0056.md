# Duplicated config needs a test, not a comment (ADR 0056)

The design tokens have to exist twice — the site ships static files, the
consoles `include_str!` theirs into the binary, and ADR 0021 bans the build step
that would generate one from the other. The previous revision handled this with
a comment in each file saying "same values as the other one". They drifted
anyway; that is what commissioning a design system turned up.

The fix that generalises: **when two files must stay identical and no mechanism
can make them one file, the mechanism is a test.**
`dashboard::tokens_css_matches_website_copy` is a three-line `assert_eq!` over
two `include_str!`s, and it makes the drift impossible rather than discouraged.
A comment asking humans to remember is not a mechanism.

The corollary is knowing what must NOT be in the check. The `@font-face` blocks
carry the same faces by different delivery (URL vs base64 `data:` URI) because
the deployments differ, so they are deliberately outside it. A check that
over-reaches gets disabled the first time it is legitimately wrong.
