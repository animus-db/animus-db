# Screenshotting a themed page in both modes needs the app's own toggle, not Playwright's `colorScheme` context option (website skin restyle, ADR 0056)

`website/assets/site.js` implements the three-state theme switch by
stamping `data-theme="light"|"dark"` on `<html>` **before first paint**,
defaulting to `"light"` when nothing is in `localStorage` yet — "system"
is a third, explicit choice that *removes* the attribute so
`prefers-color-scheme` decides, not the default state. Because
`tokens.css`'s dark values are gated behind
`:root[data-theme="dark"]`/`:root:not([data-theme="light"])`, a
Playwright `browser.newContext({ colorScheme: 'dark' })` has **no
effect** here: the explicit `data-theme="light"` attribute the page's own
JS sets pre-paint always wins over `prefers-color-scheme`, so two
contexts opened with `colorScheme: 'light'` vs `'dark'` render
byte-identical screenshots — a false "looks the same in both themes"
result that silently skips the check it was meant to run. The fix is to
drive the real control: `page.click('.theme-switch
button[data-theme-choice="dark"]')` after load, which is also strictly
better verification — it exercises `site.js`'s actual selectors
(`data-theme-choice`, `aria-pressed`) and catches a markup/JS mismatch
that a context-option shortcut never would. Generalizes to any site whose
theme mechanism is "JS sets an explicit attribute, CSS gates on that
attribute" (the common pattern for a persisted three-state toggle) rather
than pure `prefers-color-scheme`: always toggle through the app's own UI
when screenshotting for a light/dark diff, don't assume a browser-level
colour-scheme hint reaches the page.
