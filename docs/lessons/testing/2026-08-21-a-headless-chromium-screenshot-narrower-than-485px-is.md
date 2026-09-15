# A headless-Chromium screenshot narrower than ~485px is cropped, not a layout bug.

**A headless-Chromium screenshot narrower than ~485px is cropped, not a
layout bug.** The browser enforces a minimum window width, so
`--window-size=390,H` lays the page out at ~485px and captures a 390px
slice of it — content looks clipped at the right edge and a centered
container looks off-centre. Diagnosing that as responsive breakage sends
you chasing a bug that isn't there. Measure overflow numerically
(`scrollWidth` vs `clientWidth`, above) rather than trusting the
narrow-viewport image, and treat ~500px as the narrowest *trustworthy*
screenshot width. (website DynamoDB-focus pass, 2026-08-21.)
