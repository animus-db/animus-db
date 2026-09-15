# A standing instruction buried as a Conventions bullet doesn't set session posture — put binding defaults at the top of CLAUDE.md *and* inject them at boot (2026-08-23).

**A standing instruction buried as a Conventions bullet doesn't set session
posture — put binding defaults at the top of CLAUDE.md *and* inject them at
boot (2026-08-23).** The subagent-delegation / background-execution /
stack-by-default rules had lived for weeks as one bullet deep in CLAUDE.md's
Conventions, and the maintainer still had to re-issue all three as live
instructions in session after session: agents either did token-heavy work
inline in the main thread or ran subagents in the foreground and went
silent, and shipped flat PRs where a stack was wanted. The lesson is about
*placement and delivery*, not wording — an entry-point doc is skimmed once
at boot with attention concentrated at the top, and a rule that must shape
a session's default behavior (rather than answer a lookup) competes badly
from position 8 of a bullet list 300 lines in. **Fix pattern**: promote
behavioral defaults to a short, numbered "Session operating mode" section
at the very top of CLAUDE.md, and have the SessionStart hook `cat` a
faithful summary to stdout — a SessionStart hook's stdout is added to the
agent's context, which turns the posture into a boot-time instruction that
arrives the way a maintainer's own message would. Keep the two in sync by
making CLAUDE.md the named source of truth and the hook text explicitly a
summary of it; and make the rules *thresholded* so they need no judgment
call (delegate anything multi-file/exploratory/build-heavy, inline only
trivial one-file work; stack whenever there is more than one reviewable
logical step, flat PRs state why).
