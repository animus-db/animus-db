# A task prompt's reference to a specific function/mechanism by name can be stale by the time you implement it, if an earlier PR in the same stack already redesigned it out of existence — grep before assuming the follow-up still applies.

**A task prompt's reference to a specific function/mechanism by name can be
stale by the time you implement it, if an earlier PR in the same stack
already redesigned it out of existence — grep before assuming the
follow-up still applies.** ADR 0038 PR5's brief named a specific residual
item from PR3 ("mirror_loop's fixed 50ms poll → wake on the apply task's
publish signal") as a small thing to fold in. No such function exists:
PR3's cutover renamed/redesigned the shadow-mode PR2 `mirror_loop` into
`meta_apply_loop`/`meta_apply_and_compact`, which already backs off on a
short idle-only timer (`APPLY_IDLE_POLL`, 5ms) and otherwise stays in
lockstep behind commit under load — not the "fixed 50ms poll regardless of
activity" shape the follow-up described. The item was already resolved by
a prior PR in the same stack; re-implementing "a fix" for it would have
been either a no-op or, worse, a regression dressed up as progress. This
is the root `CLAUDE.md`'s "grep before implementing a documented gap"
practice applying just as much to a task-prompt's own named follow-up as
to an ADR's prose.
