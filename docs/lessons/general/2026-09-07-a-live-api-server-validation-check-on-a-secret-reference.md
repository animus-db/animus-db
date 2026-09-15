# A live (API-server) validation check on a `Secret` reference must not strip the field the way a pure spec-shape check does — "fall back to unset" can itself cause the outage the check exists to prevent (ADR 0069, S-03 PR 3, `animus-operator`)

Every pre-existing `*SpecInvalid` condition in `animus-operator`'s
reconciler (`TlsSpecInvalid`/`S3SpecInvalid`/`StoreSpecInvalid`) follows
the same shape: a pure, no-cluster-access check on the spec's own fields
(both/neither of two mutually exclusive shapes set, a malformed URI, an
empty required string) finds a problem, sets a condition, and reconciles
the *rest* of that pass with the offending field stripped to `None` — safe
because the field's own value was never going to be usable regardless of
what else is in the cluster.

`spec.encryptionKeySecretName`'s own check (ADR 0069, S-03 PR 3) looks
structurally identical — read a `Secret`, find it missing or malformed,
set a condition — but copying the "strip and fall back" shape onto it
would have been a real, self-inflicted hazard rather than a merely inert
one. Unlike a malformed URI, "the named `Secret` doesn't exist *yet*" (or
temporarily failed an API read) is not evidence the field's *value* is
wrong — it's evidence a resource hasn't shown up, which is exactly the
kind of transient condition a reconciler runs again in 30 seconds
expecting to self-heal. Falling back to "as if unset" for that one pass
would regenerate a `cluster.json` with no `encryption_key_path` on any
node; since the field's presence is baked into the config-hash restart
annotation, that fallback is not just informational, it is itself a
`spec.template` change that rolls every pod straight into the underlying
system's own loud refusal (an `animusd` process finding its data
directory already marked encrypted with no key configured) — the operator
would have manufactured the exact `CrashLoopBackOff` its own validation
was trying to give the operator advance warning about, worse than doing
nothing.

**Fix**: this check sets the condition and leaves the spec's own,
still-`Secret`-name-referencing value in place either way. A genuinely
missing `Secret` then just leaves the pod's volume unable to mount
(`ContainerCreating`, not a crash loop) until the `Secret` shows up —
harmless, and self-healing without the operator having changed anything
about the desired encryption state.

**General form**: "strip the field and reconcile as if unset" is only a
safe fallback when the field's *value itself* is what's wrong (a spec-shape
error, checkable with no cluster access, where "unset" is a value the
field could legitimately have held). It is the wrong fallback when the
check is actually asking "does this external resource exist/look right
*yet*" — there, "unset" is not a neutral default, it can be an active,
consequential state change (here: flipping an encrypted cluster's own
generated config back to plaintext) that the reconciler must never make on
its own initiative just because a live read came back empty this once.
Before copying an existing validate-and-strip pattern onto a new live
check, ask which category the check actually falls into.
