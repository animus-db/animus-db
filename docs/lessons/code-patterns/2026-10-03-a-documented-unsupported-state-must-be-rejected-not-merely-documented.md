# A documented "unsupported" state the operator still accepts is a latent mixed-version rollout

**Lesson.** ADR 0060's Upgrades section said "treat `spec.image` changes as
unsupported" while the validator accepted them and a test even edited the
image as its "other edit". Prose does not stop a `kubectl edit`: the
StatefulSet's default `RollingUpdate` ran the unsupported mixed-version
window. If a state is unsupported, reject it in the shared validator (webhook
*and* the reconciler's no-webhook fallback), and fix tests that used it as a
convenient "unrelated edit".

**How.** Compare *effective* values (resolve defaults the way the renderer
does) so `None` vs explicit-default is not a false change; for the reconciler
fallback read the truth off the live child (the StatefulSet's container
image), not the previous CR spec; before documenting a manual workaround,
verify it from the code (here: PVCs are not owned by the CR and the default
retention policy is `Retain`) and say plainly what is not e2e-tested.
