# A shared `StatefulSet` pod-template annotation restarts every pod, not just the ordinal a spec change was meant for — the pod template has no per-ordinal slot (S-07d, config-hash restart mechanism)

S-07d needed *some* already-running pod to notice a `ConfigMap` content
change and restart (role-promotion for control-voter growth is decided in
`entrypoint.sh` at container start, never hot-reloaded) — the standard
Kubernetes idiom for this is a content-hash annotation on the pod
template, which turns a `ConfigMap` change into a `StatefulSet.spec.
template` change the `StatefulSet` controller rolls out like any other
pod-template edit. The point worth recording: **there is exactly one pod
template per `StatefulSet`, shared by every ordinal** — an annotation
placed there cannot be scoped to "just the ordinal(s) that actually need
to restart." Adding this mechanism for `controlNodes` growth specifically
therefore also restarts every *other* pod on *every* config-affecting
spec change this operator already had (`spec.tls`, `spec.s3`, `spec.
backupStore`/`segmentStore`, `spec.dynamoAuthSecretName`, `spec.
quiesceAfterSecs`/`autoSplitBytes`; a `nodes`-only scale is deliberately
*not* one of them — see the sibling entry on hashing only what a pod
reads at boot) — fields that, before this change,
silently sat unapplied on an already-running pod until it happened to
restart for an unrelated reason. That silent-no-op behavior was arguably
a latent bug in every one of those features' own delivery, only now
surfaced (and fixed, as a side effect) by a mechanism built for a
different field entirely.

**General form**: a per-pod-template annotation/env-var/volume is a
whole-`StatefulSet`-scoped lever, not a per-ordinal one — if a design
needs to affect *only* certain ordinals (the way S-07d's own role
promotion conceptually only needed to touch the newly-promoted ones), the
pod template itself cannot express that; either accept the
whole-set-restarts cost (as this change did, since there is no clean way
to avoid it while every ordinal still shares one template) or reach for a
mechanism that genuinely varies per-pod (e.g. a per-ordinal `ConfigMap`/
`Secret`, or an `initContainer` reading its own ordinal at start) — never
assume a template-level annotation change stays scoped to "the pods that
actually needed it."
