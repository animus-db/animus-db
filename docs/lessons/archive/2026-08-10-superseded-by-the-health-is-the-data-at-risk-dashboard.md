# Superseded by the "health ≈ is the data at risk" dashboard ladder (PR feat/console-health-data-risk)

The dashboard's `computeHealth()` originally treated any tablet without an
elected leader (`leaderlessCount`) or with fewer hosting groups than
configured (`underReplicatedCount`) as "degraded" — collapsing every kind of
"not fully converged" tablet (including a split-child mid-formation, whose
data was never at risk per ADR 0028) into the same red status as a genuine
node-failure-driven redundancy loss. Replaced by a four-rung ladder
(`quorum-lost`/`under-replicated`/`healthy`/`forming`) keyed on whether each
assigned replica's *node* is actually live, so routine transitions render
neutral and only genuine data-risk states degrade health (ADR 0021 §7).
