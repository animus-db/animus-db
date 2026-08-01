# ADR 0005 — Placement groups + topology-aware data residency

- **Status:** Accepted
- **Date:** 2026-08-01

## Context

Operators need to control *where* data physically lives — for latency (keep data
near its users) and, increasingly, for legal **data residency** (e.g. EU
customer data must stay in EU jurisdictions). A flat, topology-blind placement
policy cannot express "these rows may only be replicated within these regions."

## Decision

We will make nodes carry **topology labels** (e.g. `region=eu-west`,
`zone=eu-west-1a`, `jurisdiction=EU`) in the control-plane membership state, and
introduce **placement groups**: named policies that constrain which topology
domains a tablet's replicas may occupy and how replicas spread across failure
domains. A tablet belongs to a placement group; the control-plane reconciler
chooses replica sets that satisfy the group's constraints.

This ADR records the model. Enforcement of residency across hinted handoff,
read-repair, anti-entropy, and backup is explicitly **later work** (out of scope
for the first milestones) — those paths can leak data across boundaries if not
designed with residency in mind, so they must be addressed deliberately.

## Consequences

- Residency and locality become declarative policy on the control plane rather
  than manual replica placement.
- Topology labels become first-class membership metadata from the start, so the
  membership model is designed to carry them even before placement groups are
  implemented.
- Residency is only as strong as its weakest path: hinted handoff, repair, and
  backup must all honor the same constraints, which is significant future work
  and a correctness (and compliance) risk if rushed.
