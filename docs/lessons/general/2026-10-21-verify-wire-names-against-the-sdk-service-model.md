# Verify wire names against the SDK service model, not doc extracts

ADR 0075 carried an "AWS field names and error texts are unverified" caveat for
weeks because the only sources were search-result extracts of the AWS docs and
the doc site was unreachable. The AWS SDKs are generated from a machine-readable
service model (botocore `service-2.json`), which is downloadable from the SDK
repositories and states every member name, nesting, required flag, enum value
and per-operation error list.

What generalises:

- For any "is this the AWS name?" question, read the service model first. It
  closed in minutes what extracts could not, and it found things the extracts
  hid (`ReplicationGroupUpdate` also defines `Disassociate`; `UPDATING` is a
  valid `ReplicaStatus` but we never had a truthful state for it).
- The model settles names and exception types per operation. It does NOT
  contain message strings or behaviour on edge cases, so record those as
  unverifiable rather than "verified".
- A doc claiming a value is emitted must be checked against the code's enum; the
  ADR said `UPDATING` was emitted and the enum had no such variant. Pin emitted
  enum values with a unit test against the model's list.
