# A roll supervisor cannot read progress from state that only exists after the roll

`animus cluster roll plan` decided which nodes were already on the new binary from
each node's *recorded* version range in replicated `Metadata`. Before the version
era is active (the first roll over Phase 1 binaries) nothing is recorded until the
last member is rolled, so a second `plan` mid-roll still listed every node as old.
The ProdEnv previous-release test only found it because it ran the real CLI against
real old bytes; the sim corpus and the fake-server CLI tests all modelled a cluster
that already had ranges.

Rule: when a derived status depends on a replicated fact, ask what that fact is
*before* the feature that produces it exists, and fall back to the one source that is
true regardless (here each node's own `/admin/cluster-version`, which an old binary
simply does not serve). Test it with a fixture of the pre-feature state, and have the
slow end-to-end test re-ask the question after every step, not once at the start
(it had been working around the bug by taking the plan once).

Related, for `kind` legs that must observe availability across a roll: run the client
in the cluster. A host `kubectl port-forward` pins one pod and dies with it, so it
measures the port-forward, not the service.
