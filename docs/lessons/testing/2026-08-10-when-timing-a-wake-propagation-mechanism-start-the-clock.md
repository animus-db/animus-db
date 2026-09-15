# When timing a wake/propagation mechanism, start the clock *after* the triggering commit is confirmed, not around the whole write

**When timing a wake/propagation mechanism, start the clock *after* the
triggering commit is confirmed, not around the whole write** — otherwise
the measurement is dominated by unrelated upstream latency (a schema
commit-wait poll, provisioning a fresh tablet) and the assertion's bound
has to be loosened to avoid flaking, which quietly defeats the point of
the test. Porting the ADR 0030 growth-node metadata mirror onto the ADR
0035 PR5 long-poll `WatchMetadata` mechanism, a first-draft regression test
started the timer immediately before a `Put` that both provisioned a fresh
table *and* triggered the mirror update, measuring ~370ms — technically
under a 600ms bound, but that duration was almost entirely the write's own
schema-provisioning round trips, not the watch propagation the test
existed to prove. Moving the clock to start only after the write call
returned (which already guarantees the triggering commit landed) dropped
the measured latency to consistently single-digit milliseconds — a
materially tighter, more honest bound with real teeth against a regression
to the old fixed-200ms poll, and no closer to flaking under parallel
test-suite load than the original number was. (`animusd`
`tests/cluster_growth.rs::growth_node_observes_metadata_promptly_via_watch`.)
