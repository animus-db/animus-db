# A kill issued once the control-plane replica set reads RF 3 can land mid-reconfigure — wait for the data-plane group (3 voters, no learners, a leader) before killing

**A test that kills a replica as soon as `Metadata` lists three replicas
for a tablet is racing the data plane's own growth, and under CPU load it
loses about 1 run in 50-100.** Found root-causing the flaky
`forward_to_tablet_leader_survives_a_dead_first_guess` (`animusd`,
`forward_transport_failure_tests`): it reproduced on `main` (and on PR
#1412's branch; the forwarding refactor was not involved) only with six
busy-loop processes on a 4-core box, 1/77 and 1/45 runs. Its panic was
`budget exhausted chasing the leader (last hop: no time remained to
attempt a hop)` after the full 10 s.

## Mechanism

`provision_tablet` records a best-effort initial replica set and
`reconcile_placement` grows it to RF 3, so the test polled `Metadata` until
`replicas.len() == 3`. That is the control-plane view. The tablet's own Raft
leader adds each new replica as a learner, catches it up, then promotes it,
which takes seconds under load. A per-node dump taken just before the kill
in the failing run showed:

    node0 (victim) Leader  voters=[n0]        learners=[n1]
    node1          Follower voters=[n0, n2]    learners=[]

The kill took out the only voter. The survivors can never form a majority,
so the group stayed leaderless for the whole budget (every hop answered
`not the leader here; leader_hint=none`, then the 100 ms `WaitElection`
backoff, in a loop). No forwarding change or timeout can fix that, so the
product code was behaving correctly; the precondition was wrong.

## Rules

- Before a fault injection, wait on the **data-plane** state the fault
  depends on (every replica hosts the group, `voters.len() == RF`, no
  learners, a leader), not on the control-plane map. This is the same
  hazard as `2026-09-21-a-convergence-poll-timeout-must-dump-each-replicas-own-raft-view.md`.
- Dump each node's own `raft_view` (role, leader, voters, learners) before
  the fault when diagnosing; counts alone cannot tell "leaderless" from
  "never finished growing".
- Reproduce a CPU-load flake with busy-loop processes (more than cores),
  not a bigger N on a quiet box. A first guess of "quiesced group" or
  "election is slow under load" was refuted cheaply by timing a forced idle
  period and reading the failing run's hop log, not by reasoning.
