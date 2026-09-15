# "Green locally under `yes`-loop contention" does not refute a hosted-runner flake — the two starvations are different in *shape*, not just degree.

**"Green locally under `yes`-loop contention" does not refute a hosted-runner
flake — the two starvations are different in *shape*, not just degree.**
Across four independent investigations into CI flakes, ~340 local executions
under heavy synthetic load (CPU burners, 2-core `taskset` pinning, parallel
same-binary runs) reproduced almost none of them. Raw thread oversubscription
on a multi-core box is round-robined fairly by CFS and mostly yields *slower
average* scheduling; a GitHub-hosted 2-vCPU runner throttles via cgroup
CPU-bandwidth quota, which produces hard periodic full-stop stalls once the
quota is exhausted. Races needing a single >100ms stall of one specific task
surface readily under the latter and rarely under the former. Practical
consequences: (a) do not treat "N clean local runs" as evidence a CI flake is
fixed — say what it does and does not show; (b) prefer a cgroup-quota-
throttled repro (`systemd-run --scope -p CPUQuota=…`, or a manual cgroup v2
write) over busy-loops when trying to reproduce one; (c) a `yes`-loop repro
campaign will happily trip *different* real bugs than the one under
investigation — verify the failure signature matches before counting it as
evidence. (2026-08-20.)
