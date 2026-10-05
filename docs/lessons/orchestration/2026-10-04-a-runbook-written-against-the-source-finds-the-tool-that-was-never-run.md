# Writing a runbook against the source finds tools that were never run end to end

Found writing `docs/runbook/` (R-01(e)): verifying each documented command against the code
and then against a running `animusd --cluster-control 1 --cluster-data 2` turned up two
defects that every test had missed, because each layer was tested only against a fake of
its neighbour.

- `animus admin <sub>` (every admin CLI subcommand) fails against a real admin port with
  `client handshake ... TimedOut`: `http_call` goes through `maybe_tls_connect`, which runs
  the client-protocol preamble before speaking HTTP, but the admin listener is plain HTTP.
  The CLI's unit tests build requests (`admin_request`) and never dial; the node's tests use
  their own HTTP helper.
- The operator's scale-down posts `/admin/drain` to the pod being removed, but drain is
  control-leader-only and a data-only pod has no control handle (`409 not the control-plane
  leader`). The operator tests use `FakeAdminClient`, and `scripts/e2e-kind.sh` never scales down.

The rule: **a procedure is not documented until each command has been run once against a real
process.** Ten minutes with a built dev cluster and `curl` is cheaper than discovering these during
an incident. Related: a fake that always succeeds (`FakeAdminClient`'s default "already drained") hides
exactly the routing preconditions (leader-only, not relayed) that the real endpoint enforces; give such
fakes a leader-only mode or add an e2e leg for the path.
