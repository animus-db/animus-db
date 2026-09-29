# Tests that rewrite `/etc/hosts` race every in-process hostname lookup (#1107)

`animus-env`'s `HostsEntryGuard` rewrites the process-global `/etc/hosts`
with a truncate-then-write. Any other test in the same process dialing
`localhost:PORT` at that instant can read the file empty and fail to resolve;
`ProdEnv::send` is fire-and-forget, so the frame is silently dropped and the
victim test (`send_delivers_to_a_peer_registered_by_hostname`) just times out
on `recv`. It only fails with other tests running concurrently, ~1 in 40 even
with only the hosts-mutating test alongside it.

Fix shape: a process-wide `RwLock` (`HOSTS_FILE_LOCK`) — the guard holds the
write side for its lifetime, hostname-resolving tests take the read side.
A test that touches any process-global external state (hosts file, env vars,
iptables) must be ordered against every other test that reads it, not just
against tests that write it. When a flake only appears alongside other tests,
look for shared process/OS state before suspecting the code under test.
