# A subagent that launches its validation gate as a *background* task and then ends its turn to "wait for the notification" stalls the whole pipeline (2026-08-25, Ledger design delivery)

**A subagent that launches its validation gate as a *background* task and
then ends its turn to "wait for the notification" stalls the whole
pipeline (2026-08-25, Ledger design delivery)** — background-task
completion notifications go to the *orchestrating* session, not to a
subagent that has already stopped; the subagent just sits "finished"
with an uncommitted tree while the orchestrator sees a completion notice
whose result is "waiting for tests". Two of three implementation agents
in one delivery did this independently, each needing a manual resume
("your test task already finished — run checks foreground and commit").
**Rule:** subagent briefs must say *run every check as a synchronous
foreground command and do not end your turn before the commit exists* —
and when an agent's completion notice reads like a status update rather
than a result, resume it immediately with that instruction instead of
waiting for a notification that will never come.
