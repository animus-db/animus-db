# A gate described in the docs is not a gate until a check enforces it, and an assistant's commit e-mail is not an identity

**What happened (2026-11-14).** ADR 0007, CONTRIBUTING.md, the README and the
website all said contributions "require a DCO sign-off and a CLA", and
CONTRIBUTING promised "a CLA bot will comment on your first PR". Only the DCO
half existed (`dco.yml`). There was no CLA text, no bot and no check, so any
outside contribution merged before this date would have been unlicensable under
the commercial terms the CLA exists for. Nobody noticed for three months
because every contributor was the maintainer or a coding assistant working for
him; the gap was only found when the beta plan asked whether the CLA was "in
place".

**Second finding.** Coding-assistant commits here are authored as
`Claude <noreply@anthropic.com>`, and GitHub resolves that e-mail to an
unrelated personal account (`claude`, id 81847). Any check that keys on the
commit's GitHub login would demand that a stranger sign the CLA, or, worse,
accept a stranger's signature for the maintainer's work.

**Rules.**
- When a doc says something is "required", find the job that fails without
  it. If there is none, the doc is a promise, and a promise in a legal or
  compliance area is a liability; build the check or soften the doc in the
  same change.
- Identify assistant-authored commits by author e-mail (`.github/cla.json`
  `assistantEmails`) and attribute them to the PR author, never by the GitHub
  login the e-mail happens to resolve to.
- A check that needs a write token on fork PRs splits in two: a
  `pull_request_target`/`issue_comment` workflow that writes and runs only the
  base branch's code, and a read-only `pull_request` job inside `ci.yml` that
  blocks via `gates`.
