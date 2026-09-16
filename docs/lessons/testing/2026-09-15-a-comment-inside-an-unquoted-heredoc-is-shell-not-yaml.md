# A comment inside an unquoted heredoc is shell, not YAML, and `bash -n` will not tell you

`scripts/e2e-kind.sh` renders the e2e `AnimusCluster` manifest with an
unquoted heredoc (`cat >"$MANIFEST_FILE" <<EOF`) so that `${AC_NAME}` and
friends expand. PR #909 added a YAML comment block inside it whose prose
carried backticks and `$(...)`-looking text. Bash performs command
substitution inside an unquoted heredoc regardless of a leading `#` (a
`#` is a YAML comment, not a shell one, in that position), so every
backtick-quoted phrase was *executed* (`storage:: command not found`,
`crates/animus-operator/CLAUDE.md: Permission denied`), the manifest came
out corrupted (`yaml: line 53: could not find expected ':'`), and every
e2e variant failed at "apply AnimusCluster". `bash -n` passed: it checks
syntax, not what a heredoc expands to.

Rules:

- Explanatory prose belongs in a shell comment *above* the heredoc, never
  inside it. Inside an unquoted heredoc keep comments free of backticks,
  `$(`, and `${` unless you mean them.
- Gate a change to any heredoc-rendered manifest by actually rendering
  it: run the heredoc with the variables set and parse the output
  (`python3 -c 'import yaml, sys; yaml.safe_load(open(sys.argv[1]))'`).
  That is a five-second check `bash -n` cannot replace.
