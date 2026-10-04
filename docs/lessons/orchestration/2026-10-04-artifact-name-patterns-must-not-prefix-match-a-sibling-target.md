# An artifact-name glob must not prefix-match a sibling matrix entry

**Context (R-01 g, `image.yml`, after #1173 merged).** The build matrix
uploaded one digest artifact per image and platform as
`digests-<target>-<suffix>`, and each `merge` job downloaded
`digests-<target>-*`. The two targets are `runtime` and
`runtime-operator`, so `digests-runtime-*` also matched
`digests-runtime-operator-amd64`. The animusd merge job then fed the
operator's digest to `docker buildx imagetools create` for the animusd
repository, and the first push to `main` failed in
`publish (animus-db/animusd)`. The operator leg passed, because its
pattern happens to be the longer name.

The merge job runs only on `push` events, so no PR run could have caught
it. Every PR showed the image workflow green.

**Rule.** When a `download-artifact` `pattern:` is built from a matrix
value, put an explicit delimiter after that value that cannot occur inside
any value (here `digests--<target>--<suffix>` with pattern
`digests--<target>--*`). Check the glob against every matrix value, not
only the one you are editing (`python3 -c 'from fnmatch import fnmatchcase'`
over the full list is enough). Treat a push-only job as untested until its
first `main` run is green.
