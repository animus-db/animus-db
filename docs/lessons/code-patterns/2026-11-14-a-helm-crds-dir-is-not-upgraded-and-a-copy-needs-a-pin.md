# A Helm chart's `crds/` copy needs its own drift pin, and Helm will never upgrade it

**What.** The first Helm chart (2026-11-14) ships the CRD under `crds/`. Helm installs files in
`crds/` verbatim (no templating) on first install only; `helm upgrade` and `helm uninstall` never
touch them. So (1) the file must be a literal copy of the generated CRD, and a copy drifts silently
unless a test pins it: `crd_manifest_pinned` got a second test against `AnimusCluster::crd()`, and
`scripts/check-helm-charts.sh` `cmp`s it with `deploy/operator/crd.yaml`; (2) a chart upgrade that
changes the schema needs a manual `kubectl apply -f crds/...`, which the chart README states.

**Related traps found while writing the chart.** The operator selects its own pod by the fixed label
`app.kubernetes.io/name: animus-operator` and (for `--admin-access direct`) by the fixed namespace
`animus-operator`; a chart that lets `nameOverride` or the release namespace change those breaks the
generated NetworkPolicy silently. Rendering checks (`helm lint`/`template`) cannot see this: assert
it in the check script. Also `helm template` has no notes output; to test `NOTES.txt` copy the chart
and rename it to a `.yaml` template.
