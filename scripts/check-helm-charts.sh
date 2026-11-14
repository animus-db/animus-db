#!/usr/bin/env bash
# Static checks for the Helm charts in deploy/helm/ (no cluster needed):
#
#   * helm lint of both charts, including the webhook in both cert modes
#   * helm template with default and non-default values
#   * the chart's ClusterRole rules == deploy/operator/rbac.yaml's
#   * the chart's CRD copy == deploy/operator/crd.yaml (byte for byte)
#   * the animus-cluster chart's rendered spec validates against the CRD's
#     openAPIV3Schema
#   * both charts' appVersion == the workspace version in Cargo.toml
#
# Needs: helm (3.x), python3 with PyYAML and jsonschema
# (`pip install pyyaml jsonschema`). Run from anywhere.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
op="$root/deploy/helm/animus-operator"
cl="$root/deploy/helm/animus-cluster"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

command -v helm >/dev/null || { echo "helm not found on PATH" >&2; exit 1; }
python3 -c 'import yaml, jsonschema' 2>/dev/null \
  || { echo "python3 needs PyYAML and jsonschema (pip install pyyaml jsonschema)" >&2; exit 1; }

step() { echo "== $*"; }

# --- versions -------------------------------------------------------------
step "appVersion matches the workspace version"
want="$(sed -n '/^\[workspace.package\]/,/^\[/{s/^version *= *"\(.*\)"/\1/p}' "$root/Cargo.toml" | head -1)"
[ -n "$want" ] || { echo "could not read the workspace version" >&2; exit 1; }
for c in "$op" "$cl"; do
  got="$(sed -n 's/^appVersion: *"\{0,1\}\([^"]*\)"\{0,1\} *$/\1/p' "$c/Chart.yaml")"
  [ "$got" = "$want" ] || { echo "$c/Chart.yaml appVersion=$got, workspace version=$want" >&2; exit 1; }
done

# --- CRD copy -------------------------------------------------------------
step "chart CRD is byte-identical to deploy/operator/crd.yaml"
cmp "$root/deploy/operator/crd.yaml" "$op/crds/animusclusters.animusdb.io.yaml" \
  || { echo "regenerate both copies: see crates/animus-operator/tests/it/crd_manifest_pinned.rs" >&2; exit 1; }

# --- lint -----------------------------------------------------------------
step "helm lint"
cm="--set webhook.enabled=true --set webhook.certManager.enabled=true --set webhook.certManager.issuerRef.name=my-issuer"
ex="--set webhook.enabled=true --set webhook.caBundle=Q0E="
helm lint "$op"
# shellcheck disable=SC2086
helm lint "$op" $cm
# shellcheck disable=SC2086
helm lint "$op" $ex
helm lint "$cl"
helm lint "$cl" --set spec.nodes=5 --set spec.image=example.com/animusd:x

# webhook without any certificate source must be refused
if helm template t "$op" --set webhook.enabled=true >/dev/null 2>&1; then
  echo "webhook.enabled with no certificate source should fail to render" >&2; exit 1
fi

# --- render ---------------------------------------------------------------
step "helm template"
helm template rel "$op" -n ns > "$tmp/op-default.yaml"
# shellcheck disable=SC2086
helm template rel "$op" -n ns $cm \
  --set image.tag=latest --set adminAccess=direct --set 'extraArgs={--foo}' \
  --set serviceAccount.annotations.a=b --set priorityClassName=high \
  --set nodeSelector.k=v --set 'tolerations[0].key=k' > "$tmp/op-cm.yaml"
# shellcheck disable=SC2086
helm template rel "$op" -n ns $ex > "$tmp/op-ex.yaml"
helm template rel "$op" -n ns --set rbac.create=false --set serviceAccount.create=false > "$tmp/op-norbac.yaml"
helm template rel "$cl" -n ns > "$tmp/cl-default.yaml"
helm template rel "$cl" -n ns --set spec.nodes=5 --set spec.controlNodes=3 \
  --set spec.storage.size=50Gi --set spec.storage.storageClassName=fast > "$tmp/cl-big.yaml"
helm template rel "$cl" -n ns --set fullnameOverride=db --set spec.image=example.com/animusd:x > "$tmp/cl-img.yaml"

# --- semantic checks ------------------------------------------------------
step "rendered RBAC, webhook and AnimusCluster checks"
python3 - "$root" "$tmp" <<'PY'
import sys, glob, yaml, json, copy
import jsonschema

root, tmp = sys.argv[1], sys.argv[2]

def load(p):
    return [d for d in yaml.safe_load_all(open(p)) if d]

def kinds(docs):
    return [d["kind"] for d in docs]

raw = load(f"{root}/deploy/operator/rbac.yaml")
want_rules = next(d for d in raw if d["kind"] == "ClusterRole")["rules"]

# RBAC rules identical to the raw manifest, in every render that creates them.
for name in ("op-default", "op-cm", "op-ex"):
    docs = load(f"{tmp}/{name}.yaml")
    cr = next(d for d in docs if d["kind"] == "ClusterRole")
    assert cr["rules"] == want_rules, f"{name}: ClusterRole rules differ from deploy/operator/rbac.yaml"
    crb = next(d for d in docs if d["kind"] == "ClusterRoleBinding")
    assert crb["subjects"][0]["namespace"] == "ns", f"{name}: binding subject namespace"
    assert crb["roleRef"]["name"] == cr["metadata"]["name"]
    dep = next(d for d in docs if d["kind"] == "Deployment")
    assert dep["metadata"]["namespace"] == "ns"
    assert dep["spec"]["replicas"] == 1
    sel = dep["spec"]["template"]["metadata"]["labels"]
    assert sel["app.kubernetes.io/name"] == "animus-operator", "pod label the operator NetworkPolicy relies on"

norbac = load(f"{tmp}/op-norbac.yaml")
assert not {"ClusterRole", "ClusterRoleBinding", "ServiceAccount"} & set(kinds(norbac)), "rbac/serviceAccount toggles"

# Default render has no webhook objects and no webhook flags.
d = load(f"{tmp}/op-default.yaml")
assert "ValidatingWebhookConfiguration" not in kinds(d) and "Service" not in kinds(d)
args = next(x for x in d if x["kind"] == "Deployment")["spec"]["template"]["spec"]["containers"][0]["args"]
assert args == ["run", "--admin-access=proxy"], args
img = next(x for x in d if x["kind"] == "Deployment")["spec"]["template"]["spec"]["containers"][0]["image"]
assert img.endswith(":0.1.0-alpha.0") or ":" in img

# Webhook, cert-manager mode: Certificate + inject annotation, flags, volume.
cm = load(f"{tmp}/op-cm.yaml")
vwc = next(x for x in cm if x["kind"] == "ValidatingWebhookConfiguration")
cert = next(x for x in cm if x["kind"] == "Certificate")
svc = next(x for x in cm if x["kind"] == "Service")
assert vwc["metadata"]["annotations"]["cert-manager.io/inject-ca-from"] == f"ns/{cert['metadata']['name']}"
w = vwc["webhooks"][0]
raw_w = next(x for x in load(f"{root}/deploy/operator/webhook.yaml") if x["kind"] == "ValidatingWebhookConfiguration")["webhooks"][0]
for k in ("name", "admissionReviewVersions", "sideEffects", "failurePolicy", "timeoutSeconds", "rules"):
    assert w[k] == raw_w[k], f"webhook field {k} differs from deploy/operator/webhook.yaml"
cc = w["clientConfig"]["service"]
assert cc == {"name": svc["metadata"]["name"], "namespace": "ns", "path": "/validate", "port": 443}, cc
assert svc["spec"]["ports"][0] == {"name": "webhook", "port": 443, "targetPort": "webhook"}
sn = svc["metadata"]["name"]
assert cert["spec"]["dnsNames"] == [sn, f"{sn}.ns", f"{sn}.ns.svc", f"{sn}.ns.svc.cluster.local"]
assert cert["spec"]["issuerRef"]["name"] == "my-issuer"
dep = next(x for x in cm if x["kind"] == "Deployment")["spec"]["template"]["spec"]
c = dep["containers"][0]
assert "--webhook-addr=0.0.0.0:9443" in c["args"] and "--admin-access=direct" in c["args"] and "--foo" in c["args"]
assert dep["volumes"][0]["secret"]["secretName"] == cert["spec"]["secretName"]
assert dep["priorityClassName"] == "high" and dep["nodeSelector"] == {"k": "v"}
assert c["image"].endswith(":latest")

# Webhook, existing-secret mode: no Certificate, caBundle set, no annotation.
ex = load(f"{tmp}/op-ex.yaml")
assert "Certificate" not in kinds(ex)
v = next(x for x in ex if x["kind"] == "ValidatingWebhookConfiguration")
assert v["webhooks"][0]["clientConfig"]["caBundle"] == "Q0E="
assert "annotations" not in v["metadata"]

# AnimusCluster renders validate against the CRD's openAPIV3Schema.
crd = yaml.safe_load(open(f"{root}/deploy/operator/crd.yaml"))
version = next(v for v in crd["spec"]["versions"] if v["name"] == "v1alpha1")
schema = version["schema"]["openAPIV3Schema"]

def strip(node):
    if isinstance(node, dict):
        out = {}
        for k, val in node.items():
            if k.startswith("x-kubernetes-"):
                continue
            if k == "nullable":
                continue
            out[k] = strip(val)
        if node.get("nullable") and "type" in out:
            out["type"] = [out["type"], "null"]
        return out
    if isinstance(node, list):
        return [strip(x) for x in node]
    return node

schema = strip(schema)
validator = jsonschema.Draft7Validator(schema)
for name in ("cl-default", "cl-big", "cl-img"):
    docs = load(f"{tmp}/{name}.yaml")
    assert kinds(docs) == ["AnimusCluster"], (name, kinds(docs))
    cr = docs[0]
    assert cr["apiVersion"] == "animusdb.io/v1alpha1"
    errs = sorted(validator.iter_errors({"spec": cr["spec"]}), key=str)
    assert not errs, f"{name}: spec does not satisfy the CRD schema: {[e.message for e in errs]}"
    # negative control: the validator must bite
    bad = copy.deepcopy(cr["spec"]); bad.pop("schemaVersion")
    assert list(validator.iter_errors({"spec": bad})), "schema validation did not reject a spec missing schemaVersion"

d = load(f"{tmp}/cl-default.yaml")[0]
assert d["metadata"]["name"] == "rel"
assert d["spec"]["image"] == "ghcr.io/animus-db/animusd:" + yaml.safe_load(open(f"{root}/deploy/helm/animus-cluster/Chart.yaml"))["appVersion"]
assert load(f"{tmp}/cl-img.yaml")[0]["metadata"]["name"] == "db"
assert load(f"{tmp}/cl-img.yaml")[0]["spec"]["image"] == "example.com/animusd:x"
print("ok")
PY
echo "helm charts: all checks passed"
