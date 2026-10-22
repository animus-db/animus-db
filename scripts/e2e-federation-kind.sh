#!/usr/bin/env bash
# Two-cluster MREC federation e2e for the animus-operator against a real
# `kind` cluster (ADR 0075 G-e, the "two-cluster kind e2e unwritten" gap).
#
# What it proves that no unit/sim/loopback test can: the operator's rendering
# of `spec.region` / `spec.peers` / `spec.tls` (cert-manager) into two real
# AnimusCluster resources whose pods dial each other's intra port over
# cluster DNS with mutual TLS across two DIFFERENT CAs (`caSecretRef`), and
# that a global table then replicates over the DynamoDB wire both ways,
# `PeerReachable` goes True, and `/admin/global-tables` names the replica.
#
# Topology (two namespaces in ONE kind cluster, not two kind clusters): a
# second kind cluster would need a routable path between the two docker
# networks' pod CIDRs (or a LoadBalancer per cluster), which kind does not
# provide; cluster DNS between two namespaces exercises the same code (the
# peer endpoint is an explicit host:port the operator renders into
# `cluster.json`, the dial is a real TLS connection to another StatefulSet's
# intra port, the trust anchors are separate per cluster). What it does NOT
# exercise: a real WAN, separate control planes/nodes, or cross-cluster
# NetworkPolicy behaviour with a CNI that enforces egress (kindnet may not).
# The WAN latency/bandwidth numbers come from the emulated-WAN real-process
# test (`crates/animusd/tests/mrec_wan_emulated.rs`), not from here.
#
# Flow: kind cluster -> CRD -> cert-manager -> TWO CA hierarchies (one per
# cluster) -> cross-copied CA Secrets (`peer-ca`) -> two AnimusClusters
# (`fed-a` in region-a, `fed-b` in region-b; nodes 3/controlNodes 3 each)
# -> out-of-cluster operator -> wait ready -> finalize each cluster to the
# MrecReplication gate (cluster version 3) -> CreateTable on A ->
# UpdateTable ReplicaUpdates Create region-b -> write on A / read on B and
# write on B / read on A -> PeerReachable=True on both -> /admin/global-tables
# -> delete both.
#
# Assumes: docker, kind, kubectl, curl, jq on PATH, a docker daemon, and the
# image `$ANIMUSD_IMAGE` (default animusd:e2e, `docker build -t animusd:e2e .`).
#
# Env overrides:
#   ANIMUSD_IMAGE    - animusd image to load into kind (default animusd:e2e).
#   KIND_NODE_IMAGE  - `kind create cluster --image` (unset = kind's default).
#   E2E_KEEP_CLUSTER - "1" keeps the kind cluster (operator/forwards stopped).
#
# UNVERIFIED locally: `kind` cannot come up in the authoring sandbox (no
# CAP_SYS_RESOURCE; see crates/animus-operator/CLAUDE.md, e2e section). This
# script is `bash -n` / shellcheck checked and reviewed against the E2E_TLS
# path of scripts/e2e-kind.sh, and has NEVER been run end to end. Treat a
# first CI failure of the `e2e-kind-federation` job as this script finding
# its first bug, not as a regression.

set -euo pipefail

ANIMUSD_IMAGE="${ANIMUSD_IMAGE:-animusd:e2e}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
E2E_KEEP_CLUSTER="${E2E_KEEP_CLUSTER:-0}"
CERT_MANAGER_VERSION="v1.16.2"
# Pinned like the upgrade leg's client (an unpinned third-party tag is a
# dependency on someone else's publishing decisions, issue #863).
CLIENT_IMAGE="${FED_CLIENT_IMAGE:-curlimages/curl:8.10.1}"

CLUSTER_NAME="animus-fed"
BOOTSTRAP_ISSUER="fed-selfsigned-bootstrap"
# Two clusters, each with its own namespace, region and CA hierarchy.
SIDES=(a b)
NS_a="animus-fed-a"
NS_b="animus-fed-b"
NAME_a="fed-a"
NAME_b="fed-b"
REGION_a="region-a"
REGION_b="region-b"
CA_CERT_a="fed-ca-a"
CA_CERT_b="fed-ca-b"
CA_SECRET_a="fed-ca-a-secret"
CA_SECRET_b="fed-ca-b-secret"
ISSUER_a="fed-ca-a-issuer"
ISSUER_b="fed-ca-b-issuer"
PEER_CA_SECRET="peer-ca"
NODES=3
# Same numbers as scripts/e2e-kind.sh (base port 14000 + PORT_*).
DYNAMO_PORT="14002"
ADMIN_PORT="14003"
INTRA_PORT="14004"
TABLE="FedItems"
CLIENT_POD="fed-client"
ADMIN_LOCAL_PORT="18301"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/animus-e2e-federation.XXXXXX")"
KIND_KUBECONFIG="${WORKDIR}/kubeconfig"
OPERATOR_LOG="${WORKDIR}/operator.log"
ADMIN_FWD_LOG="${WORKDIR}/admin-forward.log"
ADMIN_FWD_PID_FILE="${WORKDIR}/admin-forward.pid"

OPERATOR_PID=""
KIND_CLUSTER_UP="false"
PHASE="setup"

# Per-side lookups (bash 3 compatible: no associative arrays).
ns_of() { case "$1" in a) echo "$NS_a" ;; b) echo "$NS_b" ;; *) return 1 ;; esac; }
name_of() { case "$1" in a) echo "$NAME_a" ;; b) echo "$NAME_b" ;; *) return 1 ;; esac; }
region_of() { case "$1" in a) echo "$REGION_a" ;; b) echo "$REGION_b" ;; *) return 1 ;; esac; }
other_of() { case "$1" in a) echo b ;; b) echo a ;; *) return 1 ;; esac; }

log() {
    printf '[e2e-fed %s] %s\n' "$(date -u '+%H:%M:%S')" "$1" >&2
}

phase() {
    PHASE="$1"
    log "=== phase: $1 ==="
}

dump_diagnostics() {
    log "---- diagnostics ----"
    local side ns
    for side in "${SIDES[@]}"; do
        ns="$(ns_of "$side")"
        log "namespace ${ns}: animusclusters, statefulsets, pods, certificates"
        kubectl get animuscluster,statefulset,pods,certificate,networkpolicy -n "$ns" -o wide 2>&1 | sed 's/^/  /' || true
        kubectl get animuscluster -n "$ns" -o yaml 2>&1 | sed 's/^/  /' || true
        kubectl describe pods -n "$ns" 2>&1 | tail -n 150 | sed 's/^/  /' || true
        local pod
        for pod in $(kubectl get pods -n "$ns" -l "app.kubernetes.io/instance=$(name_of "$side")" \
            -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true); do
            log "logs ${ns}/${pod} (tail 120)"
            kubectl logs -n "$ns" "$pod" --tail=120 2>&1 | sed 's/^/  /' || true
        done
    done
    if [ -f "$OPERATOR_LOG" ]; then
        log "operator log (tail 400): ${OPERATOR_LOG}"
        tail -n 400 "$OPERATOR_LOG" 2>&1 | sed 's/^/  /' || true
    fi
    log "---- end diagnostics ----"
}

on_err() {
    local line="$1"
    log "FAILED at line ${line} during phase '${PHASE}'"
    if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        echo "::error title=e2e-federation failed (phase ${PHASE//[:,]/ })::command failed at scripts/e2e-federation-kind.sh line ${line}"
    fi
    dump_diagnostics
}

# A directed "we checked and it's wrong" failure skips `set -e`/the ERR trap
# via `exit`, so it dumps diagnostics itself (same contract as e2e-kind.sh).
fail() {
    log "FAILED (phase '${PHASE}'): $1"
    if [ "${GITHUB_ACTIONS:-}" = "true" ]; then
        local msg="${1//'%'/'%25'}"
        msg="${msg//$'\n'/'%0A'}"
        echo "::error title=e2e-federation failed (phase ${PHASE//[:,]/ })::${msg}"
    fi
    dump_diagnostics
    exit 1
}

stop_admin_forward() {
    if [ -f "$ADMIN_FWD_PID_FILE" ]; then
        local pid=""
        pid="$(cat "$ADMIN_FWD_PID_FILE" 2>/dev/null || true)"
        if [ -n "$pid" ]; then
            kill "$pid" >/dev/null 2>&1 || true
            wait "$pid" 2>/dev/null || true
        fi
        rm -f "$ADMIN_FWD_PID_FILE"
    fi
}

cleanup() {
    local status=$?
    stop_admin_forward
    pkill -f "port-forward pod/.* ${ADMIN_LOCAL_PORT}:${ADMIN_PORT}" >/dev/null 2>&1 || true
    if [ -n "$OPERATOR_PID" ]; then
        kill "$OPERATOR_PID" >/dev/null 2>&1 || true
        wait "$OPERATOR_PID" 2>/dev/null || true
        pkill -9 -f "target/[^ ]*/animus-operator run" >/dev/null 2>&1 || true
    fi
    if [ "$KIND_CLUSTER_UP" = "true" ] && [ "$E2E_KEEP_CLUSTER" = "1" ]; then
        log "E2E_KEEP_CLUSTER=1: keeping kind cluster ${CLUSTER_NAME}"
        log "  use it:    export KUBECONFIG=${KIND_KUBECONFIG}"
        log "  delete it: kind delete cluster --name ${CLUSTER_NAME}"
    elif [ "$KIND_CLUSTER_UP" = "true" ]; then
        log "deleting kind cluster ${CLUSTER_NAME}"
        KUBECONFIG="$KIND_KUBECONFIG" kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
    fi
    log "workdir preserved for inspection: ${WORKDIR}"
    if [ "$status" -eq 0 ]; then
        log "federation e2e PASSED"
    else
        log "federation e2e FAILED (exit ${status})"
    fi
    exit "$status"
}

trap 'on_err $LINENO' ERR
trap cleanup EXIT

wait_for() {
    # wait_for DESCRIPTION TIMEOUT_SECS INTERVAL_SECS -- CMD...
    local desc="$1" timeout_secs="$2" interval="$3"
    shift 3
    [ "$1" = "--" ] && shift
    local waited=0
    while true; do
        if "$@"; then
            log "${desc}: converged after ${waited}s"
            return 0
        fi
        if [ "$waited" -ge "$timeout_secs" ]; then
            log "${desc}: TIMED OUT after ${waited}s"
            return 1
        fi
        sleep "$interval"
        waited=$((waited + interval))
    done
}

sts_ready_equals() {
    # sts_ready_equals SIDE COUNT
    local ready
    ready="$(kubectl get statefulset "$(name_of "$1")" -n "$(ns_of "$1")" \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true)"
    [ "${ready:-0}" = "$2" ]
}

pod_fqdn() {
    # pod_fqdn SIDE ORDINAL: matches desired::pod_fqdn ({name}-{i}.{name}-internal.{ns}.svc.cluster.local).
    local name ns
    name="$(name_of "$1")"
    ns="$(ns_of "$1")"
    echo "${name}-${2}.${name}-internal.${ns}.svc.cluster.local"
}

dynamo_url() {
    # The client Service ({name}-dynamo); any pod forwards to the right leader.
    echo "https://$(name_of "$1")-dynamo.$(ns_of "$1").svc.cluster.local:${DYNAMO_PORT}/"
}

# In-cluster client: one curl pod per side, so the wire calls hit the real
# Service (no port-forward pinned to a pod that a roll could recycle). The
# dynamo port accepts any source (operator NetworkPolicy); the admin port does
# not, so admin calls below use `kubectl port-forward` like e2e-kind.sh does.
# `-k`: the client does not verify the server cert (it is signed by the
# per-cluster e2e CA, which the curl image does not trust); the TLS verification
# that matters here is the cluster-to-cluster mutual TLS under test.
dynamo_call() {
    # dynamo_call SIDE TARGET BODY -> prints "BODY\nSTATUS"
    local side="$1" target="$2" body="$3"
    kubectl exec -n "$(ns_of "$side")" "$CLIENT_POD" -- \
        curl -sS -k -m 20 -w '\n%{http_code}' \
        -X POST "$(dynamo_url "$side")" \
        -H "X-Amz-Target: DynamoDB_20120810.${target}" \
        -H "Content-Type: application/x-amz-json-1.0" \
        -d "$body"
}
dynamo_status() { tail -n1 <<<"$1"; }
dynamo_body() { sed '$d' <<<"$1"; }

# Run `curl` against one pod's admin port through a short-lived port-forward;
# prints "BODY\nSTATUS" (STATUS 000 when unreachable). Never aborts the script.
admin_call() {
    # admin_call SIDE ORDINAL METHOD PATH [JSON_BODY]
    local side="$1" ordinal="$2" method="$3" path="$4" body="${5:-}"
    local ns pod i out
    ns="$(ns_of "$side")"
    pod="$(name_of "$side")-${ordinal}"
    stop_admin_forward
    kubectl port-forward "pod/${pod}" -n "$ns" "${ADMIN_LOCAL_PORT}:${ADMIN_PORT}" \
        >"$ADMIN_FWD_LOG" 2>&1 &
    echo "$!" >"$ADMIN_FWD_PID_FILE"
    for i in $(seq 1 20); do
        if curl -sS -k -m 2 -o /dev/null "https://127.0.0.1:${ADMIN_LOCAL_PORT}/admin/health" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    if [ -n "$body" ]; then
        out="$(curl -sS -k -m 15 -w '\n%{http_code}' -X "$method" \
            -H "Content-Type: application/json" -d "$body" \
            "https://127.0.0.1:${ADMIN_LOCAL_PORT}${path}" 2>/dev/null)" || out=$'\n000'
    else
        out="$(curl -sS -k -m 15 -w '\n%{http_code}' -X "$method" \
            "https://127.0.0.1:${ADMIN_LOCAL_PORT}${path}" 2>/dev/null)" || out=$'\n000'
    fi
    stop_admin_forward
    printf '%s\n' "$out"
}

phase "preflight"
for bin in docker kind kubectl curl jq; do
    command -v "$bin" >/dev/null 2>&1 || fail "missing required tool: ${bin}"
done
log "repo root: ${REPO_ROOT}"
log "workdir: ${WORKDIR}"
log "ANIMUSD_IMAGE=${ANIMUSD_IMAGE} KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-<default>}"
docker image inspect "$ANIMUSD_IMAGE" >/dev/null 2>&1 ||
    fail "docker image ${ANIMUSD_IMAGE} not found locally — build it first (docker build -t animusd:e2e .)"

phase "kind cluster create"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
    log "a stale kind cluster named ${CLUSTER_NAME} already exists — deleting it first"
    kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
fi
KIND_CREATE_ARGS=(--name "$CLUSTER_NAME" --kubeconfig "$KIND_KUBECONFIG" --wait 120s)
if [ -n "$KIND_NODE_IMAGE" ]; then
    KIND_CREATE_ARGS+=(--image "$KIND_NODE_IMAGE")
fi
kind create cluster "${KIND_CREATE_ARGS[@]}"
KIND_CLUSTER_UP="true"
export KUBECONFIG="$KIND_KUBECONFIG"
kubectl cluster-info >/dev/null

phase "label the kind node(s) with a region/zone"
# animusd waits for the operator-resolved topology annotations at boot, so the
# node must carry labels (same as e2e-kind.sh). Both clusters share the one
# node here; this is not a placement test.
kubectl label node --all --overwrite \
    "topology.kubernetes.io/region=e2e-region" \
    "topology.kubernetes.io/zone=e2e-zone-a"

phase "load image"
kind load docker-image "$ANIMUSD_IMAGE" --name "$CLUSTER_NAME"

phase "apply CRD + namespaces"
kubectl apply -f "${REPO_ROOT}/deploy/operator/crd.yaml"
for side in "${SIDES[@]}"; do
    kubectl create namespace "$(ns_of "$side")" --dry-run=client -o yaml | kubectl apply -f -
done

phase "install cert-manager"
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
for deploy in cert-manager cert-manager-webhook cert-manager-cainjector; do
    kubectl -n cert-manager rollout status "deployment/${deploy}" --timeout=180s
done

phase "create TWO CA hierarchies, one per cluster (issue #913 shape, twice)"
# A bootstrap self-signed issuer mints one CA certificate per cluster; a
# per-cluster `ca:` ClusterIssuer then signs that cluster's leaf. The two CAs
# are deliberately different: each cluster trusts the other ONLY through
# `spec.peers[].caSecretRef`, so the peer-CA bundle path is what is tested.
kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ${BOOTSTRAP_ISSUER}
spec:
  selfSigned: {}
EOF
kubectl wait "clusterissuer/${BOOTSTRAP_ISSUER}" --for=condition=Ready --timeout=60s
for side in "${SIDES[@]}"; do
    case "$side" in
    a) ca_cert="$CA_CERT_a" ca_secret="$CA_SECRET_a" issuer="$ISSUER_a" ;;
    b) ca_cert="$CA_CERT_b" ca_secret="$CA_SECRET_b" issuer="$ISSUER_b" ;;
    esac
    kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: ${ca_cert}
  namespace: cert-manager
spec:
  isCA: true
  commonName: animus-fed-root-ca-${side}
  secretName: ${ca_secret}
  privateKey:
    algorithm: ECDSA
    size: 256
  issuerRef:
    name: ${BOOTSTRAP_ISSUER}
    kind: ClusterIssuer
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ${issuer}
spec:
  ca:
    secretName: ${ca_secret}
EOF
    kubectl -n cert-manager wait "certificate/${ca_cert}" --for=condition=Ready --timeout=60s
    kubectl wait "clusterissuer/${issuer}" --for=condition=Ready --timeout=60s
done

phase "copy each cluster's CA into the OTHER namespace as the peer-CA Secret"
# The operator only references this Secret (spec.peers[].caSecretRef); it never
# creates it. `tls.crt` of an isCA certificate off a self-signed issuer IS the
# CA certificate (see e2e-kind.sh's E2E_S3_TLS extraction).
for side in "${SIDES[@]}"; do
    other="$(other_of "$side")"
    case "$other" in
    a) other_secret="$CA_SECRET_a" ;;
    b) other_secret="$CA_SECRET_b" ;;
    esac
    ca_file="${WORKDIR}/ca-${other}.crt"
    kubectl -n cert-manager get secret "$other_secret" \
        -o jsonpath='{.data.tls\.crt}' | base64 -d >"$ca_file"
    grep -q "BEGIN CERTIFICATE" "$ca_file" ||
        fail "extracted CA for side ${other} does not look like a PEM certificate: ${ca_file}"
    kubectl create secret generic "$PEER_CA_SECRET" -n "$(ns_of "$side")" \
        --from-file=ca.crt="$ca_file" --dry-run=client -o yaml | kubectl apply -f -
done

phase "apply the two AnimusClusters (spec.region + spec.peers + spec.tls)"
# Peer endpoints: every pod's intra port of the OTHER cluster (any peer data
# node accepts a frame, so listing all keeps a rolling pod from breaking the
# link). The hosts are pod DNS names under the peer's headless Service; they
# are covered by the peer leaf's wildcard SAN `*.{name}-internal.{ns}.svc...`.
for side in "${SIDES[@]}"; do
    other="$(other_of "$side")"
    endpoints=""
    for ((i = 0; i < NODES; i++)); do
        endpoints="${endpoints}
        - $(pod_fqdn "$other" "$i"):${INTRA_PORT}"
    done
    case "$side" in
    a) issuer="$ISSUER_a" ;;
    b) issuer="$ISSUER_b" ;;
    esac
    cat >"${WORKDIR}/animuscluster-${side}.yaml" <<EOF
apiVersion: animusdb.io/v1alpha1
kind: AnimusCluster
metadata:
  name: $(name_of "$side")
  namespace: $(ns_of "$side")
spec:
  schemaVersion: 1
  image: ${ANIMUSD_IMAGE}
  nodes: ${NODES}
  controlNodes: ${NODES}
  region: $(region_of "$side")
  tls:
    certManager:
      issuerRef:
        name: ${issuer}
        kind: ClusterIssuer
  peers:
    - region: $(region_of "$other")
      endpoints:${endpoints}
      caSecretRef:
        name: ${PEER_CA_SECRET}
        key: ca.crt
EOF
    log "manifest for side ${side}:"
    sed 's/^/  /' "${WORKDIR}/animuscluster-${side}.yaml" >&2
    kubectl apply -f "${WORKDIR}/animuscluster-${side}.yaml"
done

phase "build operator (out-of-cluster)"
(
    cd "$REPO_ROOT"
    export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
    cargo build -p animus-operator --bin animus-operator
)
OPERATOR_BIN="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug/animus-operator"
[ -x "$OPERATOR_BIN" ] || fail "expected operator binary at ${OPERATOR_BIN} after the build"

phase "run operator out-of-cluster (reconciles both namespaces)"
(
    export KUBECONFIG="$KIND_KUBECONFIG"
    export RUST_LOG=info
    exec "$OPERATOR_BIN" run
) >"$OPERATOR_LOG" 2>&1 &
OPERATOR_PID=$!
log "operator running as PID ${OPERATOR_PID}, logging to ${OPERATOR_LOG}"

phase "wait for ${NODES}/${NODES} ready replicas on both clusters"
for side in "${SIDES[@]}"; do
    wait_for "side ${side}: statefulset readyReplicas==${NODES}" 420 5 -- sts_ready_equals "$side" "$NODES" ||
        fail "side ${side}: statefulset never reached ${NODES} ready replicas"
done
for side in "${SIDES[@]}"; do
    kubectl wait "certificate/$(name_of "$side")-tls" -n "$(ns_of "$side")" \
        --for=condition=Ready --timeout=120s
done

phase "start the in-cluster client pod on each side"
for side in "${SIDES[@]}"; do
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${CLIENT_POD}
  namespace: $(ns_of "$side")
spec:
  restartPolicy: Never
  containers:
    - name: curl
      image: ${CLIENT_IMAGE}
      command: ["sleep", "86400"]
EOF
    kubectl wait "pod/${CLIENT_POD}" -n "$(ns_of "$side")" --for=condition=Ready --timeout=180s
done

phase "wait for the version era and finalize both clusters to MrecReplication (cluster version 3)"
# Fresh clusters sit at cluster version 1; MREC replication needs version 3
# (animus_control::version::Gate::MrecReplication). The finalize POST is
# leader-only (409 elsewhere), so try every pod until one accepts; each step is
# retried (a transient refusal such as a blocker is not a failure).
active_version() {
    # active_version SIDE -> prints the first pod's cluster-version `active`, or nothing.
    local out
    out="$(admin_call "$1" 0 GET /admin/cluster-version)"
    [ "$(dynamo_status "$out")" = "200" ] || return 1
    jq -r 'select(.era_active == true) | .active // empty' <<<"$(dynamo_body "$out")"
}
finalize_step() {
    # finalize_step SIDE FROM TO
    local side="$1" from="$2" to="$3" i out
    for ((i = 0; i < NODES; i++)); do
        out="$(admin_call "$side" "$i" POST /admin/cluster-version/finalize \
            "{\"to\":${to},\"expected\":${from}}")"
        [ "$(dynamo_status "$out")" = "200" ] && return 0
    done
    return 1
}
cluster_at_version() {
    # cluster_at_version SIDE WANT
    local v
    v="$(active_version "$1" || true)"
    [ -n "$v" ] && [ "$v" -ge "$2" ]
}
for side in "${SIDES[@]}"; do
    wait_for "side ${side}: version era active" 180 5 -- cluster_at_version "$side" 1 ||
        fail "side ${side}: the cluster-version era never became active"
    for step in "1 2" "2 3"; do
        read -r from to <<<"$step"
        if cluster_at_version "$side" "$to"; then
            continue
        fi
        wait_for "side ${side}: finalize ${from} -> ${to}" 180 5 -- finalize_step "$side" "$from" "$to" ||
            fail "side ${side}: finalize ${from} -> ${to} was never accepted by a control leader"
    done
    wait_for "side ${side}: active cluster version >= 3" 120 5 -- cluster_at_version "$side" 3 ||
        fail "side ${side}: active cluster version never reached 3"
done

phase "CreateTable on A"
CREATE_BODY="{\"TableName\":\"${TABLE}\",\"KeySchema\":[{\"AttributeName\":\"pk\",\"KeyType\":\"HASH\"}],\"AttributeDefinitions\":[{\"AttributeName\":\"pk\",\"AttributeType\":\"S\"}],\"BillingMode\":\"PAY_PER_REQUEST\"}"
CREATE_STATUS=""
CREATE_RESP=""
create_table_a() {
    local result
    result="$(dynamo_call a CreateTable "$CREATE_BODY" 2>/dev/null)" || return 1
    CREATE_STATUS="$(dynamo_status "$result")"
    CREATE_RESP="$(dynamo_body "$result")"
    [ "$CREATE_STATUS" = "200" ]
}
# Eventual: the first control-plane write after bootstrap/finalize can briefly
# find no leader (issue #595), so converged-or-timeout, not one-shot.
wait_for "CreateTable on A" 120 5 -- create_table_a ||
    fail "CreateTable on A never succeeded: status=${CREATE_STATUS} body=${CREATE_RESP}"

phase "UpdateTable ReplicaUpdates Create ${REGION_b} on A"
RESULT="$(dynamo_call a UpdateTable \
    "{\"TableName\":\"${TABLE}\",\"ReplicaUpdates\":[{\"Create\":{\"RegionName\":\"${REGION_b}\"}}]}")"
[ "$(dynamo_status "$RESULT")" = "200" ] ||
    fail "UpdateTable Create replica failed: status=$(dynamo_status "$RESULT") body=$(dynamo_body "$RESULT")"

replicas_active_on_a() {
    local result
    result="$(dynamo_call a DescribeTable "{\"TableName\":\"${TABLE}\"}" 2>/dev/null)" || return 1
    [ "$(dynamo_status "$result")" = "200" ] || return 1
    jq -e '.Table.Replicas | (length == 2) and all(.[]; .ReplicaStatus == "ACTIVE")' \
        <<<"$(dynamo_body "$result")" >/dev/null
}
# The saga creates the peer table, copies existing rows and flips both sides
# ACTIVE; generous, it crosses real pod-to-pod mutual TLS between two CAs.
wait_for "both replicas ACTIVE on A" 300 5 -- replicas_active_on_a ||
    fail "replicas never both ACTIVE on A"

phase "write on A, read on B; write on B, read on A (ConsistentRead, over the DynamoDB wire)"
put_item() {
    # put_item SIDE KEY VALUE
    local result
    result="$(dynamo_call "$1" PutItem \
        "{\"TableName\":\"${TABLE}\",\"Item\":{\"pk\":{\"S\":\"$2\"},\"v\":{\"S\":\"$3\"}}}")"
    [ "$(dynamo_status "$result")" = "200" ] ||
        fail "PutItem $2 on side $1 failed: status=$(dynamo_status "$result") body=$(dynamo_body "$result")"
}
sees_item() {
    # sees_item SIDE KEY VALUE
    local result
    result="$(dynamo_call "$1" GetItem \
        "{\"TableName\":\"${TABLE}\",\"Key\":{\"pk\":{\"S\":\"$2\"}},\"ConsistentRead\":true}" 2>/dev/null)" || return 1
    [ "$(dynamo_status "$result")" = "200" ] || return 1
    [ "$(jq -r '.Item.v.S // empty' <<<"$(dynamo_body "$result")")" = "$3" ]
}
put_item a from-a "written-in-${REGION_a}"
put_item b from-b "written-in-${REGION_b}"
wait_for "A's write is readable on B" 120 3 -- sees_item b from-a "written-in-${REGION_a}" ||
    fail "the item written on A never became readable on B"
wait_for "B's write is readable on A" 120 3 -- sees_item a from-b "written-in-${REGION_b}" ||
    fail "the item written on B never became readable on A"
log "both directions replicate over mutual TLS between two CAs"

phase "PeerReachable=True on both AnimusClusters"
peer_reachable() {
    # peer_reachable SIDE
    [ "$(kubectl get animuscluster "$(name_of "$1")" -n "$(ns_of "$1")" \
        -o jsonpath='{.status.conditions[?(@.type=="PeerReachable")].status}' 2>/dev/null || true)" = "True" ]
}
for side in "${SIDES[@]}"; do
    # The condition is polled by the reconciler; it needs a shipper that has
    # acknowledged, which the writes above guarantee.
    wait_for "side ${side}: PeerReachable=True" 180 5 -- peer_reachable "$side" ||
        fail "side ${side}: PeerReachable never became True: $(kubectl get animuscluster "$(name_of "$side")" -n "$(ns_of "$side")" -o jsonpath='{.status.conditions}' 2>&1)"
done

phase "GET /admin/global-tables names the table and its replica"
# Shipper health is node-local (a node reports the tablets it leads), but the
# table row and replica status are cluster metadata, so any pod answers them.
global_tables_ok() {
    # global_tables_ok SIDE OTHER_REGION
    local out
    out="$(admin_call "$1" 0 GET /admin/global-tables)"
    [ "$(dynamo_status "$out")" = "200" ] || return 1
    jq -e --arg t "$TABLE" --arg r "$2" \
        '.tables[] | select(.table == $t) | (.consistency == "EVENTUAL") and (.replica_status[$r] == "ACTIVE")' \
        <<<"$(dynamo_body "$out")" >/dev/null
}
wait_for "side a: /admin/global-tables shows ${REGION_b} ACTIVE" 60 5 -- global_tables_ok a "$REGION_b" ||
    fail "side a: /admin/global-tables does not show ${REGION_b} ACTIVE"
wait_for "side b: /admin/global-tables shows ${REGION_a} ACTIVE" 60 5 -- global_tables_ok b "$REGION_a" ||
    fail "side b: /admin/global-tables does not show ${REGION_a} ACTIVE"

phase "delete both AnimusClusters and verify garbage collection"
for side in "${SIDES[@]}"; do
    kubectl delete animuscluster "$(name_of "$side")" -n "$(ns_of "$side")" --wait=true --timeout=180s
done
sts_gone() {
    ! kubectl get statefulset "$(name_of "$1")" -n "$(ns_of "$1")" >/dev/null 2>&1
}
for side in "${SIDES[@]}"; do
    wait_for "side ${side}: statefulset garbage collected" 180 5 -- sts_gone "$side" ||
        fail "side ${side}: statefulset was not garbage collected after deleting the AnimusCluster"
done
log "federation e2e: all checks passed"
