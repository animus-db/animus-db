#!/usr/bin/env bash
# End-to-end smoke test for the animus-operator + AnimusCluster CRD against a
# real `kind` cluster (ADR 0060). Not a substitute for `cargo test -p
# animus-operator`'s pure-builder unit suite (`deploy/operator/README.md`'s
# "what the operator does not do yet" list before this script existed) — this
# is the thing that suite structurally cannot prove: that the operator's YAML
# actually schedules a real 3-node AnimusDB cluster that bootstraps, serves
# the DynamoDB wire, and survives a scale-up.
#
# The operator itself runs OUT of the kind cluster for this smoke (`cargo run
# -p animus-operator -- run`, talking to the kind cluster's own API server via
# a scoped kubeconfig) — in-cluster deployment of the operator's own container
# image (`deploy/operator/deployment.yaml`) is exercised in production, not
# here; this script only proves the reconcile logic against a real API
# server + real kubelets/kube-controller-manager, which is the part no unit
# test can reach.
#
# Assumes: docker, kind, kubectl on PATH, and a docker daemon reachable.
#
# Env overrides:
#   KIND_NODE_IMAGE  - `kind create cluster --image` value. Unset (CI default)
#                       lets kind pick its own pinned node image; locally,
#                       pass e.g. mirror.gcr.io/kindest/node:v1.34.0 when
#                       Docker Hub's blob CDN is unreachable.
#   ANIMUSD_IMAGE    - the animusd image tag to load into kind and run.
#                       Default: animusd:e2e (built separately, e.g. `docker
#                       build -t animusd:e2e .`).
#   E2E_TLS          - "1" runs the ADR 0064 commit 3 TLS path instead of
#                       plain TCP: installs cert-manager, creates a
#                       self-signed ClusterIssuer, sets spec.tls.certManager
#                       on the AnimusCluster, waits for the Certificate to
#                       be issued, and drives the DynamoDB wire over
#                       `curl --cacert` instead of plain HTTP. Default "0"
#                       (unset) is the pre-existing plain-TCP path, byte-
#                       for-byte unchanged. UNVERIFIED in this sandbox: kind
#                       itself cannot come up here at all (see this repo's
#                       crates/animus-operator/CLAUDE.md e2e section, the
#                       CAP_SYS_RESOURCE note) — this path is new, careful,
#                       `bash -n`-checked code that has not been run end to
#                       end anywhere yet. Treat a first real CI failure here
#                       as "the TLS e2e found its first bug," not as this
#                       comment lying.
#
# Exit non-zero on any failure; a trap dumps cluster/operator diagnostics and
# always tears down the kind cluster and background processes it started,
# whether the run passed or failed.

set -euo pipefail

ANIMUSD_IMAGE="${ANIMUSD_IMAGE:-animusd:e2e}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
E2E_TLS="${E2E_TLS:-0}"
CERT_MANAGER_VERSION="v1.16.2"
CLUSTER_ISSUER_NAME="e2e-selfsigned"

CLUSTER_NAME="animus-e2e"
NAMESPACE="animus-e2e"
AC_NAME="e2e"
DYNAMO_LOCAL_PORT="18100"
DYNAMO_REMOTE_PORT="14002" # base_port(14000) + PORT_DYNAMO(2), the CRD's own default base port.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/animus-e2e-kind.XXXXXX")"
KIND_KUBECONFIG="${WORKDIR}/kubeconfig"
OPERATOR_LOG="${WORKDIR}/operator.log"
PORT_FORWARD_LOG="${WORKDIR}/port-forward.log"
MANIFEST_FILE="${WORKDIR}/animuscluster.yaml"
CA_FILE="${WORKDIR}/ca.crt"
# Every `curl` hitting the dynamo port, plain or TLS — kept as arrays so
# "no extra args" (plain path) and "--cacert ... --resolve ..." (TLS path)
# compose the same call sites without a second, near-duplicate function.
DYNAMO_SCHEME="http"
CURL_TLS_ARGS=()
DYNAMO_HOST="127.0.0.1"

OPERATOR_PID=""
PORT_FORWARD_PID=""
KIND_CLUSTER_UP="false"
PHASE="setup"

log() {
    printf '[e2e %s] %s\n' "$(date -u '+%H:%M:%S')" "$1" >&2
}

phase() {
    PHASE="$1"
    log "=== phase: $1 ==="
}

dump_diagnostics() {
    log "--- diagnostics (phase: ${PHASE}) ---"
    if [ "$KIND_CLUSTER_UP" = "true" ]; then
        export KUBECONFIG="$KIND_KUBECONFIG"
        log "kubectl get all -n ${NAMESPACE} -o wide"
        kubectl get all -n "$NAMESPACE" -o wide 2>&1 | sed 's/^/  /' || true
        log "kubectl get animuscluster -n ${NAMESPACE} -o yaml"
        kubectl get animuscluster -n "$NAMESPACE" -o yaml 2>&1 | sed 's/^/  /' || true
        log "kubectl describe statefulset ${AC_NAME} -n ${NAMESPACE}"
        kubectl describe statefulset "$AC_NAME" -n "$NAMESPACE" 2>&1 | sed 's/^/  /' || true
        log "kubectl describe pods -n ${NAMESPACE}"
        kubectl describe pods -n "$NAMESPACE" 2>&1 | sed 's/^/  /' || true
        log "pod logs (tail 100, per pod)"
        for pod in $(kubectl get pods -n "$NAMESPACE" -o name 2>/dev/null || true); do
            log "  logs: ${pod}"
            kubectl logs -n "$NAMESPACE" "$pod" --tail=100 2>&1 | sed 's/^/    /' || true
        done
    fi
    if [ -f "$OPERATOR_LOG" ]; then
        log "operator log (tail 200): ${OPERATOR_LOG}"
        tail -n 200 "$OPERATOR_LOG" 2>&1 | sed 's/^/  /' || true
    fi
    if [ -f "$PORT_FORWARD_LOG" ]; then
        log "port-forward log: ${PORT_FORWARD_LOG}"
        cat "$PORT_FORWARD_LOG" 2>&1 | sed 's/^/  /' || true
    fi
    log "--- end diagnostics ---"
}

on_err() {
    local line="$1"
    log "FAILED at line ${line} during phase '${PHASE}'"
    dump_diagnostics
}

# A directed, "we checked and it's wrong" failure (a non-200 status, a
# mismatched item) skips straight past `set -e`/the ERR trap via `exit`, so
# it must dump diagnostics itself before exiting — this is the one place
# every such check funnels through.
fail() {
    log "FAILED (phase '${PHASE}'): $1"
    dump_diagnostics
    exit 1
}

cleanup() {
    local status=$?
    if [ -n "$PORT_FORWARD_PID" ]; then
        kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
        wait "$PORT_FORWARD_PID" 2>/dev/null || true
    fi
    if [ -n "$OPERATOR_PID" ]; then
        kill "$OPERATOR_PID" >/dev/null 2>&1 || true
        wait "$OPERATOR_PID" 2>/dev/null || true
        # `cargo run` execs the built binary as a child process it should
        # forward signals to, but be belt-and-suspenders about a stray
        # survivor rather than leak a background `animus-operator run`.
        pkill -9 -f "target/[^ ]*/animus-operator run" >/dev/null 2>&1 || true
    fi
    if [ "$KIND_CLUSTER_UP" = "true" ]; then
        log "deleting kind cluster ${CLUSTER_NAME}"
        KUBECONFIG="$KIND_KUBECONFIG" kind delete cluster --name "$CLUSTER_NAME" >/dev/null 2>&1 || true
    fi
    log "workdir preserved for inspection: ${WORKDIR}"
    if [ "$status" -eq 0 ]; then
        log "e2e smoke PASSED"
    else
        log "e2e smoke FAILED (exit ${status})"
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

sts_ready_replicas() {
    kubectl get statefulset "$AC_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.status.readyReplicas}' 2>/dev/null || true
}

sts_ready_equals() {
    local want="$1"
    local got
    got="$(sts_ready_replicas)"
    [ -n "$got" ] && [ "$got" -eq "$want" ]
}

sts_gone() {
    ! kubectl get statefulset "$AC_NAME" -n "$NAMESPACE" >/dev/null 2>&1
}

port_forward_ready() {
    # Any real HTTP response (even a 4xx from an unrecognized bare GET)
    # proves the forwarded port is accepting and relaying connections;
    # curl exit 52 (empty reply, server accepted then closed) still proves
    # the same. Connection-refused (7) or timeout (28) means not yet. Over
    # TLS a handshake failure surfaces as a curl error too (35/60) — those
    # are real failures, not "not ready yet", but this check only needs to
    # know the port is listening at all, so it isn't split further.
    local rc=0
    curl -sS -o /dev/null -m 2 "${CURL_TLS_ARGS[@]}" \
        "${DYNAMO_SCHEME}://${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT}/" >/dev/null 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || [ "$rc" -eq 52 ]
}

dynamo_call() {
    # dynamo_call TARGET BODY -> prints "STATUS\nRESPONSE_BODY"
    local target="$1" body="$2"
    curl -sS -w '\n%{http_code}' "${CURL_TLS_ARGS[@]}" \
        -X POST "${DYNAMO_SCHEME}://${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT}/" \
        -H "X-Amz-Target: ${target}" \
        -H "Content-Type: application/x-amz-json-1.0" \
        -d "$body"
}

# dynamo_call's stdout is "<json body>\n<status code>"; split it.
dynamo_status() { tail -n1 <<<"$1"; }
dynamo_body() { sed '$d' <<<"$1"; }

phase "preflight"
for bin in docker kind kubectl curl jq; do
    command -v "$bin" >/dev/null 2>&1 || fail "missing required tool: ${bin}"
done
log "repo root: ${REPO_ROOT}"
log "workdir: ${WORKDIR}"
log "ANIMUSD_IMAGE=${ANIMUSD_IMAGE} KIND_NODE_IMAGE=${KIND_NODE_IMAGE:-<default>}"
docker image inspect "$ANIMUSD_IMAGE" >/dev/null 2>&1 ||
    fail "docker image ${ANIMUSD_IMAGE} not found locally — build it first (see script header)"

phase "kind cluster create"
# Idempotent local reruns: a stale same-named cluster from a prior failed
# run is deleted first rather than erroring out.
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

phase "load image"
kind load docker-image "$ANIMUSD_IMAGE" --name "$CLUSTER_NAME"

phase "apply CRD + namespace"
kubectl apply -f "${REPO_ROOT}/deploy/operator/crd.yaml"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -

TLS_SPEC_YAML=""
if [ "$E2E_TLS" = "1" ]; then
    phase "install cert-manager"
    kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
    for deploy in cert-manager cert-manager-webhook cert-manager-cainjector; do
        kubectl -n cert-manager rollout status "deployment/${deploy}" --timeout=180s
    done

    phase "create self-signed ClusterIssuer"
    # A self-signed root is the right, and only sane, choice for a
    # throwaway e2e cluster — no ACME account, no real CA, nothing to wait
    # on external to this kind cluster.
    kubectl apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ${CLUSTER_ISSUER_NAME}
spec:
  selfSigned: {}
EOF
    kubectl wait "clusterissuer/${CLUSTER_ISSUER_NAME}" --for=condition=Ready --timeout=60s

    TLS_SPEC_YAML="  tls:
    certManager:
      issuerRef:
        name: ${CLUSTER_ISSUER_NAME}
        kind: ClusterIssuer"
fi

phase "apply AnimusCluster"
cat >"$MANIFEST_FILE" <<EOF
apiVersion: animusdb.io/v1alpha1
kind: AnimusCluster
metadata:
  name: ${AC_NAME}
  namespace: ${NAMESPACE}
spec:
  image: ${ANIMUSD_IMAGE}
  nodes: 3
  controlNodes: 3
  storage:
    ephemeral: true
${TLS_SPEC_YAML}
EOF
kubectl apply -f "$MANIFEST_FILE"
kubectl get animuscluster "$AC_NAME" -n "$NAMESPACE" -o wide

phase "run operator out-of-cluster"
(
    cd "$REPO_ROOT"
    # A caller-provided CARGO_TARGET_DIR is respected; otherwise cargo's own
    # default applies. Incremental compilation and debuginfo are off — a
    # smoke run never reuses this build, so smaller/faster wins.
    export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
    export KUBECONFIG="$KIND_KUBECONFIG"
    exec cargo run -p animus-operator -- run
) >"$OPERATOR_LOG" 2>&1 &
OPERATOR_PID=$!
log "operator running as PID ${OPERATOR_PID}, logging to ${OPERATOR_LOG}"

phase "wait for 3/3 ready replicas"
wait_for "statefulset readyReplicas==3" 300 5 -- sts_ready_equals 3

if [ "$E2E_TLS" = "1" ]; then
    phase "wait for the cert-manager Certificate to be issued"
    kubectl wait "certificate/${AC_NAME}-tls" -n "$NAMESPACE" \
        --for=condition=Ready --timeout=120s

    phase "extract the cluster CA for curl"
    kubectl get secret "${AC_NAME}-tls" -n "$NAMESPACE" \
        -o jsonpath='{.data.ca\.crt}' | base64 -d >"$CA_FILE"
    [ -s "$CA_FILE" ] || fail "extracted CA file is empty: ${CA_FILE}"

    # The dynamo Service's cluster-DNS name is one of the Certificate's own
    # SANs (crate::desired::certificate::dns_names), so TLS hostname
    # verification passes when curl is told (via --resolve) to dial that
    # name at the locally-forwarded port instead of the port-forward's own
    # 127.0.0.1 — the port-forward still tunnels the actual bytes to
    # 127.0.0.1:${DYNAMO_LOCAL_PORT}, this only changes what curl verifies
    # the presented certificate against.
    DYNAMO_SCHEME="https"
    DYNAMO_HOST="${AC_NAME}-dynamo.${NAMESPACE}.svc.cluster.local"
    CURL_TLS_ARGS=(--cacert "$CA_FILE" --resolve "${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT}:127.0.0.1")
    log "TLS e2e path: curl will dial https://${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT} (--cacert ${CA_FILE})"
fi

phase "port-forward dynamo service"
kubectl port-forward "svc/${AC_NAME}-dynamo" -n "$NAMESPACE" \
    "${DYNAMO_LOCAL_PORT}:${DYNAMO_REMOTE_PORT}" >"$PORT_FORWARD_LOG" 2>&1 &
PORT_FORWARD_PID=$!
wait_for "port-forward listening" 30 1 -- port_forward_ready

phase "exercise DynamoDB wire: CreateTable"
RESULT="$(dynamo_call "DynamoDB_20120810.CreateTable" \
    '{"TableName":"E2EItems","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "CreateTable failed: status=${STATUS} body=${BODY}"
log "CreateTable ok"

phase "exercise DynamoDB wire: PutItem"
RESULT="$(dynamo_call "DynamoDB_20120810.PutItem" \
    '{"TableName":"E2EItems","Item":{"id":{"S":"widget-1"},"note":{"S":"hello from e2e"}}}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "PutItem failed: status=${STATUS} body=${BODY}"
log "PutItem ok"

phase "exercise DynamoDB wire: GetItem"
RESULT="$(dynamo_call "DynamoDB_20120810.GetItem" \
    '{"TableName":"E2EItems","Key":{"id":{"S":"widget-1"}},"ConsistentRead":true}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "GetItem failed: status=${STATUS} body=${BODY}"
NOTE="$(jq -r '.Item.note.S // empty' <<<"$BODY")"
[ "$NOTE" = "hello from e2e" ] || fail "GetItem did not round-trip the item: ${BODY}"
log "GetItem ok — item round-tripped"

phase "scale AnimusCluster to 4 nodes"
kubectl patch animuscluster "$AC_NAME" -n "$NAMESPACE" --type=merge -p '{"spec":{"nodes":4}}'
wait_for "statefulset readyReplicas==4" 300 5 -- sts_ready_equals 4

phase "GetItem still returns the item after scale-up"
RESULT="$(dynamo_call "DynamoDB_20120810.GetItem" \
    '{"TableName":"E2EItems","Key":{"id":{"S":"widget-1"}},"ConsistentRead":true}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "post-scale GetItem failed: status=${STATUS} body=${BODY}"
NOTE="$(jq -r '.Item.note.S // empty' <<<"$BODY")"
[ "$NOTE" = "hello from e2e" ] || fail "post-scale GetItem did not round-trip the item: ${BODY}"
log "post-scale GetItem ok"

phase "delete AnimusCluster and verify GC"
kubectl delete animuscluster "$AC_NAME" -n "$NAMESPACE"
wait_for "statefulset garbage-collected" 120 3 -- sts_gone

phase "done"
log "all phases passed"
