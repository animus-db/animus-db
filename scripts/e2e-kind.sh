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
# Issue #595: this smoke flaked twice with the identical signature — the
# first `CreateTable` (issued once, immediately after the statefulset
# reported 3/3 ready) failing with a 500 whose message is "CreateTable did
# not commit to the control plane in time (no leader reachable?)", the
# diagnostics dump showing one of the three pods `Ready: False` at that
# instant even though every pod had been ready moments earlier:
#   - run 10, https://github.com/animus-db/animus-db/actions/runs/33802730477
#     (main @ ec940df, 2026-09-03, the #573 merge) — pod e2e-1 not ready.
#   - run 13, https://github.com/animus-db/animus-db/actions/runs/33907718968
#     (PR #594 @ 1673f25, 2026-09-04) — pod e2e-0 not ready.
# The root cause (an ADR 0009 pre-vote follower's own `leader_id` clearing
# on a transient one-sided delay, which `/admin/health`'s readiness probe
# read raw) is fixed at the source in `animus-control`/`animusd::admin`
# (see ADR 0020's 2026-09-04 amendment and `docs/engineering-lessons.md`).
# This script carries two independent, complementary hardenings on top of
# that fix, per the issue's own "Ask" (root-cause the leader loss, AND
# treat the first post-bootstrap write as an eventual property): (1) an
# explicit readiness wait on the SAME pod the DynamoDB wire calls will hit,
# not just the statefulset's aggregate 3/3 count, before ever calling
# `CreateTable`; (2) a bounded converged-or-timeout retry of `CreateTable`
# itself, scoped narrowly to the one transient 500 this issue is about —
# every other error class still fails the run immediately, unchanged.
#
# Env overrides:
#   KIND_NODE_IMAGE  - `kind create cluster --image` value. Unset (CI default)
#                       lets kind pick its own pinned node image; locally,
#                       pass e.g. mirror.gcr.io/kindest/node:v1.34.0 when
#                       Docker Hub's blob CDN is unreachable.
#   ANIMUSD_IMAGE    - the animusd image tag to load into kind and run.
#                       Default: animusd:e2e (built separately, e.g. `docker
#                       build -t animusd:e2e .`).
#
# Exit non-zero on any failure; a trap dumps cluster/operator diagnostics and
# always tears down the kind cluster and background processes it started,
# whether the run passed or failed.

set -euo pipefail

ANIMUSD_IMAGE="${ANIMUSD_IMAGE:-animusd:e2e}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"

CLUSTER_NAME="animus-e2e"
NAMESPACE="animus-e2e"
AC_NAME="e2e"
DYNAMO_LOCAL_PORT="18100"
DYNAMO_REMOTE_PORT="14002" # base_port(14000) + PORT_DYNAMO(2), the CRD's own default base port.
ADMIN_LOCAL_PORT="18101"
ADMIN_REMOTE_PORT="14003" # base_port(14000) + PORT_ADMIN(3) — same numeric port on every
                           # pod (crates/animus-operator/src/desired/cluster_config.rs's own
                           # doc: unlike the local-dev `--cluster N` port stride, a k8s pod
                           # gets its own IP, so every pod binds the identical six ports).

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/animus-e2e-kind.XXXXXX")"
KIND_KUBECONFIG="${WORKDIR}/kubeconfig"
OPERATOR_LOG="${WORKDIR}/operator.log"
PORT_FORWARD_LOG="${WORKDIR}/port-forward.log"
MANIFEST_FILE="${WORKDIR}/animuscluster.yaml"

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

dynamo_endpoint_pod() {
    kubectl get endpoints "${AC_NAME}-dynamo" -n "$NAMESPACE" \
        -o jsonpath='{.subsets[0].addresses[0].targetRef.name}' 2>/dev/null || true
}

has_dynamo_endpoint() {
    [ -n "$(dynamo_endpoint_pod)" ]
}

port_forward_ready() {
    # Any real HTTP response (even a 4xx from an unrecognized bare GET)
    # proves the forwarded port is accepting and relaying connections;
    # curl exit 52 (empty reply, server accepted then closed) still proves
    # the same. Connection-refused (7) or timeout (28) means not yet.
    local rc=0
    curl -sS -o /dev/null -m 2 "http://127.0.0.1:${DYNAMO_LOCAL_PORT}/" >/dev/null 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || [ "$rc" -eq 52 ]
}

admin_port_forward_ready() {
    local rc=0
    curl -sS -o /dev/null -m 2 "http://127.0.0.1:${ADMIN_LOCAL_PORT}/" >/dev/null 2>&1 || rc=$?
    [ "$rc" -eq 0 ] || [ "$rc" -eq 52 ]
}

# Issue #595: the actual precondition `CreateTable` needs is not "the
# statefulset reported 3/3 ready a moment ago" (a point-in-time count that
# says nothing about the SPECIFIC pod the port-forward below will send the
# wire call to) — it is "that specific pod's own `/admin/health` is 200
# right now". Polled through the same pod-direct port-forward the dynamo
# call itself uses (see the "port-forward the serving pod directly" phase),
# so this checks the exact precondition, not a proxy for it.
admin_health_ready() {
    local code
    code="$(curl -sS -o /dev/null -m 2 -w '%{http_code}' "http://127.0.0.1:${ADMIN_LOCAL_PORT}/admin/health" 2>/dev/null)" || code=""
    [ "$code" = "200" ]
}

dynamo_call() {
    # dynamo_call TARGET BODY -> prints "STATUS\nRESPONSE_BODY"
    local target="$1" body="$2"
    curl -sS -w '\n%{http_code}' \
        -X POST "http://127.0.0.1:${DYNAMO_LOCAL_PORT}/" \
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

phase "resolve which pod svc/${AC_NAME}-dynamo currently routes to"
# `kubectl port-forward svc/...` resolves to exactly one backing pod for the
# life of the forward — read that same resolution off the Service's own
# Endpoints so the readiness check below and the actual wire calls are
# guaranteed to hit the SAME pod, not merely "a" ready pod (issue #595: a
# statefulset-wide 3/3 count says nothing about this one pod's own current
# state a few seconds later).
wait_for "svc/${AC_NAME}-dynamo has a resolved endpoint" 30 1 -- has_dynamo_endpoint
DYNAMO_POD="$(dynamo_endpoint_pod)"
[ -n "$DYNAMO_POD" ] || fail "could not resolve a pod backing svc/${AC_NAME}-dynamo"
log "svc/${AC_NAME}-dynamo currently routes to pod ${DYNAMO_POD}"

phase "port-forward that pod directly (dynamo + admin)"
# Forwarding the POD (not the Service) on both its dynamo and admin ports in
# one call is what lets the readiness check below and every subsequent
# dynamo_call in this script provably hit the identical pod — every pod
# binds the same numeric ports in the Kubernetes deployment shape (no
# per-pod port striping, unlike the local-dev `--cluster N` shape), so this
# is a straight substitution of `pod/${DYNAMO_POD}` for `svc/${AC_NAME}-dynamo`.
kubectl port-forward "pod/${DYNAMO_POD}" -n "$NAMESPACE" \
    "${DYNAMO_LOCAL_PORT}:${DYNAMO_REMOTE_PORT}" "${ADMIN_LOCAL_PORT}:${ADMIN_REMOTE_PORT}" \
    >"$PORT_FORWARD_LOG" 2>&1 &
PORT_FORWARD_PID=$!
wait_for "dynamo port-forward listening" 30 1 -- port_forward_ready
wait_for "admin port-forward listening" 30 1 -- admin_port_forward_ready

phase "wait for that pod's own readiness (GET /admin/health == 200)"
# Issue #595: the precondition the original one-shot CreateTable actually
# needed — this SPECIFIC pod (the one the dynamo wire calls below will hit)
# reports itself ready, not merely "the statefulset was 3/3 a moment ago".
# `/admin/health` itself now has hysteresis over a follower's own transient
# pre-vote `leader_id` clear (ADR 0020's 2026-09-04 amendment) — this wait
# is a second, independent line of defense on top of that root-cause fix,
# not a replacement for it.
wait_for "pod ${DYNAMO_POD}'s /admin/health is 200" 60 2 -- admin_health_ready

phase "exercise DynamoDB wire: CreateTable"
# Issue #595: a bounded converged-or-timeout retry, scoped narrowly to the
# one transient failure this issue is about (the control-plane commit-wait
# timing out right after bootstrap) — CreateTable is idempotent server-side
# (dynamo.rs's pre-check, ~2301-2333: a repeated CreateTable for a
# now-existing table returns the AWS-shaped ResourceInUseException, which
# this loop treats as success), so retrying is safe. Every OTHER error
# class (a validation failure, a genuinely reserved/duplicate name from a
# prior *different* run, ...) still fails the whole script immediately, on
# the very first attempt, exactly as before.
CREATE_TABLE_RETRYABLE="did not commit to the control plane in time"
CREATE_TABLE_TIMEOUT=60
CREATE_TABLE_INTERVAL=3
waited=0
while true; do
    RESULT="$(dynamo_call "DynamoDB_20120810.CreateTable" \
        '{"TableName":"E2EItems","AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}],"KeySchema":[{"AttributeName":"id","KeyType":"HASH"}]}')"
    STATUS="$(dynamo_status "$RESULT")"
    BODY="$(dynamo_body "$RESULT")"
    if [ "$STATUS" = "200" ]; then
        log "CreateTable ok"
        break
    fi
    if [ "$STATUS" = "400" ] && grep -q "ResourceInUseException" <<<"$BODY"; then
        log "CreateTable: the table already exists (a prior attempt's propose \
committed even though that attempt's own commit-wait timed out) — idempotent, treating as success"
        break
    fi
    if [ "$STATUS" = "500" ] && grep -qF "$CREATE_TABLE_RETRYABLE" <<<"$BODY"; then
        if [ "$waited" -ge "$CREATE_TABLE_TIMEOUT" ]; then
            fail "CreateTable kept timing out waiting on the control plane after ${waited}s: status=${STATUS} body=${BODY}"
        fi
        log "CreateTable: control-plane commit-wait timed out at ${waited}s — retrying (issue #595)"
        sleep "$CREATE_TABLE_INTERVAL"
        waited=$((waited + CREATE_TABLE_INTERVAL))
        continue
    fi
    fail "CreateTable failed: status=${STATUS} body=${BODY}"
done

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
