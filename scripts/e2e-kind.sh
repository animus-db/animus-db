#!/usr/bin/env bash
# End-to-end smoke test for the animus-operator + AnimusCluster CRD against a
# real `kind` cluster (ADR 0060). Not a substitute for `cargo test -p
# animus-operator`'s pure-builder unit suite (`deploy/operator/README.md`'s
# "what the operator does not do yet" list before this script existed) — this
# is the thing that suite structurally cannot prove: that the operator's YAML
# actually schedules a real 3-node AnimusDB cluster that bootstraps, serves
# the DynamoDB wire, and survives a scale-up.
#
# The operator itself runs OUT of the kind cluster for this smoke (built with
# `cargo build -p animus-operator`, then run directly — `animus-operator
# run` — against the kind cluster's own API server via a scoped kubeconfig)
# — in-cluster deployment of the operator's own container
# image (`deploy/operator/deployment.yaml`) is exercised in production, not
# here; this script only proves the reconcile logic against a real API
# server + real kubelets/kube-controller-manager, which is the part no unit
# test can reach.
#
# Assumes: docker, kind, kubectl on PATH, and a docker daemon reachable.
#
# S-07b: the AnimusCluster manifest always sets `spec.segmentStore:
# dir:<data-mount>/segments` (the non-S3 store CRD surface), and the plain
# path checks `GET /admin/segment-store` reports `store.kind: "fs"` right
# after the pod readiness wait — no separate CI job, no gating env var,
# since a `dir:` path needs no external dependency the way MinIO (E2E_S3)
# or cert-manager (E2E_TLS) do. `segmentStore`, not `backupStore`, so this
# composes with the pre-existing E2E_S3=1 leg below (which sets
# `spec.s3.backupStore` — the two backup-store fields together would be a
# rejected conflict).
#
# S-07c: the operator applies a `{name}-pdb` PodDisruptionBudget for every
# cluster now (`crate::desired::poddisruptionbudget`), with `maxUnavailable`
# derived from `nodes`/`controlNodes` rather than a constant. This manifest's
# own 3-node/3-controlNodes shape computes exactly 1
# (floor((3-1)/2) for both the control-plane and RF-capped data-plane
# terms) — checked once right after the initial 3/3-ready wait, and again
# after the scale-up to 4 nodes below (pinning that the value is
# scale-invariant once nodes/controlNodes each reach the replication
# factor, see ADR 0060's own "Amendment (2026-09-06): S-07c" section).
#
# S-07d: right after the nodes:3->4 scale-up, `spec.controlNodes` is grown
# 3 -> 4 too (promoting the already-Ready ordinal-3 pod the scale-up just
# added into a real control voter via the operator's own ADR 0037
# `control/member/add` automation), polling `GET /admin/control/members`
# for the new voter count, re-checking the PDB, and re-resolving/re-
# forwarding the serving pod (the config-hash-triggered rolling restart may
# have recycled it) before a final GetItem proves the wire still serves.
# See ADR 0060's own "Control-voter growth (S-07d, 2026-09-06)" section.
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
#   E2E_S3           - "1" adds an S-04 PR 3 leg on top of the plain-TCP
#                       path (mutually independent of E2E_TLS — either, both,
#                       or neither may be set): deploys a single-pod MinIO
#                       (the well-known `minio/minio` image) + Service into
#                       the kind cluster, creates its bucket via a throwaway
#                       `minio/mc` pod, creates the `access_key_id`/
#                       `secret_access_key` credentials Secret
#                       `spec.s3.credentialsSecretName` names, applies the
#                       AnimusCluster with `spec.s3.backupStore` pointing at
#                       `http://minio.<ns>.svc:9000` (`allowInsecureHttp:
#                       true` — a loopback-to-the-cluster MinIO dev target,
#                       never a real deployment shape), then exercises
#                       `CreateBackup`/`DescribeBackup` over the DynamoDB
#                       wire and checks `GET /admin/backup-store` reports
#                       `"kind":"s3"`. Default "0" (unset) leaves the smoke
#                       byte-for-byte unchanged. UNVERIFIED in this sandbox,
#                       same `CAP_SYS_RESOURCE` reason `E2E_TLS` is above —
#                       written carefully and `bash -n`-checked, never run
#                       end to end anywhere; treat a first real CI failure
#                       on the `e2e-kind-s3` job as this leg finding its
#                       first real bug.
#   E2E_ENCRYPTION   - "1" adds an ADR 0069 S-03 PR 3 leg on top of the
#                       plain-TCP path (mutually independent of E2E_TLS/
#                       E2E_S3 — any combination may be set): creates a
#                       `Secret` holding a freshly generated 64-hex-character
#                       key under the operator's one well-known data key
#                       (`crate::desired::cluster_config::
#                       ENCRYPTION_KEY_SECRET_DATA_KEY`, `"key"`), sets
#                       `spec.encryptionKeySecretName` on the AnimusCluster,
#                       then — after the ordinary PutItem below — execs into
#                       the serving pod and greps its own data directory
#                       recursively for the plaintext item value written
#                       earlier (must be ABSENT) and checks `GET
#                       /admin/config` never contains the raw key hex either
#                       (this operator never puts key material in the
#                       generated `ConfigMap`, so this is a belt-and-
#                       suspenders proof, not a documented risk). Default "0"
#                       (unset) leaves the smoke byte-for-byte unchanged.
#                       UNVERIFIED in this sandbox, same `CAP_SYS_RESOURCE`
#                       reason `E2E_TLS`/`E2E_S3` are above — written
#                       carefully and `bash -n`-checked, never run end to end
#                       anywhere; treat a first real CI failure on the
#                       `e2e-kind-encryption` job as this leg finding its
#                       first real bug.
#   E2E_WEBHOOK      - "1" adds an S-07e (ADR 0070) leg on top of the
#                       plain-TCP path (mutually independent of E2E_TLS/
#                       E2E_S3/E2E_ENCRYPTION — any combination may be set):
#                       proves the validating admission webhook actually
#                       rejects a bad AnimusCluster write at the API server,
#                       not merely as a reconciler-side status condition.
#                       Unlike every other leg above, this one needs the
#                       operator running IN-CLUSTER — the API server must be
#                       able to dial the webhook, which an out-of-cluster
#                       `cargo run` process (what every leg, this one
#                       included, still uses for the ordinary reconcile
#                       loop) structurally cannot serve. Rather than moving
#                       the WHOLE operator in-cluster (a materially larger
#                       change — real RBAC/ServiceAccount wiring against a
#                       live API server, and a second controller instance
#                       racing the existing out-of-cluster one over the same
#                       objects), this leg deploys a SECOND, minimal
#                       in-cluster Deployment running `--webhook-only`
#                       (main.rs's own opt-in mode: no reconcile loop, no
#                       Kubernetes client ever built at all — `validate_spec`
#                       is pure, so the webhook itself never touches the
#                       API) — see `crates/animus-operator/CLAUDE.md`'s e2e
#                       section for why this is the deliberately smaller,
#                       honest scope for this PR rather than a full
#                       in-cluster reconciler. Builds the `animus-operator`
#                       image from the same Dockerfile the ANIMUSD_IMAGE
#                       build already warmed (`docker build --target
#                       runtime-operator`, BuildKit cache-mount-shared with
#                       the animusd build above — see the Dockerfile's own
#                       "single cache-mounted compile" comment), generates a
#                       self-signed webhook cert via `openssl` (the
#                       hand-issued-`Secret` path `deploy/operator/
#                       README.md` documents — no cert-manager dependency
#                       for this leg, independent of whatever E2E_TLS did),
#                       deploys it plus a `Service` and a
#                       `ValidatingWebhookConfiguration` scoped to this
#                       leg's own namespace via `namespaceSelector` (so a
#                       webhook outage here can't affect anything outside
#                       this smoke's own objects), then asserts an invalid
#                       `spec.controlNodes` decrease is rejected by the API
#                       server itself (not merely surfaced as a status
#                       condition — this is the property no `cargo test`
#                       run can prove) and a valid edit is still admitted.
#                       Default "0" (unset) leaves the smoke byte-for-byte
#                       unchanged. UNVERIFIED in this sandbox, same
#                       `CAP_SYS_RESOURCE` reason every other leg above is —
#                       written carefully and `bash -n`-checked, never run
#                       end to end anywhere; treat a first real CI failure
#                       on the `e2e-kind-webhook` job as this leg finding
#                       its first real bug.
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
E2E_S3="${E2E_S3:-0}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"
# Throwaway kind-cluster-local credentials — never anything real, and never
# reused outside this one ephemeral cluster's lifetime.
MINIO_ACCESS_KEY="e2eaccesskey"
MINIO_SECRET_KEY="e2esecretkey123"
S3_BUCKET="e2e-backups"
S3_CREDS_SECRET_NAME="e2e-s3-creds"
E2E_ENCRYPTION="${E2E_ENCRYPTION:-0}"
ENCRYPTION_KEY_SECRET_NAME="e2e-encryption-key"
E2E_WEBHOOK="${E2E_WEBHOOK:-0}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-animus-operator:e2e}"
WEBHOOK_DEPLOYMENT_NAME="e2e-operator-webhook"
WEBHOOK_SERVICE_NAME="e2e-operator-webhook"
WEBHOOK_SECRET_NAME="e2e-operator-webhook-tls"
WEBHOOK_CONFIG_NAME="e2e-animuscluster-validating-webhook"

CLUSTER_NAME="animus-e2e"
NAMESPACE="animus-e2e"
AC_NAME="e2e"
# S-07b: the pod's own data volume mount path — must match
# crates/animus-operator/src/desired/cluster_config.rs's DATA_DIR constant.
# spec.segmentStore's dir:<path> below is required (AnimusClusterSpec::
# validate_store_spec) to live under this exact prefix.
DATA_MOUNT_DIR="/var/lib/animus"
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
CA_FILE="${WORKDIR}/ca.crt"
# Every `curl` hitting the dynamo OR the admin port, plain or TLS — kept as
# arrays so "no extra args" (plain path) and "--cacert ... --resolve ..."
# (TLS path) compose the same call sites without a second, near-duplicate
# function. `spec.tls` turns TLS on for every port this cluster binds, not
# just dynamo (ADR 0064) — the admin-port readiness probes below
# (`admin_port_forward_ready`/`admin_health_ready`) need the identical
# treatment, or they'd TLS-handshake-fail against a server-only-TLS admin
# port under `E2E_TLS=1`. `ADMIN_HOST` reuses the same hostname as
# `DYNAMO_HOST` (a name already covered by the issued cert's SAN list,
# `desired::certificate::dns_names`) — SAN coverage doesn't depend on which
# port that name is dialed on, only `--resolve` naming a different port.
DYNAMO_SCHEME="http"
ADMIN_SCHEME="http"
CURL_TLS_ARGS=()
DYNAMO_HOST="127.0.0.1"
ADMIN_HOST="127.0.0.1"

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
        # `$OPERATOR_PID` is the already-built binary's own PID directly
        # (execed in place of a `cargo run` supervisor — see the "run
        # operator out-of-cluster" phase's own doc), so the `kill` above
        # already reaches the real process; kept as belt-and-suspenders
        # against a stray survivor rather than leak a background
        # `animus-operator run`.
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

# Issue #704: the webhook Service's own Endpoints (or EndpointSlice — the
# addresses are the same set either way; Endpoints is the older, simpler
# API and this cluster's version has both) becoming non-empty is the
# earliest observable signal that a webhook Service is actually routable —
# a Deployment reporting Ready only proves the pod passed its (absent, for
# --webhook-only) readiness probe, not that kube-proxy has programmed the
# Service's ClusterIP rule yet. Used by the S-07e leg below.
webhook_endpoint_ready() {
    local ips
    ips="$(kubectl get endpoints "$WEBHOOK_SERVICE_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null || true)"
    [ -n "$ips" ]
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

admin_port_forward_ready() {
    local rc=0
    curl -sS -o /dev/null -m 2 "${CURL_TLS_ARGS[@]}" \
        "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/" >/dev/null 2>&1 || rc=$?
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
    code="$(curl -sS -o /dev/null -m 2 -w '%{http_code}' "${CURL_TLS_ARGS[@]}" \
        "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/admin/health" 2>/dev/null)" || code=""
    [ "$code" = "200" ]
}

# S-07d: the number of control voters the group itself currently reports —
# `GET /admin/control/members` is served by any node (`admin.rs`'s own
# doc), so this can be polled through whichever pod the dynamo port-forward
# currently targets, control-role or not.
control_voters_count() {
    curl -sS -m 5 "${CURL_TLS_ARGS[@]}" \
        "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/admin/control/members" 2>/dev/null \
        | jq -r '(.voters // []) | length' 2>/dev/null || echo 0
}

control_voters_equals() {
    local want="$1" got
    got="$(control_voters_count)"
    [ -n "$got" ] && [ "$got" -eq "$want" ]
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
if [ "$E2E_ENCRYPTION" = "1" ] || [ "$E2E_WEBHOOK" = "1" ]; then
    command -v openssl >/dev/null 2>&1 ||
        fail "missing required tool: openssl (needed by E2E_ENCRYPTION=1/E2E_WEBHOOK=1)"
fi
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

S3_SPEC_YAML=""
if [ "$E2E_S3" = "1" ]; then
    phase "deploy MinIO (S-04 PR 3)"
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels: {app: minio}
  template:
    metadata:
      labels: {app: minio}
    spec:
      containers:
        - name: minio
          image: ${MINIO_IMAGE}
          args: ["server", "/data"]
          env:
            - name: MINIO_ROOT_USER
              value: "${MINIO_ACCESS_KEY}"
            - name: MINIO_ROOT_PASSWORD
              value: "${MINIO_SECRET_KEY}"
          ports:
            - containerPort: 9000
          readinessProbe:
            httpGet: {path: /minio/health/ready, port: 9000}
            periodSeconds: 2
            failureThreshold: 30
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: ${NAMESPACE}
spec:
  selector: {app: minio}
  ports:
    - port: 9000
      targetPort: 9000
EOF
    kubectl -n "$NAMESPACE" rollout status deployment/minio --timeout=120s

    phase "create the MinIO bucket"
    # A throwaway in-cluster `minio/mc` pod is the simplest way to reach the
    # ClusterIP Service without a port-forward of its own — real S3/MinIO
    # never auto-creates a bucket on first PUT, so this has to happen before
    # any backup capture can succeed.
    kubectl run mc-mb --rm -i --restart=Never -n "$NAMESPACE" \
        --image="$MINIO_MC_IMAGE" --command -- \
        sh -c "mc alias set local http://minio.${NAMESPACE}.svc:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} && mc mb local/${S3_BUCKET}"

    phase "create the S3 credentials Secret"
    # access_key_id/secret_access_key are the two keys crate::desired::
    # statefulset::build mounts at /etc/animus/s3 and entrypoint.sh reads at
    # container-start time (crate::desired::cluster_config::
    # entrypoint_script) — never written into the ConfigMap/cluster.json.
    kubectl create secret generic "$S3_CREDS_SECRET_NAME" -n "$NAMESPACE" \
        --from-literal=access_key_id="$MINIO_ACCESS_KEY" \
        --from-literal=secret_access_key="$MINIO_SECRET_KEY" \
        --dry-run=client -o yaml | kubectl apply -f -

    S3_SPEC_YAML="  s3:
    backupStore: \"s3://${S3_BUCKET}?endpoint=http://minio.${NAMESPACE}.svc:9000&insecure_http=true\"
    credentialsSecretName: ${S3_CREDS_SECRET_NAME}
    allowInsecureHttp: true"
fi

ENCRYPTION_SPEC_YAML=""
if [ "$E2E_ENCRYPTION" = "1" ]; then
    phase "create the encryption key Secret (ADR 0069, S-03 PR 3)"
    # A fresh 64-hex-character key, the exact format `animus_env::
    # EncryptionKey::load_from_file` parses — this operator never generates
    # or inspects the key itself, only mounts whatever's under the one
    # well-known data key `crate::desired::cluster_config::
    # ENCRYPTION_KEY_SECRET_DATA_KEY` ("key") names. Never logged or echoed
    # anywhere below.
    ENCRYPTION_KEY_HEX="$(openssl rand -hex 32)"
    kubectl create secret generic "$ENCRYPTION_KEY_SECRET_NAME" -n "$NAMESPACE" \
        --from-literal=key="$ENCRYPTION_KEY_HEX" \
        --dry-run=client -o yaml | kubectl apply -f -

    ENCRYPTION_SPEC_YAML="  encryptionKeySecretName: ${ENCRYPTION_KEY_SECRET_NAME}"
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
  # S-07b: the non-S3 store CRD surface, exercised unconditionally (not
  # gated on E2E_S3) — segmentStore rather than backupStore specifically so
  # this composes with the E2E_S3=1 leg below, which already sets
  # spec.s3.backupStore (spec.backupStore/spec.s3.backupStore both set is a
  # rejected conflict; segmentStore has no such overlap here).
  segmentStore: "dir:${DATA_MOUNT_DIR}/segments"
${TLS_SPEC_YAML}
${S3_SPEC_YAML}
${ENCRYPTION_SPEC_YAML}
EOF
kubectl apply -f "$MANIFEST_FILE"
kubectl get animuscluster "$AC_NAME" -n "$NAMESPACE" -o wide

phase "build operator (out-of-cluster)"
(
    cd "$REPO_ROOT"
    # A caller-provided CARGO_TARGET_DIR is respected; otherwise cargo's own
    # default applies. Incremental compilation and debuginfo are off — a
    # smoke run never reuses this build, so smaller/faster wins.
    export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0
    cargo build -p animus-operator --bin animus-operator
)
# Resolve the just-built binary's path the same way cargo itself would
# (`$CARGO_TARGET_DIR/debug/animus-operator`, or `target/debug/animus-operator`
# under the repo root when that env var is unset — cargo's own default).
OPERATOR_BIN="${CARGO_TARGET_DIR:-$REPO_ROOT/target}/debug/animus-operator"
[ -x "$OPERATOR_BIN" ] || {
    log "FATAL: expected operator binary at ${OPERATOR_BIN} after the build above"
    exit 1
}

phase "run operator out-of-cluster"
# Built synchronously above (not `cargo run` backgrounded directly): kube-rs's
# own dependency tree (rustls/hyper/k8s-openapi and friends) is compiled here
# for the very first time in this script, nothing else warms the cache first,
# and a cold build of it is easily minutes long. `cargo run` in the
# background used to fold that entire compile into `$OPERATOR_LOG` itself —
# every reconcile/growth `info!`/`warn!` this script's diagnostics rely on
# ("operator log (tail 200): ...") could genuinely not have been logged yet
# by the time anything went looking, making a merely-still-compiling process
# indistinguishable from a stuck one. Building first, then execing the
# already-compiled binary directly, means every line that ever lands in
# `$OPERATOR_LOG` is real runtime tracing output, and — as a side benefit —
# removes the `cargo run` supervisor indirection `cleanup()`'s own comment
# above already had to work around with a belt-and-suspenders `pkill`.
(
    export KUBECONFIG="$KIND_KUBECONFIG"
    # `animus-operator run`'s `tracing_subscriber::fmt::init()` uses
    # `EnvFilter::from_default_env()`, which defaults to ERROR-only when
    # `RUST_LOG` is unset — every `reconcile`/growth `info!`/`warn!` this
    # script's own diagnostics rely on was silently dropped, making a stuck
    # reconcile indistinguishable from a hung process. `RUST_LOG=info`
    # surfaces both without `kube`/`hyper`'s own `debug`-level noise. (This
    # alone does not make the log useful if the operator hasn't started
    # running yet — see the "build first" comment above.)
    export RUST_LOG=info
    exec "$OPERATOR_BIN" run
) >"$OPERATOR_LOG" 2>&1 &
OPERATOR_PID=$!
log "operator running as PID ${OPERATOR_PID}, logging to ${OPERATOR_LOG}"

phase "wait for 3/3 ready replicas"
wait_for "statefulset readyReplicas==3" 300 5 -- sts_ready_equals 3

phase "check the quorum-derived PodDisruptionBudget (S-07c)"
PDB_MAX_UNAVAIL="$(kubectl get pdb "${AC_NAME}-pdb" -n "$NAMESPACE" \
    -o jsonpath='{.spec.maxUnavailable}' 2>/dev/null || true)"
[ "$PDB_MAX_UNAVAIL" = "1" ] || fail "expected PodDisruptionBudget ${AC_NAME}-pdb maxUnavailable=1 \
for nodes=3/controlNodes=3, got ${PDB_MAX_UNAVAIL:-<empty>}"
log "PodDisruptionBudget ${AC_NAME}-pdb reports maxUnavailable=1"

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
    ADMIN_SCHEME="https"
    DYNAMO_HOST="${AC_NAME}-dynamo.${NAMESPACE}.svc.cluster.local"
    ADMIN_HOST="$DYNAMO_HOST"
    CURL_TLS_ARGS=(
        --cacert "$CA_FILE"
        --resolve "${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT}:127.0.0.1"
        --resolve "${ADMIN_HOST}:${ADMIN_LOCAL_PORT}:127.0.0.1"
    )
    log "TLS e2e path: curl will dial https://${DYNAMO_HOST}:${DYNAMO_LOCAL_PORT} (dynamo) and https://${ADMIN_HOST}:${ADMIN_LOCAL_PORT} (admin) (--cacert ${CA_FILE})"
fi

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

phase "check GET /admin/segment-store reports the S-07b dir: store (kind: fs)"
RESULT="$(curl -sS -m 5 "${CURL_TLS_ARGS[@]}" \
    "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/admin/segment-store")"
KIND="$(jq -r '.store.kind // empty' <<<"$RESULT")"
[ "$KIND" = "fs" ] || fail "GET /admin/segment-store did not report store.kind \"fs\": ${RESULT}"
log "admin/segment-store reports store.kind=fs (spec.segmentStore: dir:${DATA_MOUNT_DIR}/segments)"

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

if [ "$E2E_ENCRYPTION" = "1" ]; then
    phase "check the item's plaintext value is absent from the pod's own data directory"
    # `grep -r` over the pod's own data volume (`spec.storage.ephemeral: true`
    # here, but the same on-disk shape as a real PersistentVolumeClaim) — a
    # plaintext write would land the note's exact bytes in an SSTable/WAL
    # file somewhere under here (`animus-storage`'s `LsmEngine` applies no
    # block compression), so finding it would mean the encryption wiring did
    # nothing; finding NOTHING is the property this leg exists to prove.
    if kubectl exec -n "$NAMESPACE" "$DYNAMO_POD" -- \
        sh -c "grep -r -l 'hello from e2e' ${DATA_MOUNT_DIR}" >/dev/null 2>&1; then
        fail "found the plaintext item value under ${DATA_MOUNT_DIR} in pod ${DYNAMO_POD} even \
though spec.encryptionKeySecretName is set — the data directory is not actually sealed"
    fi
    log "plaintext item value not found under ${DATA_MOUNT_DIR} in pod ${DYNAMO_POD}"

    phase "check GET /admin/config never exposes the encryption key material"
    CONFIG_BODY="$(curl -sS -m 5 "${CURL_TLS_ARGS[@]}" \
        "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/admin/config")"
    if grep -qF "$ENCRYPTION_KEY_HEX" <<<"$CONFIG_BODY"; then
        fail "GET /admin/config response contained the raw encryption key hex material"
    fi
    log "GET /admin/config does not expose the encryption key"
fi

phase "scale AnimusCluster to 4 nodes"
kubectl patch animuscluster "$AC_NAME" -n "$NAMESPACE" --type=merge -p '{"spec":{"nodes":4}}'
wait_for "statefulset readyReplicas==4" 300 5 -- sts_ready_equals 4

phase "check the PodDisruptionBudget is scale-invariant after scale-up (S-07c)"
# controlNodes stays 3 here (this scale-up only touches spec.nodes) and the
# data-plane replication factor is already plateaued at 3 nodes, so
# maxUnavailable must still be 1 — not recomputed to something larger just
# because nodes grew.
PDB_MAX_UNAVAIL="$(kubectl get pdb "${AC_NAME}-pdb" -n "$NAMESPACE" \
    -o jsonpath='{.spec.maxUnavailable}' 2>/dev/null || true)"
[ "$PDB_MAX_UNAVAIL" = "1" ] || fail "expected PodDisruptionBudget ${AC_NAME}-pdb maxUnavailable to \
stay 1 after scaling to 4 nodes, got ${PDB_MAX_UNAVAIL:-<empty>}"
log "PodDisruptionBudget ${AC_NAME}-pdb still reports maxUnavailable=1 after scale-up"

phase "GetItem still returns the item after scale-up"
RESULT="$(dynamo_call "DynamoDB_20120810.GetItem" \
    '{"TableName":"E2EItems","Key":{"id":{"S":"widget-1"}},"ConsistentRead":true}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "post-scale GetItem failed: status=${STATUS} body=${BODY}"
NOTE="$(jq -r '.Item.note.S // empty' <<<"$BODY")"
[ "$NOTE" = "hello from e2e" ] || fail "post-scale GetItem did not round-trip the item: ${BODY}"
log "post-scale GetItem ok"

# S-07d: grow spec.controlNodes 3 -> 4, promoting the already-Ready,
# already-Data-role ordinal-3 pod (added by the nodes:3->4 scale-up above)
# into a real control voter — chosen over a fresh 3->5 growth (which would
# need an *additional* spec.nodes scale-up first, provisioning and waiting
# on a brand-new pod/PVC on top of the voter-add sequence itself) so this
# leg's own runtime stays reasonable: it's the smallest possible one-voter
# growth step, reusing a pod this script already waited on.
phase "grow spec.controlNodes from 3 to 4 (S-07d)"
kubectl patch animuscluster "$AC_NAME" -n "$NAMESPACE" --type=merge -p '{"spec":{"controlNodes":4}}'
# The generated ConfigMap's role split flips immediately, which (via the
# S-07d config-hash pod-template annotation) triggers a StatefulSet rolling
# restart of *every* pod, not just ordinal 3 (the annotation is shared
# across the whole pod template) — highest ordinal first, one at a time,
# same as any other pod-template change. `GET /admin/control/members` is
# served by any node (`admin.rs`'s own doc), so this can still be polled
# through the pre-growth port-forward while that restart is in flight.
wait_for "control group reports 4 voters" 300 5 -- control_voters_equals 4
log "control group now reports 4 voters"

phase "check the PodDisruptionBudget after controlNodes growth (S-07d)"
# nodes=4/controlNodes=4 now: the control-plane term is floor((4-1)/2)=1,
# still capped at the same value by the RF-plateaued data-plane term
# (floor((min(4,3)-1)/2)=1) — the point of this check is that the operator
# recomputed maxUnavailable from the *achieved* controlNodes (4), not that
# the number itself moved.
PDB_MAX_UNAVAIL="$(kubectl get pdb "${AC_NAME}-pdb" -n "$NAMESPACE" \
    -o jsonpath='{.spec.maxUnavailable}' 2>/dev/null || true)"
[ "$PDB_MAX_UNAVAIL" = "1" ] || fail "expected PodDisruptionBudget ${AC_NAME}-pdb maxUnavailable=1 \
after growing controlNodes to 4, got ${PDB_MAX_UNAVAIL:-<empty>}"
log "PodDisruptionBudget ${AC_NAME}-pdb reports maxUnavailable=1 after growth"

# The rolling restart above may well have recycled the exact pod this
# script's port-forward targets (a `kubectl port-forward pod/...` dies the
# moment that specific pod is deleted/recreated) — re-resolve and
# re-forward the same way the original "resolve which pod .../wait for
# readiness" phases did, rather than trusting the pre-growth forward is
# still alive.
phase "re-resolve and re-forward the serving pod after controlNodes growth"
if [ -n "$PORT_FORWARD_PID" ]; then
    kill "$PORT_FORWARD_PID" >/dev/null 2>&1 || true
    wait "$PORT_FORWARD_PID" 2>/dev/null || true
    PORT_FORWARD_PID=""
fi
wait_for "svc/${AC_NAME}-dynamo has a resolved endpoint" 30 1 -- has_dynamo_endpoint
DYNAMO_POD="$(dynamo_endpoint_pod)"
[ -n "$DYNAMO_POD" ] || fail "could not resolve a pod backing svc/${AC_NAME}-dynamo after growth"
log "svc/${AC_NAME}-dynamo now routes to pod ${DYNAMO_POD}"
kubectl port-forward "pod/${DYNAMO_POD}" -n "$NAMESPACE" \
    "${DYNAMO_LOCAL_PORT}:${DYNAMO_REMOTE_PORT}" "${ADMIN_LOCAL_PORT}:${ADMIN_REMOTE_PORT}" \
    >"$PORT_FORWARD_LOG" 2>&1 &
PORT_FORWARD_PID=$!
wait_for "dynamo port-forward listening" 30 1 -- port_forward_ready
wait_for "admin port-forward listening" 30 1 -- admin_port_forward_ready
wait_for "pod ${DYNAMO_POD}'s /admin/health is 200" 60 2 -- admin_health_ready

phase "GetItem still returns the item after controlNodes growth"
RESULT="$(dynamo_call "DynamoDB_20120810.GetItem" \
    '{"TableName":"E2EItems","Key":{"id":{"S":"widget-1"}},"ConsistentRead":true}')"
STATUS="$(dynamo_status "$RESULT")"
BODY="$(dynamo_body "$RESULT")"
[ "$STATUS" = "200" ] || fail "post-growth GetItem failed: status=${STATUS} body=${BODY}"
NOTE="$(jq -r '.Item.note.S // empty' <<<"$BODY")"
[ "$NOTE" = "hello from e2e" ] || fail "post-growth GetItem did not round-trip the item: ${BODY}"
log "post-growth GetItem ok — controlNodes growth left the DynamoDB wire serving"

if [ "$E2E_S3" = "1" ]; then
    phase "exercise DynamoDB wire: CreateBackup (S-04 PR 3, S3 backup store)"
    RESULT="$(dynamo_call "DynamoDB_20120810.CreateBackup" \
        '{"TableName":"E2EItems","BackupName":"e2e-s3-backup"}')"
    STATUS="$(dynamo_status "$RESULT")"
    BODY="$(dynamo_body "$RESULT")"
    [ "$STATUS" = "200" ] || fail "CreateBackup failed: status=${STATUS} body=${BODY}"
    BACKUP_ARN="$(jq -r '.BackupDetails.BackupArn // empty' <<<"$BODY")"
    [ -n "$BACKUP_ARN" ] || fail "CreateBackup response missing BackupDetails.BackupArn: ${BODY}"
    log "CreateBackup ok — ${BACKUP_ARN}"

    phase "exercise DynamoDB wire: DescribeBackup"
    RESULT="$(dynamo_call "DynamoDB_20120810.DescribeBackup" \
        "$(jq -n --arg arn "$BACKUP_ARN" '{BackupArn: $arn}')")"
    STATUS="$(dynamo_status "$RESULT")"
    BODY="$(dynamo_body "$RESULT")"
    [ "$STATUS" = "200" ] || fail "DescribeBackup failed: status=${STATUS} body=${BODY}"
    DESCRIBED_ARN="$(jq -r '.BackupDescription.BackupDetails.BackupArn // empty' <<<"$BODY")"
    [ "$DESCRIBED_ARN" = "$BACKUP_ARN" ] || fail "DescribeBackup returned a different BackupArn: ${BODY}"
    log "DescribeBackup ok"

    phase "check GET /admin/backup-store reports kind: s3"
    RESULT="$(curl -sS -m 5 "${CURL_TLS_ARGS[@]}" \
        "${ADMIN_SCHEME}://${ADMIN_HOST}:${ADMIN_LOCAL_PORT}/admin/backup-store")"
    KIND="$(jq -r '.store.kind // empty' <<<"$RESULT")"
    [ "$KIND" = "s3" ] || fail "GET /admin/backup-store did not report store.kind \"s3\": ${RESULT}"
    log "admin/backup-store reports store.kind=s3 (${RESULT})"
fi

if [ "$E2E_WEBHOOK" = "1" ]; then
    phase "build + load the animus-operator image (S-07e)"
    # Same Dockerfile, same builder stage the ANIMUSD_IMAGE build already
    # ran (`cargo build --release -p animusd -p animus-cli -p
    # animus-operator` in one pass) — BuildKit's cache mounts (`--mount=
    # type=cache,target=/build/target`) persist across this second
    # `docker build` invocation in the same daemon, so this is a fast
    # cache hit, not a second from-scratch compile.
    docker build --target runtime-operator -t "$OPERATOR_IMAGE" "$REPO_ROOT"
    kind load docker-image "$OPERATOR_IMAGE" --name "$CLUSTER_NAME"

    phase "generate a self-signed webhook TLS cert (S-07e)"
    # The hand-issued-Secret path (`deploy/operator/README.md`'s own
    # "Without cert-manager" alternative) — no cert-manager dependency for
    # this leg, independent of whatever E2E_TLS did above. Self-signed is
    # fine: the only caller is the kind cluster's own API server, dialing
    # a Service DNS name inside the same cluster.
    WEBHOOK_KEY_FILE="${WORKDIR}/webhook-tls.key"
    WEBHOOK_CERT_FILE="${WORKDIR}/webhook-tls.crt"
    WEBHOOK_SAN="DNS:${WEBHOOK_SERVICE_NAME},DNS:${WEBHOOK_SERVICE_NAME}.${NAMESPACE},DNS:${WEBHOOK_SERVICE_NAME}.${NAMESPACE}.svc,DNS:${WEBHOOK_SERVICE_NAME}.${NAMESPACE}.svc.cluster.local"
    openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -keyout "$WEBHOOK_KEY_FILE" -out "$WEBHOOK_CERT_FILE" \
        -subj "/CN=${WEBHOOK_SERVICE_NAME}.${NAMESPACE}.svc" \
        -addext "subjectAltName=${WEBHOOK_SAN}" \
        >/dev/null 2>&1
    [ -s "$WEBHOOK_CERT_FILE" ] && [ -s "$WEBHOOK_KEY_FILE" ] ||
        fail "openssl did not produce a webhook cert/key pair"
    kubectl create secret tls "$WEBHOOK_SECRET_NAME" -n "$NAMESPACE" \
        --cert="$WEBHOOK_CERT_FILE" --key="$WEBHOOK_KEY_FILE" \
        --dry-run=client -o yaml | kubectl apply -f -
    WEBHOOK_CA_BUNDLE="$(base64 -w0 "$WEBHOOK_CERT_FILE")"

    phase "deploy the operator in-cluster, --webhook-only (S-07e)"
    # A SECOND, minimal Deployment — not deploy/operator/deployment.yaml
    # (which would start a second full reconciler racing the out-of-cluster
    # one already running against these same objects). `--webhook-only`
    # (main.rs) means this process builds no Kubernetes client at all, so
    # it needs no RBAC/ServiceAccount of its own beyond the namespace's
    # default one.
    kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${WEBHOOK_DEPLOYMENT_NAME}
  namespace: ${NAMESPACE}
spec:
  replicas: 1
  selector:
    matchLabels: {app: ${WEBHOOK_DEPLOYMENT_NAME}}
  template:
    metadata:
      labels: {app: ${WEBHOOK_DEPLOYMENT_NAME}}
    spec:
      containers:
        - name: operator
          image: ${OPERATOR_IMAGE}
          args:
            - "run"
            - "--webhook-addr=0.0.0.0:9443"
            - "--webhook-cert=/etc/animus/webhook/tls.crt"
            - "--webhook-key=/etc/animus/webhook/tls.key"
            - "--webhook-only"
          ports:
            - name: webhook
              containerPort: 9443
          volumeMounts:
            - name: webhook-tls
              mountPath: /etc/animus/webhook
              readOnly: true
      volumes:
        - name: webhook-tls
          secret:
            secretName: ${WEBHOOK_SECRET_NAME}
---
apiVersion: v1
kind: Service
metadata:
  name: ${WEBHOOK_SERVICE_NAME}
  namespace: ${NAMESPACE}
spec:
  selector: {app: ${WEBHOOK_DEPLOYMENT_NAME}}
  ports:
    - name: webhook
      port: 443
      targetPort: webhook
EOF
    kubectl -n "$NAMESPACE" rollout status "deployment/${WEBHOOK_DEPLOYMENT_NAME}" --timeout=120s

    phase "wait for the webhook Service to be routable (S-07e / issue #704)"
    # Deliberately BEFORE registering the ValidatingWebhookConfiguration, not
    # after: once the API server has a webhook config for this rule, ANY
    # matching write — including the very first patch below — triggers a
    # live dial to the Service, so the soundest ordering is to only ever
    # point the API server at a target already known to be routable, rather
    # than register-then-hope. Endpoints becoming non-empty is necessary but
    # not provably sufficient (kube-proxy's own ClusterIP-rule programming
    # trails Endpoints by a small, unbounded amount in `kind`), which is why
    # the rejection probe below still retries through the same error class
    # as a second, belt-and-suspenders guard.
    wait_for "endpoints/${WEBHOOK_SERVICE_NAME} has a routable address" 30 1 -- webhook_endpoint_ready

    phase "register the ValidatingWebhookConfiguration (S-07e)"
    # namespaceSelector scopes this webhook to this leg's own namespace
    # only — a webhook outage here (or a bug in this leg's own manifest)
    # can't affect any AnimusCluster write outside this smoke's own
    # objects, even under failurePolicy: Fail.
    kubectl apply -f - <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: ${WEBHOOK_CONFIG_NAME}
webhooks:
  - name: validate.e2e.animuscluster.animusdb.io
    admissionReviewVersions: ["v1"]
    sideEffects: None
    failurePolicy: Fail
    timeoutSeconds: 5
    namespaceSelector:
      matchLabels:
        kubernetes.io/metadata.name: ${NAMESPACE}
    rules:
      - apiGroups: ["animusdb.io"]
        apiVersions: ["v1alpha1"]
        resources: ["animusclusters"]
        operations: ["CREATE", "UPDATE"]
    clientConfig:
      service:
        name: ${WEBHOOK_SERVICE_NAME}
        namespace: ${NAMESPACE}
        path: /validate
        port: 443
      caBundle: ${WEBHOOK_CA_BUNDLE}
EOF

    phase "assert an invalid write is rejected by the API server (S-07e)"
    # spec.controlNodes decreasing from 3 (this manifest's own value,
    # grown to 4 by the S-07d leg above) to 1 is the identical grow-only
    # rule crate::validate::validate_spec enforces — a real Kubernetes
    # API-server-level rejection, not a status condition, is exactly the
    # property no `cargo test -p animus-operator` run can prove.
    #
    # Issue #704: the endpoints wait above narrows the race but does not
    # close it (kube-proxy's ClusterIP programming can still trail Endpoints
    # becoming non-empty), so this probe retries — bounded, ~30s total —
    # only while the failure looks like the webhook Service not being dialable
    # yet (`failed calling webhook` / `connection refused` / `InternalError`,
    # the exact error shape `failurePolicy: Fail` turns a dial failure into).
    # A response that IS an admission rejection (names spec.controlNodes) —
    # or the final attempt once the budget is spent — is the only place the
    # assertion below actually gets evaluated, so a genuinely missing/wrong
    # rejection still fails the phase with the same message as before this
    # fix.
    WEBHOOK_REJECT_LOG="${WORKDIR}/webhook-reject.log"
    WEBHOOK_PROBE_TIMEOUT=30
    WEBHOOK_PROBE_INTERVAL=3
    waited=0
    while true; do
        if kubectl patch animuscluster "$AC_NAME" -n "$NAMESPACE" --type merge \
            -p '{"spec":{"controlNodes":1}}' >"$WEBHOOK_REJECT_LOG" 2>&1; then
            fail "expected the admission webhook to reject a spec.controlNodes decrease, but the patch succeeded: $(cat "$WEBHOOK_REJECT_LOG")"
        fi
        if grep -q "spec.controlNodes" "$WEBHOOK_REJECT_LOG"; then
            break
        fi
        if grep -qE "failed calling webhook|connection refused|InternalError" "$WEBHOOK_REJECT_LOG" \
            && [ "$waited" -lt "$WEBHOOK_PROBE_TIMEOUT" ]; then
            log "rejection probe: webhook Service not dialable yet at ${waited}s (issue #704) — retrying: $(cat "$WEBHOOK_REJECT_LOG")"
            sleep "$WEBHOOK_PROBE_INTERVAL"
            waited=$((waited + WEBHOOK_PROBE_INTERVAL))
            continue
        fi
        break
    done
    grep -q "spec.controlNodes" "$WEBHOOK_REJECT_LOG" ||
        fail "webhook rejection did not name spec.controlNodes: $(cat "$WEBHOOK_REJECT_LOG")"
    log "invalid spec.controlNodes decrease correctly rejected by the admission webhook"

    phase "assert a valid write is still admitted (S-07e)"
    # Same dial-race guard as the rejection probe above, on the acceptance
    # side (issue #704): a valid patch goes through the identical webhook
    # call, so it can hit the identical "not routable yet" window. By this
    # point the rejection probe above has already confirmed the webhook IS
    # dialable, so this loop is expected to succeed on its first attempt in
    # practice — kept identical anyway for soundness, not because it is
    # expected to ever retry.
    WEBHOOK_ACCEPT_LOG="${WORKDIR}/webhook-accept.log"
    waited=0
    while true; do
        if kubectl patch animuscluster "$AC_NAME" -n "$NAMESPACE" --type merge \
            -p '{"spec":{"quiesceAfterSecs":7}}' >"$WEBHOOK_ACCEPT_LOG" 2>&1; then
            break
        fi
        if grep -qE "failed calling webhook|connection refused|InternalError" "$WEBHOOK_ACCEPT_LOG" \
            && [ "$waited" -lt "$WEBHOOK_PROBE_TIMEOUT" ]; then
            log "acceptance probe: webhook Service not dialable yet at ${waited}s (issue #704) — retrying: $(cat "$WEBHOOK_ACCEPT_LOG")"
            sleep "$WEBHOOK_PROBE_INTERVAL"
            waited=$((waited + WEBHOOK_PROBE_INTERVAL))
            continue
        fi
        fail "expected a valid write to be admitted, but the patch failed: $(cat "$WEBHOOK_ACCEPT_LOG")"
    done
    ACTUAL_QUIESCE="$(kubectl get animuscluster "$AC_NAME" -n "$NAMESPACE" \
        -o jsonpath='{.spec.quiesceAfterSecs}')"
    [ "$ACTUAL_QUIESCE" = "7" ] ||
        fail "expected a valid write to be admitted and persisted, got quiesceAfterSecs=${ACTUAL_QUIESCE:-<empty>}"
    log "valid write correctly admitted by the admission webhook"
fi

phase "delete AnimusCluster and verify GC"
kubectl delete animuscluster "$AC_NAME" -n "$NAMESPACE"
wait_for "statefulset garbage-collected" 120 3 -- sts_gone

phase "done"
log "all phases passed"
