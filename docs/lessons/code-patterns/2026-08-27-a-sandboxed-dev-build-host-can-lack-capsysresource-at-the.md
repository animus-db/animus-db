# A sandboxed dev/build host can lack `CAP_SYS_RESOURCE` at the kernel/hypervisor level, and no per-container `--cap-add` can restore it

**A sandboxed dev/build host can lack `CAP_SYS_RESOURCE` at the
kernel/hypervisor level, and no per-container `--cap-add` can restore
it** (ADR 0060 e2e work, ~3 hours root-causing a `kind create cluster`
failure). Every `kind` node's own Kubernetes control plane (`etcd`/
`kube-apiserver`/`kube-scheduler`/`kube-controller-manager`, static pods)
gets a **negative** `oom_score_adj` from kubelet unconditionally — not
configurable via pod spec or kind config, standard "protect the critical
pods from the OOM killer" behavior. Applying a negative value needs
`CAP_SYS_RESOURCE` at container-create time inside `runc`'s own `nsexec`,
and a capability absent from the outermost privilege domain can never be
regranted to a nested/privileged container — confirmed here by `docker
run --cap-add SYS_RESOURCE` being flatly rejected as "not supported by
your kernel or not available in the current environment," not merely
denied at use. The symptom at the `kubectl`/kubelet layer gives almost no
hint of this: containerd launders the real error into the generic `can't
get final child's PID from pipe: EOF`, which looks exactly like a cgroup-
driver mismatch, a containerd-version regression, or a seccomp profile
issue — all three were tried and ruled out (`SystemdCgroup` true/false,
two node images spanning containerd 1.7 and 2.1, an unconfined seccomp
profile) before a direct `runc create --debug` reproduction against a
hand-built OCI bundle isolated the actual line: `nsexec: failed to update
/proc/self/oom_score_adj: Permission denied`. **General rule**: when a
nested-container workload fails identically across every runtime-version/
cgroup-driver/seccomp permutation you can think to vary, stop varying
*its* configuration and check the *host's own* capability set directly
(`capsh --print`, or `docker run --cap-add <X> ... true` for the specific
capability) — a wrapped, generic runtime error can be hiding a single
missing capability that no amount of downstream reconfiguration can work
around. See `crates/animus-operator/CLAUDE.md`'s e2e section for the full
diagnosis and the exact log signature to grep for.
