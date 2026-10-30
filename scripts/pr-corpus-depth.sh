#!/usr/bin/env bash
# Run, at its NIGHTLY depth, every deep corpus whose test file a PR changed
# (issue #1271). Per-push CI runs each corpus at K=1 seed, so a new cell or a
# new negative control can merge "born red" and only fail in the nightly
# `corpus-deep.yml`. This is the PR-time half of that gate.
#
#   scripts/pr-corpus-depth.sh select <base-ref>   # print the corpora a diff selects
#   scripts/pr-corpus-depth.sh run    <base-ref>   # run them (what CI does)
#   scripts/pr-corpus-depth.sh run-all             # every corpus (manual)
#   scripts/pr-corpus-depth.sh check               # table vs corpus-deep.yml drift
#   scripts/pr-corpus-depth.sh list-filters        # `-- --list`: every name filter matches tests
#
# THE TABLE below is the only thing to edit. One row per `corpus-deep.yml`
# step id: `id | changed-file regex | env (space separated) | commands (;;)`.
# The env and commands are copied from the nightly step (`check` fails when
# they drift from it). A new corpus: add its nightly step, then its row here.
# A file regex should name the corpus's own test file(s) and the helper
# module that carries its cells, not the whole crate (a crate-wide regex would
# run a 30-minute corpus for every unrelated edit).
set -uo pipefail

# id|regex|env|commands
TABLE=$(cat <<'TBL'
raftkv|crates/animus-test/tests/it/raftkv_linearizable\.rs|ANIMUS_RAFTKV_SEEDS=40|cargo test -p animus-test --test it raftkv_linearizable::
raftkv_lsm|crates/animus-test/tests/it/raftkv_linearizable\.rs|ANIMUS_RAFTKV_LSM=1 ANIMUS_RAFTKV_SEEDS=40|cargo test -p animus-test --test it raftkv_linearizable::
raftkv_wal_faults|crates/animus-test/tests/it/raftkv_linearizable\.rs|ANIMUS_RAFTKV_WAL_FAULTS=1 ANIMUS_RAFTKV_SEEDS=40|cargo test -p animus-test --test it raftkv_linearizable::raftkv_wal_faults_corpus_is_linearizable
txn|crates/animus-test/tests/it/txn_serializable\.rs|ANIMUS_TXN_SEEDS=40|cargo test -p animus-test --test it txn_serializable::
streams|crates/animus-test/tests/it/stream_lineage_corpus\.rs|ANIMUS_STREAM_SEEDS=40|cargo test -p animus-test --test it stream_lineage_corpus::
backfill|crates/animus-test/tests/it/backfill_fault_corpus\.rs|ANIMUS_BACKFILL_SEEDS=40|cargo test -p animus-test --test it backfill_fault_corpus::
split|crates/animus-cp-data/tests/it/seed_batch\.rs|ANIMUS_SPLIT_SEEDS=40|cargo test -p animus-cp-data --test it seed_batch::
reconciler|crates/animus-cp-data/tests/it/reconciler_corpus\.rs|ANIMUS_RECONCILER_SEEDS=40|cargo test -p animus-cp-data --test it reconciler_corpus::
quiesce|crates/animus-cp-data/tests/it/quiescence\.rs|ANIMUS_QUIESCE_SEEDS=40|cargo test -p animus-cp-data --test it quiescence::
learner|crates/animus-control/tests/it/learner_corpus\.rs|ANIMUS_LEARNER_SEEDS=40|cargo test -p animus-control --test it learner_corpus::
reconfigure_drop|crates/animus-cp-data/tests/it/reconfigure_healthy_drop\.rs|ANIMUS_RECONFIGURE_DROP_SEEDS=40|cargo test -p animus-cp-data --test it reconfigure_healthy_drop::
inplace_split|crates/animus-cp-data/tests/it/inplace_split_reconciler\.rs|ANIMUS_INPLACE_SPLIT_SEEDS=40|cargo test -p animus-cp-data --test it inplace_split_reconciler::
backup|crates/animus-test/tests/it/backup_fault_corpus\.rs|ANIMUS_BACKUP_SEEDS=40|cargo test -p animus-test --test it backup_fault_corpus::
pitr|crates/animus-test/tests/it/pitr_fault_corpus\.rs|ANIMUS_PITR_SEEDS=40|cargo test -p animus-test --test it pitr_fault_corpus::
lsm_crash|crates/animus-storage/tests/it/lsm_crash\.rs|ANIMUS_LSM_CRASH_SEEDS=40|cargo test -p animus-storage --test it lsm_crash::
lsm_disk_fault|crates/animus-storage/tests/it/lsm_disk_faults\.rs|ANIMUS_LSM_DISK_FAULT_SEEDS=40|cargo test -p animus-storage --test it lsm_disk_faults::
lsm_stall|crates/animus-storage/tests/it/lsm_write_stall\.rs crates/animus-cp-data/tests/it/lsm_stall_admission\.rs crates/animus-cp-data/tests/it/compaction_io_budget_corpus\.rs crates/animusd/src/sim_cluster_lsm_stall\.rs|ANIMUS_LSM_STALL_SEEDS=20|cargo test -p animus-storage --test it lsm_write_stall:: ;; cargo test -p animus-cp-data --test it lsm_stall_admission:: ;; cargo test -p animus-cp-data --test it compaction_io_budget_corpus:: ;; cargo test -p animusd --lib sim_cluster_lsm_stall
lsm_encrypted|crates/animus-storage/tests/it/lsm_crash_encrypted\.rs|ANIMUS_LSM_ENCRYPTED_SEEDS=40|cargo test -p animus-storage --test it lsm_crash_encrypted::
segment_store_encrypted|crates/animus-test/tests/it/segment_store_encrypted_fault_corpus\.rs|ANIMUS_SEGMENT_STORE_ENCRYPTED_SEEDS=40|cargo test -p animus-test --test it segment_store_encrypted_fault_corpus::
control|crates/animus-control/tests/it/control_corpus\.rs|ANIMUS_CONTROL_SEEDS=40|cargo test -p animus-control --test it control_corpus::
simcluster|crates/animusd/src/sim_cluster_corpus\.rs|ANIMUS_SIMCLUSTER_SEEDS=25|cargo test -p animusd --lib sim_cluster_corpus
dynamo_wire|crates/animusd/src/sim_cluster_dynamo_corpus\.rs|ANIMUS_DYNAMO_WIRE_SEEDS=25|cargo test -p animusd --lib sim_cluster_dynamo_corpus
zone_placement|crates/animusd/src/sim_cluster_zone_placement\.rs|ANIMUS_ZONE_PLACEMENT_SEEDS=25|cargo test -p animusd --lib sim_cluster_zone_placement
split_relocation|crates/animusd/src/sim_cluster_split_relocation\.rs|ANIMUS_SPLIT_RELOCATION_SEEDS=20|cargo test -p animusd --lib sim_cluster_split_relocation
export_import|crates/animus-test/tests/it/export_import_fault_corpus\.rs|ANIMUS_EXPORT_IMPORT_SEEDS=40|cargo test -p animus-test --test it export_import_fault_corpus::
s3_fault|crates/animus-test/tests/it/s3_fault_corpus\.rs|ANIMUS_S3_FAULT_SEEDS=100|cargo test -p animus-test --test it s3_fault_corpus::
heartbeat|crates/animus-cp-data/tests/it/heartbeat_batch_corpus\.rs|ANIMUS_HEARTBEAT_SEEDS=40|cargo test -p animus-cp-data --test it heartbeat_batch_corpus::
wan_timing|crates/animus-cp-data/tests/it/wan_timing_corpus\.rs|ANIMUS_WAN_TIMING_SEEDS=40|cargo test -p animus-cp-data --test it wan_timing_corpus::
density_admission|crates/animus-cp-data/tests/it/density_admission_corpus\.rs|ANIMUS_DENSITY_SEEDS=20|cargo test -p animus-cp-data --test it density_admission_corpus::
repair_pacing|crates/animus-control/tests/it/repair_pacing\.rs|ANIMUS_REPAIR_PACING_SEEDS=20|cargo test -p animus-control --test it repair_pacing::
mrsc_leader|crates/animus-cp-data/tests/it/preferred_leader_corpus\.rs|ANIMUS_MRSC_SEEDS=40|cargo test -p animus-cp-data --test it preferred_leader_corpus::
mrsc_cluster|crates/animusd/src/sim_cluster_mrsc\.rs|ANIMUS_MRSC_SEEDS=10|cargo test -p animusd --lib sim_cluster_mrsc
mrec_world|crates/animusd/src/sim_world_mrec_corpus\.rs|ANIMUS_MREC_SEEDS=15|cargo test -p animusd --lib sim_world_mrec_corpus
sharedwal|crates/animus-cp-data/tests/it/sharedwal_fault_corpus\.rs|ANIMUS_SHAREDWAL_SEEDS=40|cargo test -p animus-cp-data --test it sharedwal_fault_corpus::
wal_rewrite_crash|crates/animus-cp-data/tests/it/wal_rewrite_crash\.rs|ANIMUS_WAL_REWRITE_CRASH_SEEDS=50|cargo test -p animus-cp-data --test it wal_rewrite_crash::
upgrade_restart|crates/animus-test/tests/it/upgrade_restart_corpus\.rs crates/animus-test/tests/it/upgrade_restart_tier0\.rs crates/animus-test/tests/it/upgrade_restart_txn_envelope\.rs|ANIMUS_UPGRADE_RESTART_SEEDS=100|cargo test -p animus-test --test it upgrade_restart_corpus::
upgrade_restart_cluster|crates/animusd/src/sim_cluster_upgrade_corpus\.rs|ANIMUS_UPGRADE_RESTART_SEEDS=50|cargo test -p animusd --lib sim_cluster_upgrade_corpus
upgrade_mixed|crates/animus-control/tests/it/version_mixed_corpus\.rs|ANIMUS_UPGRADE_SEEDS=100|cargo test -p animus-control --test it version_mixed_corpus::
upgrade_mixed_cluster|crates/animusd/src/sim_cluster_mixed_version_corpus\.rs|ANIMUS_UPGRADE_SEEDS=40|cargo test -p animusd --lib sim_cluster_mixed_version
upgrade_roll|crates/animusd/src/sim_cluster_roll_orchestrator\.rs|ANIMUS_UPGRADE_SEEDS=40|cargo test -p animusd --lib sim_cluster_roll_orchestrator
learner_snapshot_livelock|crates/animus-cp-data/tests/it/learner_snapshot_livelock_under_continuous_writer\.rs|ANIMUS_LEARNER_SNAPSHOT_LIVELOCK_SEEDS=16|cargo test -p animus-cp-data --test it learner_snapshot_livelock_under_continuous_writer::
scale|crates/animusd/src/sim_cluster_scale\.rs|ANIMUS_SCALE_SEEDS=10 ANIMUS_SCALE_MAX_TABLETS=50000|cargo test -p animusd --lib sim_cluster_scale
TBL
)

# The regex field may hold several space-separated alternatives (`|` is the
# field separator, so a regex cannot use alternation).
rows() { printf '%s\n' "$TABLE" | grep -v '^$'; }

matches() { # <regex-field> <file>
  for r in $1; do
    if printf '%s\n' "$2" | grep -Eq "^${r}$"; then return 0; fi
  done
  return 1
}

select_ids() { # <changed files on stdin>
  local files
  files=$(cat)
  rows | while IFS='|' read -r id re _env _cmd; do
    while IFS= read -r f; do
      [ -n "$f" ] || continue
      if matches "$re" "$f"; then echo "$id"; break; fi
    done <<<"$files"
  done
}

run_id() { # <id>
  local row id re env cmds
  row=$(rows | grep -E "^$1\|" | head -1)
  IFS='|' read -r id re env cmds <<<"$row"
  echo "::group::corpus $id (nightly depth: ${env:-default})"
  local rc=0 cmd
  while IFS= read -r cmd; do
    cmd=$(echo "$cmd" | sed 's/^ *//; s/ *$//')
    [ -n "$cmd" ] || continue
    echo "+ $env $cmd -- --nocapture"
    # shellcheck disable=SC2086
    env $env $cmd -- --nocapture || rc=1
  done <<<"${cmds// ;; /$'\n'}"
  echo "::endgroup::"
  return $rc
}

case "${1:-}" in
  select)
    git diff --name-only "${2:?base ref}"...HEAD | select_ids | sort -u ;;
  run)
    ids=$(git diff --name-only "${2:?base ref}"...HEAD | select_ids | sort -u)
    if [ -z "$ids" ]; then echo "no corpus test file changed: nothing to run at nightly depth"; exit 0; fi
    echo "corpora selected: $(echo $ids)"
    failed=()
    for id in $ids; do run_id "$id" || failed+=("$id"); done
    if [ ${#failed[@]} -gt 0 ]; then echo "RED at nightly depth: ${failed[*]}"; exit 1; fi ;;
  run-all)
    failed=()
    for id in $(rows | cut -d'|' -f1); do run_id "$id" || failed+=("$id"); done
    [ ${#failed[@]} -eq 0 ] || { echo "RED: ${failed[*]}"; exit 1; } ;;
  check)
    # The table must mirror the nightly step: same id, env values, commands.
    yml=.github/workflows/corpus-deep.yml
    bad=0
    while IFS='|' read -r id re env cmds; do
      grep -q "id: $id\$" "$yml" || { echo "table row '$id' has no step in $yml"; bad=1; continue; }
      step=$(awk -v id="$id" '$0 ~ "id: "id"$" {f=1; next} f && /^      - name:/ {exit} f' "$yml")
      for kv in $env; do
        grep -qF "${kv%%=*}" <<<"$step" || { echo "$id: $yml step does not set ${kv%%=*}"; bad=1; }
        v=${kv#*=}
        grep -Eq "${kv%%=*}: .*'?\"?${v}'?\"?" <<<"$step" || { echo "$id: $yml step has a different depth than $kv"; bad=1; }
      done
      while IFS= read -r cmd; do
        cmd=$(echo "$cmd" | sed 's/^ *//; s/ *$//')
        [ -n "$cmd" ] || continue
        grep -qF -- "$cmd" <<<"$step" || { echo "$id: $yml step does not run: $cmd"; bad=1; }
      done <<<"${cmds// ;; /$'\n'}"
    done < <(rows)
    for id in $(grep -oE '^        id: [a-z_0-9]+' "$yml" | awk '{print $2}' | grep -v '^report$'); do
      rows | cut -d'|' -f1 | grep -qx "$id" || { echo "$yml step '$id' has no row in $0"; bad=1; }
    done
    exit $bad ;;
  list-filters)
    # Every command's name filter must select at least one test, else the
    # corpus step passes green while running nothing.
    bad=0
    while IFS='|' read -r id re env cmds; do
      while IFS= read -r cmd; do
        cmd=$(echo "$cmd" | sed 's/^ *//; s/ *$//')
        [ -n "$cmd" ] || continue
        n=$($cmd -- --list 2>/dev/null | grep -c ': test$')
        echo "$id: $n tests  ($cmd)"
        [ "$n" -gt 0 ] || bad=1
      done <<<"${cmds// ;; /$'\n'}"
    done < <(rows)
    exit $bad ;;
  *) sed -n '2,12p' "$0"; exit 2 ;;
esac
