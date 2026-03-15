#!/usr/bin/env bash

set -euo pipefail

# Fixed values
RECORDCOUNT=125000000
OPERATIONCOUNT=125000000
FIELDCOUNT=1
FIELDLENGTH=128

# Single-value arrays for later expansion
THREADS_LIST=(14)
WORKLOAD_FILES=("./workloads/workloada")
LIMIT_OPS_LIST=(2000000)
MAX_BACKGROUND_JOBS_LIST=(2)

# Other fixed command arguments
NUMA_MEMBIND=0
NUMA_CPUBIND=0
DB="rocksdb"
DB_PROPERTIES="./rocksdb/rocksdb.properties"
STATUS_INTERVAL=1
SLEEP_AFTER_LOAD=200
ENABLE_ASYNC_TEST=true
OUTPUT_DIR="./logs/ycsbrocks"
YCSB_BIN="./ycsbrocks"

usage() {
  cat <<'EOF'
Usage:
  ./run_ycsbrocks_matrix.sh [-p name=value]...

Example:
  ./run_ycsbrocks_matrix.sh -p rocksdb.write_buffer_size=134217728 -p rocksdb.max_open_files=5000

Notes:
  - Top-of-file arrays are traversed automatically.
  - Extra properties must be passed as repeated "-p name=value" pairs.
EOF
}

EXTRA_PROPS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p)
      if [[ $# -lt 2 ]]; then
        echo "Missing value after -p" >&2
        usage >&2
        exit 1
      fi
      EXTRA_PROPS+=("-p" "$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unsupported argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

mkdir -p "$OUTPUT_DIR"

run_id=0

for workload_file in "${WORKLOAD_FILES[@]}"; do
  workload_name="$(basename "$workload_file")"

  for threads in "${THREADS_LIST[@]}"; do
    for limit_ops in "${LIMIT_OPS_LIST[@]}"; do
      for max_background_jobs in "${MAX_BACKGROUND_JOBS_LIST[@]}"; do
        run_id=$((run_id + 1))
        log_file="$OUTPUT_DIR/${workload_name}_t${threads}_ops${limit_ops}_bg${max_background_jobs}.log"

        cmd=(
          numactl
          --membind "$NUMA_MEMBIND"
          --cpubind "$NUMA_CPUBIND"
          "$YCSB_BIN"
          -load
          -run
          -db "$DB"
          -P "$workload_file"
          -P "$DB_PROPERTIES"
          -threads "$threads"
          -s
          -p "recordcount=${RECORDCOUNT}"
          -p "operationcount=${OPERATIONCOUNT}"
          -p "fieldcount=${FIELDCOUNT}"
          -p "fieldlength=${FIELDLENGTH}"
          -p "limit.ops=${limit_ops}"
          -p "rocksdb.max_background_jobs=${max_background_jobs}"
          -p "status.interval=${STATUS_INTERVAL}"
          -p "sleepafterload=${SLEEP_AFTER_LOAD}"
        )

        if [[ "$ENABLE_ASYNC_TEST" == true ]]; then
          cmd+=(-asynctest)
        fi

        cmd+=("${EXTRA_PROPS[@]}")

        echo "[$run_id] Running workload=$workload_name threads=$threads limit.ops=$limit_ops rocksdb.max_background_jobs=$max_background_jobs"
        echo "[$run_id] Log: $log_file"

        "${cmd[@]}" >"$log_file" 2>&1
      done
    done
  done
done

echo "Completed $run_id run(s). Logs are in $OUTPUT_DIR"