#!/usr/bin/env bash
set -euo pipefail

# --- EDIT THESE ---

# Node list from PBS or fallback
NODE_FILE=${PBS_NODEFILE:-nodes.txt}
nodes=( $(cat "$NODE_FILE") )
JM_HOST="${nodes[0]}"

JOB_ROOT_DIR="/home/ndhai/diaspora/src/flink"
JOB_PATH_HOST="${JOB_PATH_HOST:-$JOB_ROOT_DIR/workloads/aps-mini-apps/test_sirt_ops.py}"   # host path to job.py (shared FS)
JOB_PATH_CONT="${JOB_PATH_CONT:-/opt/workloads/aps-mini-apps/test_sirt_ops.py}"            # path inside JM container
PY_IN_CONT="${PY_IN_CONT:-python3}"                                                        # python inside container
FLINK_BIN="${FLINK_BIN:-/opt/flink/bin/flink}"                                            # inside container

# Verify host-side job exists (remove this block if the file only exists in the container)
if [[ -f "$JOB_PATH_HOST" ]]; then
  echo "[submit] Using shared job at $JOB_PATH_HOST"
else
  echo "[submit] ERROR: JOB_PATH_HOST not found: $JOB_PATH_HOST"
  exit 1
fi

# Remote command executed on JM host
read -r -d '' REMOTE <<'EOS' || true
set -euo pipefail
cd "$HOME" && source load-apptainer.sh

INST=$(apptainer instance list | awk 'NR>1 {print $1}' | grep -i 'flink-jobmanager' | head -n1 || true)
if [[ -z "$INST" ]]; then
  echo "[submit/JM] No running Apptainer instance matching /flink-jobmanager/"; exit 1
fi
echo "[submit/JM] Using instance: $INST"

# Run inside the container; export required vars first to avoid 'unbound variable'
apptainer exec --cleanenv instance://"$INST" bash -lc '
  set -euo pipefail
  export JOB_PATH_CONT="__JOB_PATH_CONT__"
  export PY_IN_CONT="__PY_IN_CONT__"
  export FLINK_BIN="__FLINK_BIN__"

  if [[ ! -f "$JOB_PATH_CONT" ]]; then
    echo "[submit/cont] job.py not found at: $JOB_PATH_CONT"; exit 1
  fi

  export PATH="/opt/flink/bin:$PATH"
  echo "[submit/cont] flink version: $(flink --version | head -n1)"
  echo "[submit/cont] submitting job: $JOB_PATH_CONT with python: $PY_IN_CONT"
  "$FLINK_BIN" run -py "$JOB_PATH_CONT" --pyExecutable "$PY_IN_CONT" -p 1
'
EOS

# Safely inject variable values into the REMOTE heredoc
escape() { printf '%s' "$1" | sed "s/'/'\\\\''/g"; }
REMOTE=${REMOTE//__JOB_PATH_CONT__/$(escape "$JOB_PATH_CONT")}
REMOTE=${REMOTE//__PY_IN_CONT__/$(escape "$PY_IN_CONT")}
REMOTE=${REMOTE//__FLINK_BIN__/$(escape "$FLINK_BIN")}

echo "[submit] Submitting from login node to JM host: $JM_HOST"
ssh -o BatchMode=yes "$JM_HOST" "$REMOTE"
echo "[submit] Done. Check Flink UI on the JM (default :8081) or JM/TM logs."
