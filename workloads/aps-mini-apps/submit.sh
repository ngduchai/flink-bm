#!/usr/bin/env bash
set -euo pipefail

# --- EDIT THESE ---
JM_HOST="${JM_HOST:-<jobmanager-hostname>}"    # e.g. node001
JOB_ROOT_DIR="/home/ndhai/diaspora/src/flink"
JOB_PATH_HOST="${JOB_PATH_HOST:-$JOB_ROOT_DIR/aps-mini-apps/test_sirt_ops.py}"  # host path to job.py (shared FS)
JOB_PATH_CONT="${JOB_PATH_CONT:-/opt/workloads/aps-mini-apps/test_sirt_ops.py}" # path inside JM container
PY_IN_CONT="${PY_IN_CONT:-python3}"            # python path inside container (e.g. /usr/bin/python3.10)
FLINK_BIN="/opt/flink/bin/flink"               # inside container; adjust if different

# Copy/ensure job.py is visible at the container path if needed
# If your JM container already sees the shared path at /opt/workloads/aps-mini-apps,
# you can skip scp. Otherwise, push it to the JM host's bind-mount.
if [[ -f "$JOB_PATH_HOST" ]]; then
  echo "[submit] Using shared job at $JOB_PATH_HOST"
else
  echo "[submit] ERROR: JOB_PATH_HOST not found: $JOB_PATH_HOST"
  exit 1
fi

# Remote command executed on JM host
read -r -d '' REMOTE <<'EOS' || true
set -euo pipefail
INST=$(apptainer instance list | awk 'NR>1 {print $1}' | grep -i 'flink' | head -n1 || true)
if [[ -z "$INST" ]]; then
  echo "[submit/JM] No running Apptainer instance matching /flink/"; exit 1
fi
echo "[submit/JM] Using instance: $INST"

apptainer exec instance://"$INST" bash -lc '
  set -euo pipefail
  if [[ ! -f "'"$JOB_PATH_CONT"'" ]]; then
    echo "[submit/cont] job.py not found at: '"$JOB_PATH_CONT"'"; exit 1
  fi
  export PATH="/opt/flink/bin:$PATH"
  echo "[submit/cont] flink version: $(flink --version | head -n1)"
  echo "[submit/cont] submitting job..."
  "'"$FLINK_BIN"'" run -py "'"$JOB_PATH_CONT"'" --pyExecutable "'"$PY_IN_CONT"'" -p 1
'
EOS

echo "[submit] Submitting from login node to JM host: $JM_HOST"
ssh -o BatchMode=yes "$JM_HOST" "$REMOTE"
echo "[submit] Done. Check Flink UI on the JM (default :8081) or JM/TM logs."
