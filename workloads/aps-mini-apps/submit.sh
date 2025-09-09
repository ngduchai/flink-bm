#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 job_script.py config.json"
  echo "  job_script.py : APS mini-apps Flink job script to submit (e.g. test_sirt_ops.py)"
  echo "  config.json   : JSON file with workflow parameters"
  exit 1
fi

JOB_SCRIPT="$1"
CONFIG_JSON="$2"

# Node list from PBS or fallback
NODE_FILE=${PBS_NODEFILE:-nodes.txt}
nodes=( $(cat "$NODE_FILE") )
JM_HOST="${nodes[0]}"

JOB_ROOT_DIR="/home/ndhai/diaspora/src/flink"
JOB_PATH_HOST="${JOB_PATH_HOST:-$JOB_ROOT_DIR/workloads/aps-mini-apps/$JOB_SCRIPT}"    # host path
JOB_PATH_CONT="${JOB_PATH_CONT:-/opt/workloads/aps-mini-apps/$JOB_SCRIPT}"             # container path
PY_IN_CONT="${PY_IN_CONT:-python3}"                                                    # python inside container
FLINK_BIN="${FLINK_BIN:-/opt/flink/bin/flink}"                                         # flink inside container

# Verify host-side job exists
if [[ -f "$JOB_PATH_HOST" ]]; then
  echo "[submit] Using shared job at $JOB_PATH_HOST"
else
  echo "[submit] ERROR: JOB_PATH_HOST not found: $JOB_PATH_HOST"
  exit 1
fi

# Verify config exists
if [[ ! -f "$CONFIG_JSON" ]]; then
  echo "[submit] ERROR: Config JSON not found: $CONFIG_JSON"
  exit 1
fi

# Build -pyargs from JSON on the login node (no jq required)
# - Converts { "k": v } to "--k v", keeping numbers as numbers and quoting strings.
# - If simulation_file is relative, prefixes the container base path.
PYARGS=$(
  python3 - "$CONFIG_JSON" <<'PY'
import json, sys, shlex, os
cfg_path = sys.argv[1]
with open(cfg_path, 'r') as f:
    d = json.load(f)

# Container base where workloads/aps-mini-apps is visible:
CONT_BASE = "/opt/workloads/aps-mini-apps"

args = []
for k, v in d.items():
    key = f"--{k.replace('_','-')}"  # --ntask-sirt style (or keep underscores if your parser expects them)
    if k == "simulation_file":
        # rewrite relative paths to container-visible path
        if not os.path.isabs(v):
            v = os.path.normpath(os.path.join(CONT_BASE, v.lstrip("./")))
    if isinstance(v, bool):
        # pass as 1/0 or true/false depending on your argparse
        v = "1" if v else "0"
    elif isinstance(v, (int, float)):
        v = str(v)
    else:
        v = str(v)
    args.extend([key, v])
print(" ".join(shlex.quote(x) for x in args))
PY
)

echo "[submit] Built pyargs: $PYARGS"

# Remote command executed on JM host
read -r -d '' REMOTE <<'EOS' || true
set -euo pipefail
cd "$HOME" && source load-apptainer.sh

INST=$(apptainer instance list | awk 'NR>1 {print $1}' | grep -i 'flink-jobmanager' | head -n1 || true)
if [[ -z "$INST" ]]; then
  echo "[submit/JM] No running Apptainer instance matching /flink-jobmanager/"; exit 1
fi
echo "[submit/JM] Using instance: $INST"

# Run inside the container
apptainer exec --cleanenv instance://"$INST" bash -lc '
  set -euo pipefail
  export JOB_PATH_CONT="__JOB_PATH_CONT__"
  export PY_IN_CONT="__PY_IN_CONT__"
  export FLINK_BIN="__FLINK_BIN__"
  export PYARGS="__PYARGS__"

  if [[ ! -f "$JOB_PATH_CONT" ]]; then
    echo "[submit/cont] job.py not found at: $JOB_PATH_CONT"; exit 1
  fi

  export PATH="/opt/flink/bin:$PATH"
  echo "[submit/cont] flink version: $(flink --version | head -n1)"
  echo "[submit/cont] submitting job: $JOB_PATH_CONT"
  echo "[submit/cont] pyargs: $PYARGS"
  "$FLINK_BIN" run -py "$JOB_PATH_CONT" --pyExecutable "$PY_IN_CONT" -p 1 -pyargs "$PYARGS"
'
EOS

# Safely inject variable values into the REMOTE heredoc
escape() { printf '%s' "$1" | sed "s/'/'\\\\''/g"; }
REMOTE=${REMOTE//__JOB_PATH_CONT__/$(escape "$JOB_PATH_CONT")}
REMOTE=${REMOTE//__PY_IN_CONT__/$(escape "$PY_IN_CONT")}
REMOTE=${REMOTE//__FLINK_BIN__/$(escape "$FLINK_BIN")}
REMOTE=${REMOTE//__PYARGS__/$(escape "$PYARGS")}

echo "[submit] Submitting from login node to JM host: $JM_HOST"
ssh -o BatchMode=yes "$JM_HOST" "$REMOTE"
echo "[submit] Done. Check Flink UI on the JM (:8081) and logs if needed."
