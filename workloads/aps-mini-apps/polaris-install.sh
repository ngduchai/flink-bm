#!/usr/bin/env bash
set -euo pipefail

# --- HOST-side config you can tweak ---
HOSTFILE=${PBS_NODEFILE:?PBS_NODEFILE not set}   # PBS provides this
PYBIN=${PYBIN:-python3}                           # python inside the container

# Paths *inside the running containers* (must already be valid in those instances)
CONT_REPO=${CONT_REPO:-/opt/workloads/aps-mini-apps}  # where your repo lives in the running container
CONT_WHEELS=${CONT_WHEELS:-/tmp/wheels}               # writable dir inside the container

# (Optional) show which nodes we'll touch
NODES=$(sort -u "$HOSTFILE")
NUM_NODES=$(echo "$NODES" | wc -l | awk '{print $1}')
echo "[launch] Nodes (${NUM_NODES}):"
echo "$NODES"

# The command that will execute on each node (outside containers) to find & run in all flink* instances
NODE_CMD=$(cat <<EOF
set -euo pipefail
INSTANCES=\$(apptainer instance list | awk 'NR>1 {print \$1}' | grep -i 'flink' || true)
if [ -z "\$INSTANCES" ]; then
  echo "[node:\$(hostname)] No running instances matching /flink/; nothing to do."
  exit 0
fi

for inst in \$INSTANCES; do
  echo "[node:\$(hostname)] Installing in instance: \$inst"
  apptainer exec instance://\$inst bash -lc "
    set -euo pipefail
    export SRC='$CONT_REPO'
    export WHEEL_DIR='$CONT_WHEELS'
    export PYBIN='$PYBIN'
    cd \"\$SRC\" || { echo '[instance:\$inst] repo path not found: '\$SRC; exit 1; }
    bash ./install.sh
  " && echo "[node:\$(hostname)] OK: \$inst" || echo "[node:\$(hostname)] FAILED: \$inst"
done
EOF
)

# Run the per-node command on each node (one process per node)
mpiexec \
  -hostfile "$HOSTFILE" \
  --map-by ppr:1:node \
  -np "$NUM_NODES" \
  bash -lc "$NODE_CMD"

echo "[launch] Completed on all nodes."
