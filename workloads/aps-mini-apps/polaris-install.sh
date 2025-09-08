#!/usr/bin/env bash
set -euo pipefail

# --- HOST-side config you can tweak ---
HOSTFILE=${PBS_NODEFILE:?PBS_NODEFILE not set}   # PBS provides this
PYBIN=${PYBIN:-python3}                           # python inside the container

# Paths *inside the running containers* (must already be valid in those instances)
CONT_REPO=${CONT_REPO:-/opt/workloads/aps-mini-apps}  # where your repo lives in the running container
CONT_WHEELS=${CONT_WHEELS:-/tmp/wheels}               # writable dir inside the container

# Create a temporary de-duplicated hostfile (many PBS files repeat nodes)
UNIQ_HOSTFILE="$(mktemp)"
sort -u "$HOSTFILE" > "$UNIQ_HOSTFILE"
NUM_NODES=$(wc -l < "$UNIQ_HOSTFILE")

echo "[launch] Nodes (${NUM_NODES}):"
cat "$UNIQ_HOSTFILE"

# Command to execute on each node: run install.sh inside *every* running instance with "flink" in the name
read -r -d '' NODE_CMD <<'EOF' || true
set -euo pipefail
INSTANCES=$(apptainer instance list | awk 'NR>1 {print $1}' | grep -i 'flink' || true)
if [ -z "$INSTANCES" ]; then
  echo "[node:$(hostname)] No running instances matching /flink/; nothing to do."
  exit 0
fi

for inst in $INSTANCES; do
  echo "[node:$(hostname)] Installing in instance: $inst"
  apptainer exec instance://"$inst" bash -lc "
    set -euo pipefail
    export SRC='__CONT_REPO__'
    export WHEEL_DIR='__CONT_WHEELS__'
    export PYBIN='__PYBIN__'
    cd \"\$SRC\" || { echo '[instance:'\"$inst\"'] repo path not found: '\"\$SRC\"; exit 1; }
    bash ./install.sh
  " && echo "[node:$(hostname)] OK: $inst" || echo "[node:$(hostname)] FAILED: $inst"
done
EOF

# Inject the container-side variables into the per-node command
NODE_CMD=${NODE_CMD/__CONT_REPO__/$CONT_REPO}
NODE_CMD=${NODE_CMD/__CONT_WHEELS__/$CONT_WHEELS}
NODE_CMD=${NODE_CMD/__PYBIN__/$PYBIN}

echo "[launch] Executing on each node via mpiexec (MPICH/Hydra style: --hostfile, --ppn, -n)..."

# MPICH/Hydra: one process per node using --ppn 1, with -n equal to num unique nodes
mpiexec \
  --hostfile "$UNIQ_HOSTFILE" \
  --ppn 1 \
  -n "$NUM_NODES" \
  bash -lc "$NODE_CMD"

echo "[launch] Completed on all nodes."

# cleanup
rm -f "$UNIQ_HOSTFILE"
