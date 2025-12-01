#!/usr/bin/env bash
#
# failure-inject-remote.sh
# Kill a random Flink TaskManagerRunner (inside Apptainer) on a random node
# listed in the hostfile every $1 seconds.

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <MeanInterval> <RecoverInterval> <ActiveDir> <hostfile>"
  exit 1
fi

source "$HOME/load-apptainer.sh"

# counter for how many times we've killed a TM
FAIL_COUNT=0

MEAN_INTERVAL=$1
RECOVER_INTERVAL=$2
ACTIVE_DIR=$3
HOSTFILE=$4

if [[ ! -f "$HOSTFILE" ]]; then
  echo "Hostfile '$HOSTFILE' not found"
  exit 1
fi

# Read nodes from hostfile, skip empty lines and comments
mapfile -t NODES < <(grep -v '^\s*#' "$HOSTFILE" | awk 'NF {print $1}')

if (( ${#NODES[@]} == 0 )); then
  echo "No nodes found in hostfile '$HOSTFILE'"
  exit 1
fi

echo "[$(hostname)] Flink failure injector: killing a random TaskManager on a random node every ${MEAN_INTERVAL}s"

restart_task() {
  local delay=$1
  local INSTANCE_NAME=$2
  local NODE=$3

  echo "[$NODE/$INSTANCE_NAME] waiting ${delay}s before restart"
  sleep "$delay"

  local CFG_FILE="$ACTIVE_DIR/configs/tmp/$INSTANCE_NAME.yaml"
  local LOG_DIR="$ACTIVE_DIR/log/$INSTANCE_NAME"

  echo "[$NODE/$INSTANCE_NAME] restarting instance $INSTANCE_NAME"
  ssh "$NODE" "cd \"\$HOME\" && source load-apptainer.sh && cd \"$ACTIVE_DIR\" && bash start-taskmanager-instance.sh \"$LOG_DIR\" \"$CFG_FILE\" \"$INSTANCE_NAME\""
}

while true; do
  # Here we just use a fixed mean interval; replace with sampling if desired
  DELAY=$MEAN_INTERVAL
  INTERVAL=$(printf "%.3f" "$DELAY")
  echo "Killing the next TaskManager in ${INTERVAL}s"
  sleep "$INTERVAL"

  # Pick a random node from the host list
  node_idx=$(( RANDOM % ${#NODES[@]} ))
  TARGET_NODE=${NODES[$node_idx]}

  echo "Target node for this failure: $TARGET_NODE"

  # Find TaskManagerRunner instances on the target node
  mapfile -t INSTANCES < <(
    ssh "$TARGET_NODE" "cd \"\$HOME\" && source load-apptainer.sh && apptainer instance list | grep flink-taskmanager- | awk '{print \$1}'" 2>/dev/null
  )

  # Strip the prefix to get logical instance names
  INST_NMS=( "${INSTANCES[@]#flink-taskmanager-}" )

  if (( ${#INSTANCES[@]} > 0 )); then
    idx=$(( RANDOM % ${#INSTANCES[@]} ))
    inst=${INSTANCES[$idx]}
    INSTANCE_NAME=${INST_NMS[$idx]}

    FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    echo "[$(date '+%F %T')] Failure #$FAIL_COUNT on $TARGET_NODE: stopping instance $inst"

    ssh "$TARGET_NODE" "apptainer instance stop \"$inst\""

    # Sample an exponential delay for restart around RECOVER_INTERVAL
    delay=$(awk -v m="$RECOVER_INTERVAL" 'BEGIN{srand(); u=rand(); if(u<1e-9) u=1e-9; print -m * log(u)}')

    # Restart on the same node after the delay, in background
    restart_task "$delay" "$INSTANCE_NAME" "$TARGET_NODE" &
  else
    echo "[$(date '+%F %T')] No running Apptainer TM instances found on $TARGET_NODE"
  fi
done
