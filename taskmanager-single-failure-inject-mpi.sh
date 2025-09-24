#!/usr/bin/env bash
#
# failure_inject_mpi.sh
# Orchestrate the local injector across many nodes via MPI.

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <interval-sec> <recover-interval-sec> <active_dir> <target_host_index>"
  echo "Example: $0 30 20"
  exit 1
fi

HOSTFILE=${PBS_NODEFILE}
INTERVAL="$1"
RECOVER_INTERVAL="$2"
ACTIVE_DIR=$3
HOST_INDEX=$4
#HOST_INDEX=0

# Get total hosts
NUM_HOSTS=$(wc -l < "$HOSTFILE")
if (( HOST_INDEX < 0 || HOST_INDEX >= NUM_HOSTS )); then
  echo "Error: Host index $HOST_INDEX out of range (0 to $((NUM_HOSTS - 1)))"
  exit 3
fi
TARGET_HOST=$(sed -n "$((HOST_INDEX + 1))p" "$HOSTFILE")

INJECTOR_SCRIPT="$ACTIVE_DIR/taskmanager-single-failure-inject-local.sh"

echo "Launching failure injector on $TARGET_HOST in $INTERVAL seconds"

ssh \
  "$TARGET_HOST" \
  "cd $HOME && source load-apptainer.sh && cd $ACTIVE_DIR && bash $INJECTOR_SCRIPT $INTERVAL $RECOVER_INTERVAL $ACTIVE_DIR"



