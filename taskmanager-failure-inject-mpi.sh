#!/usr/bin/env bash
#
# failure_inject_mpi.sh
# Orchestrate the local injector across many nodes via MPI.

if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <failure-injection-script> <interval-sec> <recover-interval-sec> <active_dir>"
  echo "Example: $0 taskmanager-random-failure-inject-local.sh 30 20"
  exit 1
fi

HOSTFILE=${PBS_NODEFILE}
INTERVAL="$2"
RECOVER_INTERVAL="$3"
ACTIVE_DIR=$4
# Number of ranks = number of hosts
NUM_HOSTS=$(wc -l < "$HOSTFILE")

INJECTOR_SCRIPT="$(pwd)/$1"

echo "Launching failure injector on $NUM_HOSTS hosts every $INTERVAL seconds"

mpiexec \
  --hostfile "$HOSTFILE" \
  --np "$NUM_HOSTS" \
  /bin/bash -lc "bash $INJECTOR_SCRIPT $INTERVAL $RECOVER_INTERVAL $ACTIVE_DIR"



