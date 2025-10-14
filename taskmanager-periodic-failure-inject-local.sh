#!/usr/bin/env bash
#
# failure-inject-local.sh
# Kill a random Flink TaskManagerRunner (inside Apptainer) on the local host every $1 seconds.

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <MeanInterval> <RecoverInterval> <ActiveDir>"
  exit 1
fi

source $HOME/load-apptainer.sh

# counter for how many times we've killed a TM
FAIL_COUNT=0

MEAN_INTERVAL=$1
RECOVER_INTERVAL=$2
ACTIVE_DIR=$3

echo "[$(hostname)] Flink local failure injector: killing a random TaskManager once for every ${MEAN_INTERVAL}s"

restart_task() {
  
  delay=$1
  INSTANCE_NAME=$2
  NODENAME=`hostname`
  echo "[${INSTANCE_NAME}] waiting ${delay}s before restart"
  sleep "$delay"

  CFG_FILE=$ACTIVE_DIR/configs/tmp/$INSTANCE_NAME.yaml
  LOG_DIR=$ACTIVE_DIR/log/$INSTANCE_NAME
  echo "[${inst}] restarting instance $INSTANCE_NAME"
  ssh "$NODENAME" "cd $HOME && source load-apptainer.sh && cd $ACTIVE_DIR && bash start-taskmanager-instance.sh $LOG_DIR $CFG_FILE $INSTANCE_NAME"

}

while true; do
  # sample an exponential delay:  delay = -mean * ln(U), U~Uniform(0,1)
  DELAY=$MEAN_INTERVAL
  # round to 3 decimal places
  INTERVAL=$(printf "%.3f" "$DELAY")
  echo "Kill the next TaskManager in ${INTERVAL}s"
  sleep "$INTERVAL"

  # find TaskManagerRunner PIDs (matches Java class name, even inside Apptainer)
  mapfile -t INSTANCES < <(
    apptainer instance list | grep flink-taskmanager- | awk '{ print $1 }'
  )
  INST_NMS=( "${INSTANCES[@]#flink-taskmanager-}" )

  if (( ${#INSTANCES[@]} > 0 )); then
    idx=$(( RANDOM % ${#INSTANCES[@]} ))
    inst=${INSTANCES[$idx]}

    FAIL_COUNT=$(( FAIL_COUNT + 1 ))
    echo "[$(date '+%F %T')] Failure #$FAIL_COUNT: Stopping instance: $inst"
    apptainer instance stop "$inst"

    # Wait a random delay before restart (0 to INTERVAL)
    delay=$(awk -v m="$RECOVER_INTERVAL" 'BEGIN{srand(); u=rand(); if(u<1e-9) u=1e-9; print -m * log(u)}')
    INSTANCE_NAME=${INST_NMS[$idx]}
    restart_task $delay $INSTANCE_NAME &
    #sleep 10
  else
    echo "[$(date '+%F %T')] No running Apptainer TM instances found"
  fi
done


