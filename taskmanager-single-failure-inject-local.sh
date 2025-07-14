#!/usr/bin/env bash
#
# failure-inject-local.sh
# Kill a random Flink TaskManagerRunner (inside Apptainer) on the local host every $1 seconds.

if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <MeanInterval> <RecoverInterval> <active_dir>"
  exit 1
fi

MEAN_INTERVAL=$1
RECOVER_INTERVAL=$2
ACTIVE_DIR=$3

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

echo "Kill the next TaskManager in ${MEAN_INTERVAL}s"
sleep "$MEAN_INTERVAL"

# find TaskManagerRunner PIDs (matches Java class name, even inside Apptainer)
mapfile -t INSTANCES < <(
  apptainer instance list | grep flink-taskmanager- | awk '{ print $1 }'
)
INST_NMS=( "${INSTANCES[@]#flink-taskmanager-}" )

if (( ${#INSTANCES[@]} > 0 )); then
  #idx=$(( RANDOM % ${#INSTANCES[@]} ))
  idx=0
  inst=${INSTANCES[$idx]}
  
  echo "[$(date '+%F %T')] Stopping instance: $inst"
  apptainer instance stop "$inst"

  # Wait  before restart (0 to INTERVAL)
  INSTANCE_NAME=${INST_NMS[$idx]}
  restart_task $RECOVER_INTERVAL $INSTANCE_NAME
else
  echo "[$(date '+%F %T')] No running Apptainer TM instances found"
fi


