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
  OLD_INSTANCE_NAME=$2
  NODENAME=`hostname`
  echo "[${NODENAME}] waiting ${delay}s before restart"
  sleep "$delay"

  read RPC_PORT < "$ACTIVE_DIR/configs/tmp/$NODENAME.rpc-port"
  read COUNT < "$ACTIVE_DIR/configs/tmp/$NODENAME.count"
  COUNT=$(( COUNT + 1 ))
  RPC_PORT=$(( RPC_PORT + COUNT - 1 ))
  echo $COUNT > "$ACTIVE_DIR/configs/tmp/$NODENAME.count"
  NEW_INSTANCE_NAME=$NODENAME-$COUNT

  CFG_FILE=$ACTIVE_DIR/configs/tmp/$NEW_INSTANCE_NAME.yaml
  LOG_DIR=$ACTIVE_DIR/log/$NEW_INSTANCE_NAME
  echo "[${NODENAME}] replace instance $OLD_INSTANCE_NAME with $NEW_INSTANCE_NAME"

  cp "$ACTIVE_DIR/configs/tmp/$OLD_INSTANCE_NAME.yaml" "$CFG_FILE"
  RPC_PORT=$(cat "$ACTIVE_DIR/configs/tmp/$NEW_INSTANCE_NAME.rpc-port")
  sed -i '/^taskmanager:/,/^[^ ]/ {
    /^\s\{4\}port:/ s/\(port:\s*\)[0-9]\+/\1'"$RPC_PORT"'/
  }' "$CFG_FILE"

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


