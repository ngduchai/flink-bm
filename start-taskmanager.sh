if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <numTaskManagers>"
  exit 1
fi

NUM_INST=$1
#ACTIVE_DIR=$HOME/diaspora/src/flink
ACTIVE_DIR=$2
BASE_CONF="$ACTIVE_DIR/configs/taskmanager-config.yaml"
NODENAME=`hostname`
BASE_RPC_PORT=3216
BASE_REST_PORT=8180

for i in $(seq 1 $NUM_INST); do
  INSTANCE_NAME="$NODENAME-$i"
  CFG_FILE="$ACTIVE_DIR/configs/tmp/$INSTANCE_NAME.yaml"
  LOG_DIR="$ACTIVE_DIR/log/$INSTANCE_NAME"
  
  mkdir -p "$LOG_DIR"
  cp "$BASE_CONF" "$CFG_FILE"

  RPC_PORT=$(( BASE_RPC_PORT + i - 1 ))
  REST_PORT=$(( BASE_REST_PORT + i - 1 ))

  #sed -i "/rpc:/,/^[^ ]/s/^\(\s*port:\s*\).*$/\1$RPC_PORT/" "$CFG_FILE"
  #sed -i "/rest:/,/^[^ ]/s/^\(\s*port:\s*\).*$/\1$REST_PORT/" "$CFG_FILE"
  sed -i '/^taskmanager:/,/^[^ ]/ {
    /^\s\{4\}port:/ s/\(port:\s*\)[0-9]\+/\1'"$RPC_PORT"'/
  }' "$CFG_FILE"

  echo "Starting instance $INSTANCE_NAME: rpc=$RPC_PORT, rest=$REST_PORT"

#  apptainer instance start --cleanenv --fakeroot \
#	--bind $LOG_DIR:/opt/flink/log \
#	--bind $CFG_FILE:/opt/flink/conf/config.yaml \
#	--bind /soft/xalt/:/soft/xalt/ \
#	flink:latest flink-taskmanager-$INSTANCE_NAME
#
#  apptainer exec \
#        --fakeroot --cleanenv \
#	instance://flink-taskmanager-$INSTANCE_NAME /opt/flink/bin/taskmanager.sh start-foreground \
#	> /dev/null 2> /dev/null &
  bash $ACTIVE_DIR/start-taskmanager-instance.sh $LOG_DIR $CFG_FILE $INSTANCE_NAME
  sleep 5

done


