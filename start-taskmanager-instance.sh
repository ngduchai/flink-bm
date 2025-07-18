if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <LogDir> <CfgFile> <Instance-Name>"
  exit 1
fi

LOG_DIR=$1
CFG_FILE=$2
INSTANCE_NAME=$3
img_dir=$HOME/diaspora/src/flink

apptainer instance start --cleanenv --fakeroot \
        --bind $LOG_DIR:/opt/flink/log \
        --bind $CFG_FILE:/opt/flink/conf/config.yaml \
        --bind /soft/xalt/:/soft/xalt/ \
        $img_dir/flink_img flink-taskmanager-$INSTANCE_NAME

apptainer exec \
        --fakeroot --cleanenv \
        instance://flink-taskmanager-$INSTANCE_NAME /opt/flink/bin/taskmanager.sh start-foreground \
        > /dev/null 2> /dev/null &


