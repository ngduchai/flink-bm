#!/usr/bin/env bash
# Flink cluster startup via SSH using shared filesystem

update_config_file() {
  main_jm_host=$1
  config_file=$2
  echo "Updating jobmanager.rpc.address to $main_jm_host in $config_file"
  if command -v yq >/dev/null 2>&1; then
    yq eval ".jobmanager.rpc.address = \"$main_jm_host\"" -i "$config_file"
    yq eval ".rest.address = \"$main_jm_host\"" -i "$config_file"
  else
    sed -i "/jobmanager:/,/^[^ ]/s/^\(\s*address:\s*\).*$/\1$main_jm_host/" "$config_file"
    sed -i "/rest:/,/^[^ ]/s/^\(\s*address:\s*\).*$/\1$main_jm_host/" "$config_file"
  fi

 
}

# Directory where this script (and start-*.sh) resides
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

# Node list from PBS or fallback
NODE_FILE=${PBS_NODEFILE:-nodes.txt}
nodes=( $(cat "$NODE_FILE") )

# First node is JobManager
jm_node=${nodes[0]}
# All nodes run TaskManagers
tm_nodes=( "${nodes[@]}" )
if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <numTaskManagersPerNode>"
  exit 1
fi
taskmanager_per_node=$1

echo "JobManager node: $jm_node"
echo "TaskManager nodes: ${tm_nodes[*]}, each runs $taskmanager_per_node instances"

# Clean local logs directory
echo "Cleaning log/ directory..."
rm -rf "$SCRIPT_DIR/log"/*
echo "Logs cleaned."
echo "Cleaning conf director..."
rm -rf "$SCIPT_DIR/configs/tmp"/*
echo "conf cleaned"

# Update jobmanager.rpc.address in config.yaml
main_jm_host="${jm_node%%.*}"
update_config_file $main_jm_host $SCRIPT_DIR/configs/jobmanager-config.yaml
update_config_file $main_jm_host $SCRIPT_DIR/configs/taskmanager-config.yaml

# Start JobManager via SSH
echo "Starting JobManager on $jm_node..."
ssh "$jm_node" "cd $HOME && source load-apptainer.sh && cd $SCRIPT_DIR && bash start-jobmanager.sh $SCRIPT_DIR" &
sleep 5
echo "JobManager started."

# Start TaskManagers via SSH in background
for tm in "${tm_nodes[@]}"; do
  echo "Starting TaskManager on $tm..."
  ssh "$tm" "cd $HOME && source load-apptainer.sh && cd $SCRIPT_DIR && bash start-taskmanager.sh $taskmanager_per_node $SCRIPT_DIR"
done

echo "All TaskManagers started."


