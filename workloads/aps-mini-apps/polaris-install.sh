nodes=$(cat "$PBS_NODEFILE")
node_array=($nodes)
num_nodes=${#node_array[@]}

workloads_dir=$HOME/diaspora/src/flink/workloads/aps-mini-apps

echo "Install on SIRT opt on JobManager node"
mpiexec -hostfile $PBS_NODEFILE -np $num_nodes cd $workloads_dir; bash install.sh
echo "Install on SIRT opt on TaskManager nodes"
mpiexec -hostfile $PBS_NODEFILE -np $num_nodes cd $workloads_dir; bash install.sh