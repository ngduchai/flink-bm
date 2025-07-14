nodes=$(cat "$PBS_NODEFILE")
node_array=($nodes)
num_nodes=${#node_array[@]}

mpiexec -hostfile $PBS_NODEFILE -np $num_nodes bash stop-jobmanager.sh
mpiexec -hostfile $PBS_NODEFILE -np $num_nodes bash stop-taskmanager.sh


