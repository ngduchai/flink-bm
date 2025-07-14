#!/bin/bash
#PBS -l select=2:system=polaris
#PBS -l walltime=01:00:00
#PBS -N Flink
#PBS -q debug-scaling

#file systems used by the job
#PBS -l filesystems=home:eagle

#Project name
#PBS -A diaspora

#DIR=$PWD
DIR=$HOME/diaspora/src/flink
echo $DIR

taskmanager_per_node=2
#workload=workloads/shuffle/FlinkStreamingShuffle.jar
#workload=workloads/join/FlinkStreamingAggregation.jar
workload=workloads/broadcast/FlinkStreamingBroadcast.jar
rateMs=100
sourceParallelism=6
downstreamParallelism=6
downstreamDelayMs=0
maxRecords=12000

failure_rates=(100 50)
recovery_delay=5

TOP=`cat $DIR/recent-run`

# Load apptainer for container execution
source $HOME/load-apptainer.sh

for failure_rate in "${failure_rates[@]}"
do
    WORKSPACE=$TOP/$failure_rate
    mkdir -p $WORKSPACE
    cd $WORKSPACE
    echo FAILURE_RATE: $failure_rate =====================================
    echo Copy execution scripts from $DIR to workspace $WORKSPACE
    rsync -av --filter='- /flink*' --filter='- /flink*/**' $DIR/* $WORKSPACE > /dev/null
    echo Start Flink cluster
    bash start-all.sh $taskmanager_per_node
    echo Run the test
    bash test-failure.sh $failure_rate $recovery_delay $workload \
    	--rateMs $rateMs \
	--sourceParallelism $sourceParallelism \
	--downstreamParallelism $downstreamParallelism \
	--downstreamDelayMs $downstreamDelayMs \
	--maxRecords $maxRecords \
	> test-log.out 2> test-log.err
    echo Stop Flink cluster
    bash stop-all.sh
    sleep 1
done
