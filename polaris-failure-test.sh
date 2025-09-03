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

taskmanager_per_node=1
workload=workloads/shuffle/FlinkStreamingShuffle.jar
#workload=workloads/join/FlinkStreamingAggregation.jar
#workload=workloads/broadcast/FlinkStreamingBroadcast.jar
ratePerSecond=100
sourceParallelism=1
sinkParallelism=1
sinkDelayMs=0
maxRecords=20000

failure_rates=(70 135)
ckptDurations=(10000 20000 40000)
recovery_delay=0

TOP=`cat $DIR/recent-run`

# Load apptainer for container execution
source $HOME/load-apptainer.sh
count=0

for failure_rate in "${failure_rates[@]}"
do
    for ckptDuration in "${ckptDurations[@]}"
    do
        WORKSPACE=$TOP/$failure_rate-$ckptDuration
        mkdir -p $WORKSPACE
        cd $WORKSPACE
        echo FAILURE_RATE: $failure_rate CKPT_DURATION: $ckptDuration =====================================
        echo Copy execution scripts from $DIR to workspace $WORKSPACE
        rsync -av --filter='- /flink*' --filter='- /flink*/**' $DIR/* $WORKSPACE > /dev/null
        echo Start Flink cluster
        bash start-all.sh $taskmanager_per_node
        echo Run the test
        bash test-single-failure.sh $failure_rate $recovery_delay $workload \
            --ratePerSecond $ratePerSecond \
            --sourceParallelism $sourceParallelism \
            --sinkParallelism $sinkParallelism \
            --sinkDelayMs $sinkDelayMs \
            --maxRecords $maxRecords \
            --ckptDuration $ckptDuration \
            > test-log.out 2> test-log.err
        echo Stop Flink cluster
        bash stop-all.sh
        sleep 1
        count=$((count + 1))
    done
done
