#!/bin/bash

DATE=$(date +"%Y-%m-%d-%Hh%Mmin%Ssec")
TOP=/lus/eagle/projects/Diaspora/ndhai/flink/failure-injection/periodic/D${DATE}/
mkdir -p $TOP
echo $TOP > recent-run
cd $TOP
qsub -o $TOP $HOME/diaspora/src/flink/workloads/aps-mini-apps/polaris-test-failure.sh

