# flink-bm
Measure flink resilient capability with APS mini-app.
About the original APS mini-app, please check this [repo](https://github.com/diaspora-project/aps-mini-apps).

# Installation

Build Apptainer image.

```
apptainer build --fakeroot flink_img flink-sb.def
```

# On Polaris

TL;DR: go to `workloads/aps-mini-apps`, try
```
cd ../..; bash stop-all.sh; bash start-all.sh 1; cd workloads/aps-mini-apps; bash polaris-install.sh; bash test-single-failure.sh 10000 1 execute-pipeline.py params.json &
```

Step-by-step instructions: Start Flink cluster

```
bash start-all.sh <number of task slots per task manager>
```

Stop Flink cluster

```
bash stop-all.sh
```

Install workflow component into the Flink cluster

```
cd workloads/aps-mini-apps
bash polaris-install.sh
```

Before running experiment, check its configuration in `params.json`.
To run experiment with failure injection, try

```
 bash test-single-failure.sh <MTTF> <reconstruction parallelism> execute-pipeline.py params.json
```



