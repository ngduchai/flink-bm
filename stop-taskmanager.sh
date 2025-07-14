#apptainer instance stop flink-taskmanager
apptainer instance list \
  | awk 'NR>1 {print $1}' \
  | grep 'flink-taskmanager' \
  | xargs -I{} apptainer instance stop "{}"

