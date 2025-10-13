
if [[ $# -lt 4 ]]; then
  echo "Usage: $0 <FailureInjectionScript> <MeanFailureInterval> <MeanRecoverInterval> <FlinkJob> <params>"
  echo "Example: $0 taskmanager-single-failure-inject-local.sh 30 /path/to/job.jar --rateMs 100 --sourceParallelism 2 --downstreamParallelism 2 --downstreamDelayMs 0 --maxRecords 1000"
  exit 1
fi
failure_script=$1
mean_failure_interval=$2
mean_recover_interval=$3
shift 3
flink_args=( $@ )
# Node list from PBS or fallback
NODE_FILE=${PBS_NODEFILE:-nodes.txt}
if [[ ! -f $NODE_FILE ]]; then
  echo "ERROR: Node file '$NODE_FILE' not found" >&2
  exit 1
fi
mapfile -t nodes <"$NODE_FILE"

# inject failure in background
echo "Start inhjecting failures"
hostindex=0
active_dir=$PWD/../../
log_dir=$active_dir/log
bash $active_dir/$failure_script $mean_failure_interval $mean_recover_interval $active_dir $hostindex > $log_dir/failure-injector.out 2> $log_dir/failure-injector.err &
injector_pid=$!
echo "Failure injector PID is $injector_pid"

# Ensure we clean up the injector on script exit
cleanup() {
  echo "Stopping failure injector (PID $injector_pid)..."
  kill "$injector_pid" 2>/dev/null || true
  wait "$injector_pid" 2>/dev/null || true
  echo "Complete"
}
trap cleanup EXIT

# Submit the Flink job (this will block until the job finishes or fails)
echo "Submitting Flink job: ${flink_args[*]}"
bash submit.sh "${flink_args[@]}"

# When the job finishes, the trap will fire and stop the injector



