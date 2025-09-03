from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
import os

def main():
    # Create streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Source: a simple in-memory collection
    data = env.from_collection(
        collection=["Hello", "PyFlink", "Cluster"],
        type_info=Types.STRING()
    )

    # Transformation (optional, just to test operator chain)
    uppercased = data.map(lambda x: x.upper(), output_type=Types.STRING())

    # Sink: print to stdout (will appear in TaskManager logs)
    uppercased.print()

    # Alternatively, write to a file in a mounted dir
    out_dir = "outdata/test_job_output"
    if not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    sink = StreamingFileSink.for_row_format(
        out_dir,
        SimpleStringEncoder()
    ).build()
    uppercased.add_sink(sink)

    # Execute the job
    env.execute("pyflink_test_job")

if __name__ == "__main__":
    main()