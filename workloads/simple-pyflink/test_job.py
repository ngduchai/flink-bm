from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    src = env.from_collection(
        collection=["Hello", "PyFlink", "Cluster"],
        type_info=Types.STRING()
    )

    src.map(lambda x: x.upper(), output_type=Types.STRING()).print()

    env.execute("pyflink_smoke_test")

if __name__ == "__main__":
    main()