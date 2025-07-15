import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class FlinkStreamingAggregation {

    public static void main(String[] args) throws Exception {
        // parse named parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("ratePerSecond") || !params.has("sourceParallelism") || !params.has("sinkParallelism") || !params.has("sinkDelayMs")) {
            System.err.println("Usage: --ratePerSecond <ms> --sourceParallelism <int> --sinkParallelism <int> --sinkDelayMs <ms> [--maxRecords <long>]");
            return;
        }
        final long ratePerSecond = params.getLong("ratePerSecond");
        final int sourceParallelism = params.getInt("sourceParallelism");
        final int aggParallelism = params.getInt("sinkParallelism");
        final long aggDelayMs = params.getLong("sinkDelayMs");
        final long maxRecords = params.getLong("maxRecords", Long.MAX_VALUE);

        // ensure output directory exists
        Files.createDirectories(Paths.get("outdata"));

        // number of distinct keys: 20 x source parallelism
        final int numKeys = 20 * sourceParallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // make parameters available in the runtime
        env.getConfig().setGlobalJobParameters(params);

        // Source operator: generate and throttle random key-value pairs
        DataStream<Tuple2<String, Integer>> source = env
            .fromSequence(1, maxRecords)
            .map(new RichMapFunction<Long, Tuple2<String, Integer>>() {
                private transient Random rand;

                @Override
                public void open(OpenContext openContext) throws Exception {
                    long seed = System.nanoTime() ^ getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                    rand = new Random(seed);
                }

                @Override
                public Tuple2<String, Integer> map(Long value) throws Exception {
                    Thread.sleep(ratePerSecond);
                    String key = "key" + rand.nextInt(numKeys);
                    int val = rand.nextInt(10);
                    return Tuple2.of(key, val);
                }
            })
            .setParallelism(sourceParallelism);

                // Aggregation operator: keyed sum with slowdown on subtask 0
        DataStream<Tuple2<String, Integer>> aggregated = source
            .keyBy(t -> t.f0)
            .reduce(new org.apache.flink.api.common.functions.RichReduceFunction<Tuple2<String, Integer>>() {
                private transient int subtaskIndex;
                @Override
                public void open(OpenContext openContext) throws Exception {
                    subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                }

                @Override
                public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                    if (subtaskIndex == 0) {
                        Thread.sleep(params.getLong("sinkDelayMs"));
                    }
                    return Tuple2.of(t1.f0, t1.f1 + t2.f1);
                }
            })
            .setParallelism(aggParallelism);

        // Map to string for output
        DataStream<String> output = aggregated
            .map(new MapFunction<Tuple2<String, Integer>, String>() {
                @Override
                public String map(Tuple2<String, Integer> value) {
                    return value.f0 + "," + value.f1;
                }
            })
            .setParallelism(aggParallelism);

        // Sink: append tuples to a single file
        FileSink<String> sink = FileSink
            .forRowFormat(new Path("outdata/aggregated-results.txt"), new SimpleStringEncoder<String>("UTF-8"))
            .build();

        output
            .sinkTo(sink)
            .setParallelism(1);

        env.execute("Source-to-Aggregation with Slowdown on Subtask 0");
    }
}



