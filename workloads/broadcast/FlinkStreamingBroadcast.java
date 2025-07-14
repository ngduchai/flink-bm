import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ParameterTool;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.zip.CRC32;

public class FlinkStreamingBroadcast {

    public static void main(String[] args) throws Exception {
        // parse named parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("rateMs") || !params.has("sourceParallelism") 
            || !params.has("downstreamParallelism") || !params.has("downstreamDelayMs")) {
            System.err.println("Usage: --rateMs <ms> --sourceParallelism <int>"
                + " --downstreamParallelism <int> --downstreamDelayMs <ms> [--maxRecords <long>]");
            return;
        }
        final long rateMs = params.getLong("rateMs");
        final int sourceParallelism = params.getInt("sourceParallelism");
        final int downstreamParallelism = params.getInt("downstreamParallelism");
        final long downstreamDelayMs = params.getLong("downstreamDelayMs");
        final long maxRecords = params.getLong("maxRecords", Long.MAX_VALUE);

        // ensure output directory exists
        Files.createDirectories(Paths.get("outdata"));

        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);
        // disable closure cleaning to avoid reflection issues
        env.getConfig().disableClosureCleaner();

        // Source: generate & throttle
        DataStream<Tuple2<String,Integer>> source = env
            .fromSequence(1, maxRecords)
            .map(new RichMapFunction<Long, Tuple2<String,Integer>>() {
                private transient Random rand;
                private final int numKeys = 20 * sourceParallelism;
                @Override
                public void open(OpenContext ctx) throws Exception {
                    rand = new Random(System.nanoTime() ^ getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
                }
                @Override
                public Tuple2<String,Integer> map(Long value) throws Exception {
                    Thread.sleep(rateMs);
                    String key = "key" + rand.nextInt(numKeys);
                    int val = rand.nextInt(10);
                    return Tuple2.of(key, val);
                }
            })
            .setParallelism(sourceParallelism);

        // Broadcast: each record to all downstream subtasks
        DataStream<Tuple2<String,Integer>> broadcasted = source.broadcast();

        // Combine slowdown and checksum formatting
        DataStream<String> output = broadcasted
            .map(new RichMapFunction<Tuple2<String,Integer>, String>() {
                private transient int subtask;
                @Override
                public void open(OpenContext ctx) throws Exception {
                    subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                }
                @Override
                public String map(Tuple2<String,Integer> v) throws Exception {
                    if (subtask == 0) {
                        Thread.sleep(downstreamDelayMs);
                    }
                    CRC32 crc = new CRC32();
                    crc.update(v.f0.getBytes());
                    long c = crc.getValue();
                    for (int i = 0; i < 1000; i++) {
                        c = (c << 1) ^ (c >> 3);
                    }
                    return v.f0 + "," + v.f1 + "," + c;
                }
            })
            .setParallelism(downstreamParallelism);

        // Sink: write to single file
        FileSink<String> sink = FileSink
            .forRowFormat(new Path("outdata/broadcasted-results.txt"),
                new SimpleStringEncoder<String>("UTF-8"))
            .build();

        output.sinkTo(sink).setParallelism(1);

        env.execute("Broadcast Workflow with Delay and Checksum");
    }
}


