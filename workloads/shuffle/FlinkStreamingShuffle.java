import org.apache.flink.api.common.functions.ReduceFunction; 
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.CRC32;

public class FlinkStreamingShuffle {

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
        final int shuffleParallelism = params.getInt("downstreamParallelism");
        final long shuffleDelayMs = params.getLong("downstreamDelayMs");
        final long maxRecords = params.getLong("maxRecords", Long.MAX_VALUE);

        // ensure output directory exists
        Files.createDirectories(Paths.get("outdata"));

        // number of distinct keys: 20 x source parallelism
        final int numKeys = 20 * sourceParallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source operator: generate, throttle, and log random key-value pairs
        DataStream<Tuple2<String, Integer>> source = env
            .fromSequence(1, maxRecords)
            .map(new RichMapFunction<Long, Tuple2<String, Integer>>() {
                private transient Logger logger;
                private transient Random rand;

                @Override
                public void open(org.apache.flink.api.common.functions.OpenContext opencontext) throws Exception {
                    logger = LoggerFactory.getLogger(FlinkStreamingShuffle.class);
                    // Seed Random uniquely per subtask using high-resolution time and subtask index
                    long seed = System.nanoTime() ^ getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                    rand = new Random(seed);
                }

                @Override
                public Tuple2<String, Integer> map(Long value) throws Exception {
                    Thread.sleep(rateMs);
                    String key = "key" + rand.nextInt(numKeys);
                    int val = rand.nextInt(10);
                    // Log generated key for debugging
                    //logger.info("[Source subtask {}] Generated key: {} value: {}",
                    //            getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), key, val);
                    return Tuple2.of(key, val);
                }
            })
            .setParallelism(sourceParallelism);

        // Merge shuffle and output: apply shuffle, computation, and direct sink
        FileSink<String> sink = FileSink
            .forRowFormat(new Path("outdata/shuffled-results"), new SimpleStringEncoder<String>("UTF-8"))
            .build();

        source
            .shuffle()
            .map(new RichMapFunction<Tuple2<String, Integer>, String>() {
                @Override
                public String map(Tuple2<String, Integer> value) throws Exception {
		    // slow down if needed
		    int id = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
		    if (id == 0) {
		    	Thread.sleep(shuffleDelayMs);
		    }
                    // Compute CRC32 checksum
                    CRC32 crc = new CRC32();
                    crc.update(value.f0.getBytes(StandardCharsets.UTF_8));
                    long checksum = crc.getValue();
                    // Simulate CPU work
                    for (int i = 0; i < 1000; i++) {
                        checksum = (checksum << 1) ^ (checksum >> 3);
                    }
                    return value.f0 + "," + value.f1 + "," + checksum;
                }
            })
            .setParallelism(shuffleParallelism)
            .sinkTo(sink)
            .setParallelism(1);

        env.execute("Source-to-Shuffle Combined Workflow");
    }
}


