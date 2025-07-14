import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.typeinfo.Types;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.zip.CRC32;

public class FlinkStreamingShuffle {

    public static void main(String[] args) throws Exception {
        // parse named parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("rateMs") || !params.has("sourceParallelism") || !params.has("downstreamParallelism")) {
            System.err.println("Usage: --rateMs <ms> --sourceParallelism <int> --downstreamParallelism <int> [--maxRecords <long>]");
            return;
        }
        final long rateMs = params.getLong("rateMs");
        final int sourceParallelism = params.getInt("sourceParallelism");
        final int sinkParallelism = params.getInt("downstreamParallelism");
        final long maxRecords = params.getLong("maxRecords", Long.MAX_VALUE);

        // ensure output directory exists
        Files.createDirectories(Paths.get("outdata"));

        // number of distinct keys: 20 x source parallelism
        final int numKeys = 20 * sourceParallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source operator: generate random key-value pairs
        DataStream<Tuple2<String, Integer>> source = env
            .fromSequence(1, maxRecords)
            .map(value -> {
                Random rand = new Random(System.nanoTime() ^ value);
                String key = "key" + rand.nextInt(numKeys);
                int val = rand.nextInt(10);
                Thread.sleep(rateMs); // throttle generation
                return Tuple2.of(key, val);
            })
            .returns(Types.TUPLE(Types.STRING, Types.INT))
            .setParallelism(sourceParallelism);

        // Sink operator: compute CRC32 checksum and write to file
        FileSink<String> sink = FileSink
            .forRowFormat(new Path("outdata/shuffled-results"), new SimpleStringEncoder<String>("UTF-8"))
            .build();

        source
            .partitionCustom((key, numPartitions) -> {
                int k = key.hashCode();
                Random rand = new Random(System.nanoTime() * k);
                return Math.abs(rand.nextInt(numPartitions));
            }, value -> {
                CRC32 crc = new CRC32();
                crc.update(value.f0.getBytes(StandardCharsets.UTF_8));
                long checksum = crc.getValue();
                // Simulate CPU work
                for (int i = 0; i < 1000; i++) {
                    checksum = (checksum << 1) ^ (checksum >> 3);
                }
                return (int)checksum;  // Fixed cast syntax
            })
            .map(value -> value.toString())
            .setParallelism(sinkParallelism)
            .sinkTo(sink)
            .setParallelism(1);

        env.execute("Source-to-Sink Workflow with CRC32 Computation");
    }
}