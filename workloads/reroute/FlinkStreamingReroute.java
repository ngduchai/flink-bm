import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.zip.CRC32;

public class FlinkStreamingReroute {

    /** Delay‐only‐subtask‐0 sink, unchanged. */
    public static class DelayingSink<IN> implements Sink<IN> {
        private final Sink<IN> delegate;
        private final long delayMs;
        public DelayingSink(Sink<IN> delegate, long delayMs) {
            this.delegate = delegate;
            this.delayMs = delayMs;
        }
        @Override
        public SinkWriter<IN> createWriter(WriterInitContext ctx) throws IOException {
            final int idx = ctx.getTaskInfo().getIndexOfThisSubtask();
            final SinkWriter<IN> writer = delegate.createWriter(ctx);
            return new SinkWriter<IN>() {
                @Override
                public void write(IN element, Context c) throws IOException, InterruptedException {
                    if (idx == 0) Thread.sleep(delayMs);
                    writer.write(element, c);
                }
                @Override public void flush(boolean end) throws IOException, InterruptedException {
                    writer.flush(end);
                }
                @Override public void writeWatermark(org.apache.flink.api.common.eventtime.Watermark w) throws IOException, InterruptedException {
                    writer.writeWatermark(w);
                }
                @Override public void close() throws Exception {
                    writer.close();
                }
            };
        }
    }

    /**
     * Partitioner that:
     *   • computes default = abs(hash(key)) % numPartitions
     *   • if default == 0 → send to 1
     *   • else → send to default
     */
    public static class ShiftZeroToOnePartitioner implements Partitioner<String> {
        @Override
        public int partition(String key, int numPartitions) {
            int p = Math.abs(key.hashCode()) % numPartitions;
            if (p == 0) {
                // reroute any default‐0 record to subtask 1
                return (1 < numPartitions) ? 1 : 0;
            } else {
                return p;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("ratePerSecond")
         || !params.has("sourceParallelism")
         || !params.has("sinkParallelism")) {
            System.err.println("Usage: --ratePerSecond <long> --sourceParallelism <int> --sinkParallelism <int>"
                    + " [--maxRecords <long> --ckptDuration <ms> --sinkDelayMs <ms>]");
            return;
        }

        long ratePerSecond   = params.getLong("ratePerSecond");
        int  srcPar          = params.getInt("sourceParallelism");
        int  sinkPar         = params.getInt("sinkParallelism");
        long sinkDelayMs     = params.getLong("sinkDelayMs", 0L);
        long maxRecords      = params.getLong("maxRecords", Long.MAX_VALUE);
        long ckptDuration    = params.getLong("ckptDuration", 0L);

        // ensure output directory
        Files.createDirectories(Paths.get("outdata"));

        // set up Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.disableOperatorChaining();
        if (ckptDuration > 0) {
            env.enableCheckpointing(ckptDuration);
            System.out.println("Checkpointing: " + ckptDuration + " ms");
        } else {
            System.out.println("Checkpointing disabled");
        }

        // 1) Source generator
        RateLimiterStrategy limiter = RateLimiterStrategy.perSecond(ratePerSecond * srcPar);
        DataGeneratorSource<Tuple2<String,Integer>> src =
            new DataGeneratorSource<>(
                new GeneratorFunction<Long, Tuple2<String,Integer>>() {
                    private transient Random rnd;
                    private transient int keys;
                    @Override public void open(SourceReaderContext ctx) {
                        rnd  = new Random();
                        keys = 20 * ctx.currentParallelism();
                    }
                    @Override public Tuple2<String,Integer> map(Long value) {
                        return Tuple2.of("key" + rnd.nextInt(keys), rnd.nextInt(10));
                    }
                },
                maxRecords,
                limiter,
                TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {})
            );

        // 2) Apply the custom partitioner in the same slot‐sharing group as source
        SingleOutputStreamOperator<Tuple2<String,Integer>> sourceStream = env
            .fromSource(src, WatermarkStrategy.noWatermarks(), "DataGenSource")
            .setParallelism(srcPar)
            .slotSharingGroup("source-group");
        DataStream<Tuple2<String,Integer>> stream = sourceStream
            .shuffle()
            .partitionCustom(
                new ShiftZeroToOnePartitioner(),
                (KeySelector<Tuple2<String,Integer>,String>) value -> value.f0
            );

        // 3) Sink writer with delay on task 0
        Encoder<Tuple2<String,Integer>> encoder = (rec, out) -> {
            CRC32 crc = new CRC32();
            crc.update(rec.f0.getBytes(StandardCharsets.UTF_8));
            long cs = crc.getValue();
            for (int i = 0; i < 1_000; i++) {
                cs = (cs << 1) ^ (cs >>> 3);
            }
            String line = rec.f0 + "," + rec.f1 + "," + cs + "\n";
            out.write(line.getBytes(StandardCharsets.UTF_8));
        };
        FileSink<Tuple2<String,Integer>> baseSink = FileSink
            .forRowFormat(new Path("outdata/custom-results.csv"), encoder)
            .build();
        Sink<Tuple2<String,Integer>> sink = new DelayingSink<>(baseSink, sinkDelayMs);

        // 4) Connect to sink
        stream
            .sinkTo(sink)
            .setParallelism(sinkPar)
            .slotSharingGroup("sink-group");

        env.execute("Source → ShiftZeroToOne → Sink");
    }
}