import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.zip.CRC32;


public class FlinkStreamingShuffle {

    public static class DelayingSink<IN> implements Sink<IN> {
        private final Sink<IN> delegate;
        private final long delayMs;

        public DelayingSink(Sink<IN> delegate, long delayMs) {
            this.delegate = delegate;
            this.delayMs = delayMs;
        }

        @Override
        public SinkWriter<IN> createWriter(WriterInitContext initContext) throws IOException {
            // grab subtask id once at initialization
            final int thisSubtask = initContext.getTaskInfo().getIndexOfThisSubtask();
            // create the real writer
            SinkWriter<IN> writer = delegate.createWriter(initContext);

            return new SinkWriter<IN>() {
                @Override
                public void write(IN element, Context context) throws IOException, InterruptedException {
                    // only delay in subtask 0
                    if (thisSubtask == 0) {
                        Thread.sleep(delayMs);
                    }
                    writer.write(element, context);
                }

                @Override
                public void flush(boolean endOfInput) throws IOException, InterruptedException {
                    writer.flush(endOfInput);
                }

                @Override
                public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
                    writer.writeWatermark(watermark);
                }

                @Override
                public void close() throws Exception {
                    writer.close();
                }
            };
        }
    }

    public static void main(String[] args) throws Exception {
        // parse named parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        if (!params.has("ratePerSecond") || !params.has("sourceParallelism") || !params.has("sinkParallelism")) {
            System.err.println("Usage: --rateMs <ms> --sourceParallelism <int> --sinkParallelism <int> [--maxRecords <long>]");
            return;
        }
        final long ratePerSecond = params.getLong("ratePerSecond");
        final int sourceParallelism = params.getInt("sourceParallelism");
        final int sinkParallelism = params.getInt("sinkParallelism");
        final long sinkDelayMs        = params.getLong("sinkDelayMs", 0L);
        final long maxRecords = params.getLong("maxRecords", Long.MAX_VALUE);  // optional

        // ensure output directory exists
        Files.createDirectories(Paths.get("outdata"));

        // // number of distinct keys: 20 x source parallelism
        // final int numKeys = 20 * sourceParallelism;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // compute total events/sec from rateMs
        RateLimiterStrategy rateLimiterStrategy = RateLimiterStrategy.perSecond(ratePerSecond * sourceParallelism);

        DataGeneratorSource<Tuple2<String,Integer>> sourceGen =
        new DataGeneratorSource<Tuple2<String,Integer>>(
            new GeneratorFunction<Long, Tuple2<String,Integer>>() {
                private transient Random rand;
                private transient int numKeys;
    
                @Override
                public void open(SourceReaderContext ctx) {
                    rand    = new Random();
                    numKeys = 20 * ctx.currentParallelism();
                }
    
                @Override
                public Tuple2<String,Integer> map(Long value) {
                    String key = "key" + rand.nextInt(numKeys);
                    int    val = rand.nextInt(10);
                    return Tuple2.of(key, val);
                }
            },
            maxRecords,
            rateLimiterStrategy,
            TypeInformation.of(new TypeHint<Tuple2<String,Integer>>() {})
        );
        
        // custom Encoder in FileSink handles CRC + CSV formatting
        Encoder<Tuple2<String,Integer>> encoder = new Encoder<>() {
            @Override
            public void encode(Tuple2<String,Integer> element, OutputStream out) throws IOException {
                CRC32 crc = new CRC32();
                crc.update(element.f0.getBytes(StandardCharsets.UTF_8));
                long checksum = crc.getValue();
                // simulate CPU work
                for (int i = 0; i < 1_000; i++) {
                    checksum = (checksum << 1) ^ (checksum >>> 3);
                }
                String line = element.f0 + "," + element.f1 + "," + checksum + "\n";
                out.write(line.getBytes(StandardCharsets.UTF_8));
            }
        };
        FileSink<Tuple2<String,Integer>> baseSink = FileSink
            .forRowFormat(new Path("outdata/shuffled-results"), encoder)
            .build();
        Sink<Tuple2<String,Integer>> sink = new DelayingSink<>(baseSink, sinkDelayMs);

        // Shuffle data before sending to the sink
        // Source operator
        env
            .fromSource(sourceGen, WatermarkStrategy.noWatermarks(), "Custom Key-Value Source")
            .setParallelism(sourceParallelism)
            .shuffle()
            .sinkTo(sink)
            .setParallelism(sinkParallelism);


        env.execute("Source-to-Sink Workflow with Integrated Computation");
    }
}