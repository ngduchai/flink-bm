# job.py — minimal streaming example
from pyflink.datastream import StreamExecutionEnvironment, MapFunction, RuntimeContext
from pyflink.common import Types
import sirt_ops  # your C++ extension

# Example: a config & a minimal setup meta you already use in C++
SETUP_META = {
    "n_sinograms": 10,
    "n_rays_per_proj_row": 128,
    "beg_sinograms": 0,
    "tn_sinograms": 10,
    "window_step": 2,
}

CFG = {
    "rank": 0,
    "center": 0,
    "window_iter": 2,
    "write_freq": 5,
}

META_IN = {
    "Type": "DATA",
    "seq_n": "0",
    "projection_id": "0",
    "theta": "0.0",
    "center": "0.0",
}

import numpy as np

class SirtMap(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        self.engine = sirt_ops.SirtEngine()
        self.engine.setup(SETUP_META)

    def map(self, value):
        # value is assumed to be a flat list/array of float32 sinogram samples
        arr = np.asarray(value, dtype=np.float32)
        out_bytes, out_meta = self.engine.process(CFG, META_IN, arr)
        # return some simple text so we can see output; real job would write to sink
        return f"len={len(out_bytes)}, meta={out_meta.get('iteration_stream','NA')}"

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Example source: from elements (in practice, use Kafka, files, sockets, etc.)
    # Each element here is a payload to process.
    n = SETUP_META["n_sinograms"] * SETUP_META["n_rays_per_proj_row"]
    payloads = [list(np.random.rand(n).astype(np.float32)) for _ in range(12)]
    ds = env.from_collection(payloads, type_info=Types.PRIMITIVE_ARRAY(Types.FLOAT()))

    ds.map(SirtMap(), output_type=Types.STRING()) \
      .print()

    env.execute("sirt_ops_minimal")

if __name__ == "__main__":
    run()
