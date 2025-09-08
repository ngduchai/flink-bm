from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction
import numpy as np
import sirt_ops

CFG = {
    "rank": 0,
    "window_iter": 1,
    "write_freq": 1,
    "center": 1,
}
META_IN = {"Type": "DATA", "seq_n": "0", "projection_id": "0", "theta": "0.0", "center": "0.0"}

# Prepare payloads as **Python floats**, not numpy.float32 scalars
def make_payloads(n):
    return [np.random.rand(n).astype("float32").tolist() for _ in range(12)]

payloads = make_payloads(1024)

class SirtMap(MapFunction):
    def __init__(self):
        self.engine = sirt_ops.SirtEngine()
        # You still call setup from Python side
        self.engine.setup({
            "n_sinograms": 4,
            "n_rays_per_proj_row": 1024,
            "beg_sinograms": 0,
            "tn_sinograms": 4,
            "window_step": 1,
        })

    def map(self, value):
        # Convert to numpy.float32 here
        arr = np.asarray(value, dtype=np.float32)
        out_bytes, out_meta = self.engine.process(CFG, META_IN, arr)
        return f"out={len(out_bytes)} bytes, iter={out_meta.get('iteration_stream', 'NA')}"

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # IMPORTANT: use LIST(Types.FLOAT()), not PRIMITIVE_ARRAY
    ds = env.from_collection(payloads, type_info=Types.LIST(Types.FLOAT()))

    ds.map(SirtMap()).print()

    env.execute("SIRT Ops Test Job")

if __name__ == "__main__":
    run()
