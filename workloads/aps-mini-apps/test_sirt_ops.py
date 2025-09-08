from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, RuntimeContext
import numpy as np
import sirt_ops

# --- SIRT config and minimal metadata ---
SETUP_META = {
    "task_id": 0,
    "n_sinograms": 1,
    "n_rays_per_proj_row": 1024,
    "beg_sinograms": 0,
    "tn_sinograms": 1,
    "window_step": 1,
    "thread_count": 1
}
CFG = {"rank": 0, "center": 0, "window_iter": 1, "write_freq": 1}
META_IN = {
    "Type": "DATA",
    "seq_n": "0",
    "projection_id": "0",
    "theta": "0.0",
    "center": "0.0",
}

class SirtMap(MapFunction):
    def open(self, ctx: RuntimeContext):
        # One engine per task
        self.engine = sirt_ops.SirtEngine()
        self.engine.setup(SETUP_META)

        self.n = SETUP_META["n_sinograms"] * SETUP_META["n_rays_per_proj_row"]
        # (Optional) deterministic payload per subtask
        self.subtask = ctx.get_index_of_this_subtask()

    def map(self, i: int) -> str:
        # Generate payload locally (no list serialization issues)
        rng = np.random.default_rng(seed=1234 + self.subtask * 1000 + i)
        arr = rng.random(self.n, dtype=np.float32)

        out_bytes, out_meta = self.engine.process(CFG, META_IN, arr)
        return f"i={i}, out={len(out_bytes)} bytes, iter={out_meta.get('iteration_stream','NA')}"

def run():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Source of simple ints (serializes cleanly in Flink)
    ds = env.from_collection(list(range(12)), type_info=Types.INT())

    # Run SIRT in the map and only emit strings
    ds.map(SirtMap(), output_type=Types.STRING()).print()

    env.execute("SIRT Ops Test Job")

if __name__ == "__main__":
    run()
