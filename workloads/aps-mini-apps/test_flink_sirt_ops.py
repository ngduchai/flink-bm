from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import MapFunction, RuntimeContext
import traceback
import numpy as np
import sirt_ops

# --- SIRT config and minimal metadata ---
SETUP_META = {
    "task_id": 0,
    "n_sinograms": 1,
    "n_rays_per_proj_row": 1024,
    "beg_sinogram": 0,
    "tn_sinograms": 1,
    "window_step": 4,
    "thread_count": 4,
    "window_length": 4
}
CFG ={
    "thread_count": 2,
    "window_step": 4,
    "beg_sinogram": 0,
    "center": 10,
    "write_freq": 4,
    "window_iter": 4,
    "window_length": 4,
    "num_sinogram_columns": 1024,
    "num_sinograms": 1
}
META_IN = {
    "Type":"DATA",
    "seq_n":"0",
    "projection_id":"0",
    "theta":"0.0",
    "center":"0.0"
}

class SirtMap(MapFunction):
    def open(self, runtime_ctx):
        try:
            import sirt_ops
            self.engine = sirt_ops.SirtEngine()
            with sirt_ops.ostream_redirect():
                self.engine.setup(SETUP_META)  # this now validates keys in the binding
        except Exception as e:
            print("[SirtOperator.open] failed to import/create engine:", e, file=sys.stderr)
            traceback.print_exc()
            return

    def map(self, x):
        try:
            cfg = CFG
            meta = META_IN
            # x is a list[float]; send as numpy float32 for contiguous buffer
            try:
                import numpy as np
                import sirt_ops
                payload = np.asarray(x, dtype=np.float32)
                with sirt_ops.ostream_redirect():
                    out_bytes, out_meta = self.engine.process(cfg, meta, payload)
                return f"ok {len(out_bytes)}"
            except Exception as e:
                print("[SirtOperator] engine.process failed. meta=", meta, file=sys.stderr)
                traceback.print_exc()
                return "error"
        except Exception as e:
            # Force a Python traceback to go into TM logs:
            import traceback, sys
            traceback.print_exc()
            sys.stderr.flush()
            raise

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
