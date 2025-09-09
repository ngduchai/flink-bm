import numpy as np, sirt_ops
eng = sirt_ops.SirtEngine()
SETUP_META = {
    "task_id": 0,
    "n_sinograms": 1,
    "n_rays_per_proj_row": 1024,
    "beg_sinograms": 0,
    "tn_sinograms": 1,
    "window_step": 1,
    "thread_count": 1
}
eng.setup(SETUP_META)
# minimal payload size your C++ expects is n_sinograms*n_rays_per_proj_row
payload = np.arange(SETUP_META["n_sinograms"]*SETUP_META["n_rays_per_proj_row"], dtype=np.float32)
cfg = {"rank": 0, "center": 512, "window_iter": 1, "write_freq": 1}
meta = {
    "Type":"DATA",
    "seq_n":"0",
    "projection_id":"0",
    "theta":"0.0",
    "center":"0.0"
    }
print("calling process...")
out_bytes, out_meta = eng.process(cfg, meta, payload)
print("process #1 ok:", len(out_bytes), out_meta)
out_bytes, out_meta = eng.process(cfg, meta, payload)
print("process #2 ok:", len(out_bytes), out_meta)