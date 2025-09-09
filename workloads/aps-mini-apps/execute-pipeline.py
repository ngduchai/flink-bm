import sys, os, glob, argparse, logging, math, time
import numpy as np, h5py, dxchange, tomopy as tp

sys.path.append(os.path.join(os.path.dirname(__file__), 'common'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'common/local'))
import TraceSerializer
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import SourceFunction, FlatMapFunction, MapFunction, SinkFunction, RuntimeContext
from pyflink.datastream.state import ListStateDescriptor


# -------------------------
# Args
# -------------------------
def parse_arguments():
    p = argparse.ArgumentParser(description='Data Acquisition Process Simulator')
    p.add_argument("--image_pv")
    p.add_argument('--ntask_sirt', type=int, default=0)
    p.add_argument('--simulation_file')
    p.add_argument('--d_iteration', type=int, default=1)
    p.add_argument('--iteration_sleep', type=float, default=0.0)
    p.add_argument('--proj_sleep', type=float, default=0.6)
    p.add_argument('--beg_sinogram', type=int, default=0)
    p.add_argument('--num_sinograms', type=int, default=0)
    p.add_argument('--num_sinogram_columns', type=int)
    p.add_argument('--num_sinogram_projections', type=int, default=1440)
    p.add_argument('--logdir', type=str, default='.')

    # preprocessing
    p.add_argument('--degree_to_radian', action='store_true', default=False)
    p.add_argument('--mlog', action='store_true', default=False)
    p.add_argument('--uint16_to_float32', action='store_true', default=False)
    p.add_argument('--uint8_to_float32', action='store_true', default=False)
    p.add_argument('--cast_to_float32', action='store_true', default=False)
    p.add_argument('--normalize', action='store_true', default=False)
    p.add_argument('--remove_invalids', action='store_true', default=False)
    p.add_argument('--remove_stripes', action='store_true', default=False)

    # SIRT
    p.add_argument('--write_freq', type=str, default="10000")
    p.add_argument('--window_length', type=str, default="32")
    p.add_argument('--window_step', type=str, default="1")
    p.add_argument('--window_iter', type=str, default="1")
    p.add_argument('--thread_count', type=int, default=1)
    p.add_argument('--center', type=float, default=0.0)
    return p.parse_args()


# -------------------------
# IO helpers
# -------------------------
def setup_simulation_data(input_f, beg_sinogram=0, num_sinograms=0):
    print(f"Loading tomography data: {input_f}")
    t0 = time.time()
    idata, flat, dark, itheta = dxchange.read_aps_32id(input_f)
    idata = np.array(idata, dtype=np.float32)

    if num_sinograms > 0 and idata.shape[1] < num_sinograms:
        print(f"num_sinograms = {num_sinograms} < loaded sinograms = {idata.shape[1]}. Duplicating.")
        n_copies = math.ceil(num_sinograms / idata.shape[1])
        duplicated = np.tile(idata, (1, n_copies, 1))
        if duplicated.shape[1] > num_sinograms:
            duplicated = duplicated[:, :num_sinograms, :]
        idata = duplicated

    flat = None if flat is None else np.array(flat, dtype=np.float32)
    dark = None if dark is None else np.array(dark, dtype=np.float32)
    itheta = None if itheta is None else np.array(itheta, dtype=np.float32)
    itheta = itheta * 180 / np.pi

    print(f"Projection dataset IO time={time.time()-t0:.2f}; "
          f"dataset shape={idata.shape}; size={idata.size}; Theta shape={itheta.shape};")
    return idata, flat, dark, itheta


def serialize_dataset(idata, flat, dark, itheta, seq=0):
    data, start_index, time_ser = [], 0, 0.0
    S = TraceSerializer.ImageSerializer()

    if flat is not None:
        for uniqueFlatId, flatId in zip(range(start_index, start_index + flat.shape[0]), range(flat.shape[0])):
            t0 = time.time()
            dflat = flat[flatId]
            itype = S.ITypes.WhiteReset if flatId == 0 else S.ITypes.White
            data.append(S.serialize(image=dflat, uniqueId=uniqueFlatId, itype=itype, rotation=0, seq=seq))
            time_ser += time.time() - t0
            seq += 1
        start_index += flat.shape[0]

    if dark is not None:
        for uniqueDarkId, darkId in zip(range(start_index, start_index + dark.shape[0]), range(dark.shape[0])):
            t0 = time.time()
            ddark = dark[darkId]
            itype = S.ITypes.DarkReset if darkId == 0 else S.ITypes.Dark
            data.append(S.serialize(image=ddark, uniqueId=uniqueDarkId, itype=itype, rotation=0, seq=seq))
            time_ser += time.time() - t0
            seq += 1
        start_index += dark.shape[0]

    for uniqueId, projId, rotation in zip(range(start_index, start_index + idata.shape[0]),
                                          range(idata.shape[0]), itheta):
        t0 = time.time()
        data.append(S.serialize(image=idata[projId], uniqueId=uniqueId, itype=S.ITypes.Projection,
                                rotation=rotation, seq=seq))
        time_ser += time.time() - t0
        seq += 1

    print(f"Serialization time={time_ser:.2f}")
    return np.array(data, dtype=object)


def ordered_subset(max_ind, nelem):
    nsubsets = int(np.ceil(max_ind / nelem))
    all_arr = np.array([], dtype=int)
    for i in np.arange(nsubsets):
        all_arr = np.append(all_arr, np.arange(start=i, stop=max_ind, step=nsubsets))
    return all_arr.astype(int)


# -------------------------
# Source
# -------------------------
class DaqOperator(SourceFunction):
    def __init__(self, input_f, beg_sinogram=0, num_sinograms=0, seq=0,
                 slp=0.0, iteration=1, save_after_serialize=False, prj_slp=0.0, logdir="."):
        super().__init__()  # IMPORTANT: initializes PyFlink bridge
        self.input_f = input_f
        self.beg_sinogram = beg_sinogram
        self.num_sinograms = num_sinograms
        self.seq = seq
        self.slp = slp
        self.iteration = iteration
        self.save_after_serialize = save_after_serialize
        self.prj_slp = prj_slp
        self.logdir = logdir
        self.running = True

    def cancel(self):
        self.running = False

    def run(self, ctx):
        if self.input_f.endswith('.npy'):
            serialized_data = np.load(self.input_f, allow_pickle=True)
        else:
            idata, flat, dark, itheta = setup_simulation_data(self.input_f, self.beg_sinogram, self.num_sinograms)
            serialized_data = serialize_dataset(idata, flat, dark, itheta)
            if self.save_after_serialize:
                np.save(f"{self.input_f}.npy", serialized_data)
            del idata, flat, dark

        tot_transfer_size = 0
        time0 = time.time()
        indices = ordered_subset(serialized_data.shape[0], 16)

        for it in range(self.iteration):
            print(f"Current iteration over dataset: {it + 1}/{self.iteration}")
            for index in indices:
                if not self.running:
                    return
                time.sleep(self.prj_slp)
                md = {"index": int(index), "Type": "DATA", "sequence_id": self.seq}
                ctx.collect([md, serialized_data[index]])
                self.seq += 1
                tot_transfer_size += len(serialized_data[index])

        ctx.collect([{"Type": "FIN"}, bytearray(1)])

        elapsed_time = time.time() - time0
        tot_MiBs = (tot_transfer_size * 1.0) / 2 ** 20
        nproj = self.iteration * len(serialized_data)
        print(f"Sent projections: {nproj}; Size (MiB): {tot_MiBs:.2f}; Elapsed (s): {elapsed_time:.2f}")
        print(f"Rate (MiB/s): {tot_MiBs / elapsed_time:.2f}; (msg/s): {nproj / elapsed_time:.2f}")


# -------------------------
# FlatMap distributor
# -------------------------
class DistOperator(FlatMapFunction):
    def __init__(self, args):
        super().__init__()  # IMPORTANT
        self.args = args
        self.serializer = TraceSerializer.ImageSerializer()
        self.running = True
        self.total_received = 0
        self.total_size = 0
        self.white_imgs, self.dark_imgs = [], []
        self.tot_white_imgs = 0
        self.tot_dark_imgs = 0
        self.seq = 0

    @staticmethod
    def _msg(meta, data_bytes):
        return [meta, data_bytes]

    def prepare_data_rep_msg(self, task_id: int, seq: int, projection_id: int, theta: float,
                             center: float, data: np.ndarray) -> list:
        meta = {
            "Type": "MSG_DATA_REP",
            "task_id": task_id,
            "seq_n": seq,
            "projection_id": projection_id,
            "theta": float(theta),
            "center": float(center),
            "dtype": str(data.dtype),
        }
        data_bytes = data.astype(np.float32, copy=False).tobytes()
        return self._msg(meta, data_bytes)

    def generate_worker_msgs(self, data: np.ndarray, dims: list, projection_id: int, theta: float,
                             n_ranks: int, center: float, seq: int) -> list:
        row, col = int(dims[0]), int(dims[1])
        assert data.size == row * col, "Flattened data size mismatch with dims"
        msgs = []
        nsin, rem = row // n_ranks, row % n_ranks
        offset_rows = 0
        for rank in range(n_ranks):
            rows_here = nsin + (1 if rank < rem else 0)
            elems = rows_here * col
            chunk = data[offset_rows * col:(offset_rows * col) + elems]
            msgs.append(self.prepare_data_rep_msg(rank, seq, projection_id, theta, center, chunk))
            offset_rows += rows_here
        return msgs

    def flat_map(self, value, collector):
        metadata, data = value

        if metadata["Type"] == "FIN":
            collector.collect(value)
            self.running = False
            return
        if not self.running:
            return

        sequence_id = metadata["sequence_id"]
        self.total_received += 1
        self.total_size += len(data)

        read_image = self.serializer.deserialize(serialized_image=data)

        my_image_np = read_image.TdataAsNumpy()
        if self.args.uint8_to_float32:
            my_image_np.dtype = np.uint8
            sub = np.array(my_image_np, dtype="float32")
        elif self.args.uint16_to_float32:
            my_image_np.dtype = np.uint16
            sub = np.array(my_image_np, dtype="float32")
        elif self.args.cast_to_float32:
            my_image_np.dtype = np.float32
            sub = my_image_np
        else:
            sub = my_image_np

        sub = sub.reshape((1, read_image.Dims().Y(), read_image.Dims().X()))

        if read_image.Itype() is self.serializer.ITypes.Projection:
            rotation = read_image.Rotation()
            if self.args.degree_to_radian:
                rotation = rotation * math.pi / 180.0

            if self.args.normalize and self.tot_white_imgs > 0 and self.tot_dark_imgs > 0:
                sub = tp.normalize(sub, flat=self.white_imgs, dark=self.dark_imgs)
            if self.args.remove_stripes:
                sub = tp.remove_stripe_fw(sub, level=7, wname='sym16', sigma=1, pad=True)
            if self.args.mlog:
                sub = -np.log(sub)
            if self.args.remove_invalids:
                sub = tp.remove_nan(sub, val=0.0)
                sub = tp.remove_neg(sub, val=0.00)
                sub[np.where(sub == np.inf)] = 0.00

            data_flat = sub.flatten()
            ncols = sub.shape[2]
            theta = rotation
            projection_id = read_image.UniqueId()
            center = read_image.Center()
            dims = [self.args.num_sinograms, ncols]
            center = (dims[1] / 2.0) if center == 0.0 else center

            msgs = self.generate_worker_msgs(data_flat, dims, projection_id, theta,
                                             self.args.ntask_sirt, center, sequence_id)
            for i in range(self.args.ntask_sirt):
                md = msgs[i][0]
                print(f"Task {i}: seq_id {md['seq_n']} proj_id {md['projection_id']}, theta: {md['theta']} center: {md['center']}")
                collector.collect(msgs[i])

        if read_image.Itype() is self.serializer.ITypes.White:
            self.white_imgs.extend(sub); self.tot_white_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.WhiteReset:
            self.white_imgs = []; self.white_imgs.extend(sub); self.tot_white_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.Dark:
            self.dark_imgs.extend(sub); self.tot_dark_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.DarkReset:
            self.dark_imgs = []; self.dark_imgs.extend(sub); self.tot_dark_imgs += 1

        self.seq += 1


# -------------------------
# Map: SIRT
# -------------------------
class SirtOperator(MapFunction):
    def __init__(self, cfg):
        super().__init__()  # IMPORTANT
        self.cfg = {
            "thread_count": cfg.thread_count,
            "window_step": cfg.window_step,
            "beg_sinogram": cfg.beg_sinogram,
            "center": cfg.center,
            "write_freq": cfg.write_freq,
            "window_iter": cfg.window_iter,
            "window_length": cfg.window_length,
            "num_sinogram_columns": cfg.num_sinogram_columns,
            "num_sinograms": cfg.num_sinograms,
        }
        self.engine = None
        self.state = None

    def open(self, ctx: RuntimeContext):
        import sirt_ops
        self.engine = sirt_ops.SirtEngine()

        desc = ListStateDescriptor("sirt_state", Types.PRIMITIVE_ARRAY(Types.BYTE()))
        self.state = ctx.get_list_state(desc)

        task_id = ctx.get_index_of_this_subtask()
        num_tasks = ctx.get_number_of_parallel_subtasks()
        total_sinograms = int(self.cfg["num_sinograms"])

        nsino = total_sinograms // num_tasks
        rem = total_sinograms % num_tasks
        n_sinograms = nsino + (1 if task_id < rem else 0)
        beg_sinogram = task_id * nsino + min(task_id, rem)

        tmetadata = {
            "task_id": task_id,
            "n_sinograms": n_sinograms,
            "n_rays_per_project_row": int(self.cfg["num_sinogram_columns"]),
            "beg_sinogram": beg_sinogram,
            "tn_sinograms": total_sinograms,
            "window_step": int(self.cfg["window_step"]),
            "thread_count": int(self.cfg["thread_count"]),
        }
        self.engine.setup(tmetadata)

        saved = list(self.state.get())
        if saved:
            self.engine.restore(bytes(saved[0]))

    def map(self, value):
        meta_in, payload = value
        if isinstance(meta_in, dict) and meta_in.get("Type") == "FIN":
            return value
        out_bytes, out_meta = self.engine.process(self.cfg, meta_in or {}, payload)
        return [dict(out_meta), bytes(out_bytes)]


# -------------------------
# Sink: Denoiser
# -------------------------
class DenoiserOperator(SinkFunction):
    def __init__(self, args):
        super().__init__()  # IMPORTANT
        self.args = args
        self.serializer = TraceSerializer.ImageSerializer()
        self.waiting_metadata = {}
        self.waiting_data = {}
        self.running = True

    def invoke(self, value, context):
        meta, data = value
        if meta.get("Type") == "FIN":
            self.running = False
            return
        if not self.running:
            return

        nproc_sirt = self.args.ntask_sirt
        recon_path = self.args.logdir

        dd = np.frombuffer(data, dtype=np.float32).reshape(meta["rank_dims"])
        iteration_stream = meta["iteration_stream"]
        rank = meta["rank"]

        if iteration_stream not in self.waiting_metadata:
            self.waiting_metadata[iteration_stream] = {}
            self.waiting_data[iteration_stream] = {}
        self.waiting_metadata[iteration_stream][rank] = meta
        self.waiting_data[iteration_stream][rank] = dd

        if len(self.waiting_metadata[iteration_stream]) == nproc_sirt:
            sorted_ranks = sorted(self.waiting_metadata[iteration_stream].keys())
            sorted_data = [self.waiting_data[iteration_stream][r] for r in sorted_ranks]

            os.makedirs(recon_path, exist_ok=True)
            with h5py.File(os.path.join(recon_path, f"{iteration_stream}-denoised.h5"), 'w') as h5_output:
                h5_output.create_dataset('/data', data=np.concatenate(sorted_data, axis=0))

            del self.waiting_metadata[iteration_stream]
            del self.waiting_data[iteration_stream]


# -------------------------
# Main
# -------------------------
def main():
    args = parse_arguments()
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)

    # ship wheel if needed
    for whl in glob.glob("./dist/sirt_ops-0.2.0-*.whl"):
        env.add_python_file(whl)

    daq = env.add_source(
        DaqOperator(
            input_f=args.simulation_file,
            beg_sinogram=args.beg_sinogram,
            num_sinograms=args.num_sinograms,
            seq=0,
            slp=args.iteration_sleep,
            iteration=args.d_iteration,
            prj_slp=args.proj_sleep,
            logdir=args.logdir
        ),
        "DAQ Source",
        type_info=Types.PICKLED_BYTE_ARRAY()
    )

    dist = daq.flat_map(
        DistOperator(args),
        output_type=Types.PICKLED_BYTE_ARRAY()   # <-- correct kw
    ).name("Data Distributor")

    sirt = dist.key_by(
        lambda x: x[0]["task_id"],
        key_type_info=Types.INT()
    ).map(
        SirtOperator(cfg=args),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("SIRT Operator").set_parallelism(args.ntask_sirt)

    sirt.add_sink(DenoiserOperator(args)).name("Denoiser Sink").set_parallelism(1)

    env.execute("APS Mini-Apps Pipeline")


if __name__ == '__main__':
    main()
