import sys, os, glob, argparse, logging, math, time, tempfile, zipfile, shutil
import numpy as np, h5py, dxchange, tomopy as tp
import traceback

# Only add 'common' (we'll ship a filtered zip via env.add_python_file)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMMON_DIR = os.path.join(BASE_DIR, "common")
TRACE_SERIALIZER_PATH = os.path.join(BASE_DIR, "TraceSerializer.py")

# Do NOT append common/local to sys.path (it contains a vendored 'flatbuffers' we must avoid)
if os.path.isdir(COMMON_DIR):
    sys.path.append(COMMON_DIR)
if os.path.isfile(TRACE_SERIALIZER_PATH):
    sys.path.append(BASE_DIR)

try:
    # when shipped as a zip: import from the 'common' package
    from common import TraceSerializer  # preferred
except ModuleNotFoundError:
    # when the file lives next to this script (dev/local runs)
    import TraceSerializer

from pyflink.common import Types, Configuration, Duration
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.functions import FlatMapFunction, MapFunction, KeyedProcessFunction, RuntimeContext, ProcessFunction
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend
from pyflink.datastream.execution_mode import RuntimeExecutionMode
from pyflink.datastream.functions import SourceFunction
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.functions import Partitioner


# -------------------------
# Args
# -------------------------
def parse_arguments():
    p = argparse.ArgumentParser(description='Data Acquisition Process Simulator')
    p.add_argument("--image_pv")
    p.add_argument('--ntask_sirt', type=int, default=1)
    p.add_argument('--simulation_file')
    p.add_argument('--d_iteration', type=int, default=1)
    p.add_argument('--iteration_sleep', type=float, default=0.0)
    p.add_argument('--proj_sleep', type=float, default=0.6)
    p.add_argument('--beg_sinogram', type=int, default=0)
    p.add_argument('--num_sinograms', type=int, default=0)
    p.add_argument('--num_sinogram_columns', type=int)
    p.add_argument('--num_sinogram_projections', type=int, default=1440)
    p.add_argument('--logdir', type=str, default='.')
    p.add_argument("--checksum", action='store_true', default=False)

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
    p.add_argument('--ckpt_freq', type=int, default=4)
    p.add_argument('--window_length', type=str, default="32")
    p.add_argument('--window_step', type=str, default="1")
    p.add_argument('--window_iter', type=str, default="1")
    p.add_argument('--thread_count', type=int, default=1)
    p.add_argument('--center', type=float, default=0.0)
    return p.parse_args()

# Checksum
def fnv1a32(data) -> int:
    """Compute 32-bit FNV-1a checksum for str, bytes, or numpy.ndarray."""
    # Convert input to bytes
    if isinstance(data, str):
        data = data.encode("utf-8")
    elif isinstance(data, np.ndarray):
        if not data.flags['C_CONTIGUOUS']:
            data = np.ascontiguousarray(data)
        data = data.tobytes()
    elif not isinstance(data, (bytes, bytearray)):
        raise TypeError(f"Unsupported type: {type(data)}")

    # FNV-1a 32-bit
    h = 0x811C9DC5
    for b in data:
        h ^= b
        h = (h * 0x01000193) & 0xFFFFFFFF
    return h

# -------------------------
# IO helpers
# -------------------------
def setup_simulation_data(input_f, beg_sinogram=0, num_sinograms=0):
    print(f"Loading tomography data: {input_f}")
    t0 = time.time()
    idata, flat, dark, itheta = dxchange.read_aps_32id(input_f)
    idata = np.array(idata, dtype=np.float32)

    # if num_sinograms > 0 and idata.shape[1] < num_sinograms:
    #     print(f"num_sinograms = {num_sinograms} < loaded sinograms = {idata.shape[1]}. Duplicating.")
    #     n_copies = math.ceil(num_sinograms / idata.shape[1])
    #     duplicated = np.tile(idata, (1, n_copies, 1))
    #     if duplicated.shape[1] > num_sinograms:
    #         duplicated = duplicated[:, :num_sinograms, :]
    #     idata = duplicated

    #     # loaded = idata.shape[1]
    #     # print(f"loaded sinograms = {loaded} < requested = {num_sinograms}. Duplicating.")

    #     # # Indices that wrap around the existing columns to reach the target count
    #     # idx = np.arange(num_sinograms) % loaded
    #     # # Build a slicer that selects along the given axis
    #     # slc = [slice(None)] * idata.ndim
    #     # slc[1] = idx
    #     # # np.take handles arbitrary axis; advanced indexing returns a view/copy as needed
    #     # idata = idata[tuple(slc)]
        
    #     # duplicated = np.zeros((idata.shape[0], num_sinograms, idata.shape[2]), dtype=idata.dtype)
    #     # for i in range(idata.shape[0]):
    #     #     for j in range(num_sinograms):
    #     #         for k in range(idata.shape[2]):
    #     #             duplicated[i, j, k] = idata[i, j % idata.shape[1], k]
    #     # idata = duplicated

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
# DAQ emitter (yield-style flatMap)
# -------------------------
# -------------------------
# DAQ emitter (preload in open(), warm-up first)
# -------------------------
class DaqEmitter(FlatMapFunction):
    def __init__(self, *, input_f, beg_sinogram, num_sinograms, seq0,
                 iteration_sleep, d_iteration, proj_sleep, logdir,
                 save_after_serialize=False):
        super().__init__()
        self.input_f = input_f
        self.beg_sinogram = int(beg_sinogram)
        self.num_sinograms = int(num_sinograms)
        self.seq0 = int(seq0)
        self.iteration_sleep = float(iteration_sleep)
        self.d_iteration = int(d_iteration)
        self.proj_sleep = float(proj_sleep)
        self.logdir = logdir
        self.save_after_serialize = bool(save_after_serialize)

        # set in open()
        self.serialized_data = None   # np.ndarray(dtype=object) of bytes
        self.indices = None           # np.ndarray of ints (ordering across dataset)
        self._running = True

        self.warmup = False
        self.index_state = None
        self.seq_state = None

        self.seq = self.seq0
        self.tot_transfer_size = 0
        self.t_start = time.time()
        self.last_send = self.t_start
        self.index = 0
        self.it = 0
        self._index_loaded = False

    def open(self, _ctx: RuntimeContext):
        """Load & serialize once so the first record can flow immediately."""
        try:
            print(f"[DaqEmitter.open] preparing dataset from: {self.input_f}")
            t0 = time.time()

            if str(self.input_f).endswith('.npy'):
                self.serialized_data = np.load(self.input_f, allow_pickle=True)
                print(f"[DaqEmitter.open] loaded .npy pre-serialized data "
                      f"({self.serialized_data.shape[0]} messages) in {time.time()-t0:.2f}s")
            else:
                idata, flat, dark, itheta = setup_simulation_data(
                    self.input_f, self.beg_sinogram, self.num_sinograms
                )
                self.serialized_data = serialize_dataset(idata, flat, dark, itheta)
                if self.save_after_serialize:
                    np.save(f"{self.input_f}.npy", self.serialized_data)
                # free large arrays ASAP
                del idata, flat, dark

                print(f"[DaqEmitter.open] serialized {self.serialized_data.shape[0]} messages "
                      f"in {time.time()-t0:.2f}s")

            # Precompute the access order once (keeps per-record overhead tiny)
            self.indices = ordered_subset(self.serialized_data.shape[0], 16)
            print(f"[DaqEmitter.open] precomputed index order of length {len(self.indices)}")

            # Optional initial pacing before we emit the first real element
            if self.iteration_sleep > 0:
                print(f"[DaqEmitter.open] initial iteration_sleep={self.iteration_sleep}s")
                time.sleep(self.iteration_sleep)

            # Duplicate data if needed to fit the reconstruction output
            print(f"Serialized data shape: {self.serialized_data.shape}")
            if self.num_sinograms > 0 and self.serialized_data.shape[1] < self.num_sinograms:
                print(f"num_sinograms = {self.num_sinograms} < loaded sinograms = {self.serialized_data.shape[1]}. Duplicating.")
                n_copies = math.ceil(self.num_sinograms / self.serialized_data.shape[1])
                duplicated = np.tile(self.serialized_data, (1, n_copies, 1))
                if duplicated.shape[1] > self.num_sinograms:
                    duplicated = duplicated[:, :self.num_sinograms, :]
                self.serialized_data = duplicated

            # Load index state
            index_desc = ValueStateDescriptor("daq_emitter_index_v1", Types.INT())
            seq_desc = ValueStateDescriptor("daq_emitter_seq_v1", Types.LONG())
            self.index_state = _ctx.get_state(index_desc)
            self.seq_state = _ctx.get_state(seq_desc)

        except Exception as e:
            print("[DaqEmitter.open] failed to prepare dataset:", e, file=sys.stderr)
            traceback.print_exc()
            # Let the job fail early—downstream will restore on restart
            raise

    def close(self):
        """Free references to help GC in long sessions."""
        self.serialized_data = None
        self.indices = None

    # Cooperative cancellation support (Flink may call this on cancel)
    def cancel(self):
        self._running = False

    def flat_map(self, _ignored):
        if not self._running:
            return
        
        if not self._index_loaded:
            v = self.index_state.value()
            s = self.seq_state.value()
            self.index = int(v) if v is not None else 0
            self.seq = int(s) if s is not None else self.seq0
            print(f"[DaqEmitter] start with index state: {self.index}, seq state: {self.seq}")
            self._index_loaded = True
        
        if self.warmup == False:
            self.warmup = True
            # 1) Emit a tiny warm-up record so downstream operators "open" immediately.
            warmup_md = {"Type": "WARMUP", "note": "pipeline warm-up", "ts": time.time()}
            yield [warmup_md, b"\x00"]  # 1 byte payload; downstream should ignore Type!=DATA
            return

        if self.serialized_data is None or self.indices is None:
            print("[DaqEmitter] not initialized—no data to emit", file=sys.stderr)
            return

        # 2) Stream real data
        if self.it == self.d_iteration:
            self._running = True
            # 3) FIN marker so downstream can flush/close cleanly
            yield [{"Type": "FIN"}, b""]

            elapsed = max(1e-9, time.time() - self.t_start)
            nproj = (self.d_iteration * len(self.indices)) if self._running else (self.seq - self.seq0)
            tot_MiB = self.tot_transfer_size / (1024.0 ** 2)
            print(f"[DaqEmitter] sent={nproj} msgs size={tot_MiB:.2f} MiB "
                  f"elapsed={elapsed:.2f}s rate={tot_MiB/elapsed:.2f} MiB/s "
                  f"msgs/s={nproj/elapsed:.2f}")

        else:
            if self.index == 0:
                print(f"[DaqEmitter] iteration {self.it+1}/{self.d_iteration}")

            # now = time.time()
            # waittime_left = self.proj_sleep - (now-self.last_send)
            # if waittime_left > 0:
            #     time.sleep(waittime_left)

            md = {"index": int(self.index), "Type": "DATA", "seq_n": self.seq}
            payload = self.serialized_data[self.index]

            # minimal, useful log
            now = time.time()
            print(f"[DaqEmitter] send seq={self.seq} idx={self.index} dt={now-self.last_send:.3f}s "
                    f"size={len(payload)}")
            self.last_send = now

            self.tot_transfer_size += len(payload)
            self.seq += 1
            self.index += 1
            if self.index == len(self.indices):
                self.index = 0
                self.it += 1

            # Save index and sequence state
            self.index_state.update(self.index)
            self.seq_state.update(self.seq)
            print(f"[DaqEmitter] checkpointed index state: {self.index_state.value()}, seq state: {self.seq_state.value()}")

            yield [md, payload]

# -------------------------
# FlatMap distributor (yield-style)
# -------------------------
class DistOperator(FlatMapFunction):
    def __init__(self, args):
        super().__init__()
        self.args = args
        self.serializer = None   # set in open()
        self.running = True
        self.total_received = 0
        self.total_size = 0
        self.white_imgs, self.dark_imgs = [], []
        self.tot_white_imgs = 0
        self.tot_dark_imgs = 0
        self.seq = 0
        self.seq_state = None
        self.seq_loaded = False

    def open(self, ctx: RuntimeContext):
        self.serializer = TraceSerializer.ImageSerializer()
        try:
            import flatbuffers
            has_force = hasattr(getattr(flatbuffers, "Builder"), "ForceDefaults")
            print("[DistOperator] flatbuffers:", getattr(flatbuffers, "__version__", "unknown"),
                  "ForceDefaults?", has_force)
        except Exception as e:
            print("[DistOperator] flatbuffers probe failed:", e)

        seq_desc = ValueStateDescriptor("dist_seq_v1", Types.LONG()) 
        self.seq_state = ctx.get_state(seq_desc)
        self.seq_loaded = False

    @staticmethod
    def _msg(meta, data_bytes):
        return [meta, data_bytes]

    def prepare_data_rep_msg(self, row_id: int, seq: int, projection_id: int, theta: float,
                             center: float, data: np.ndarray) -> list:
        meta = {
            "Type": "MSG_DATA_REP",
            "row_id": str(row_id),
            "seq_n": str(seq),
            "projection_id": str(projection_id),
            "theta": str(float(theta)),
            "center": str(float(center)),
            "dtype": str(data.dtype),
        }

        if self.args.checksum:
            checksum = fnv1a32(data)
            meta["checksum"] = checksum
        else:
            meta["checksum"] = 0

        data_bytes = data.astype(np.float32, copy=False).tobytes()
        return self._msg(meta, data_bytes)

    def generate_worker_msgs(self, data: np.ndarray, dims: list, projection_id: int, theta: float,
                             n_ranks: int, center: float, seq: int) -> list:
        row, col = int(dims[0]), int(dims[1])
        assert data.size == row * col, f"Flattened data size mismatch with dims: {data.size} != {row}*{col}"
        msgs = []
        # # nsin, rem = row // n_ranks, row % n_ranks
        # # offset_rows = 0
        # # for rank in range(n_ranks):
        # #     rows_here = nsin + (1 if rank < rem else 0)
        # #     elems = rows_here * col
        # #     chunk = data[offset_rows * col:(offset_rows * col) + elems]
        # #     msgs.append(self.prepare_data_rep_msg(rank, seq, projection_id, theta, center, chunk))
        # #     offset_rows += rows_here
        # for offset_sinogram in range(self.args.num_sinograms):
        #     chunk = data[offset_sinogram*col : (offset_sinogram+1)*col]
        #     msgs.append(self.prepare_data_rep_msg(offset_sinogram, seq, projection_id, theta, center, chunk))
        nsin, rem = row // self.args.num_sinograms, row % self.args.num_sinograms
        offset_rows = 0
        for rank in range(self.args.num_sinograms):
            rows_here = nsin + (1 if rank < rem else 0)
            elems = rows_here * col
            chunk = data[offset_rows * col:(offset_rows * col) + elems]
            msgs.append(self.prepare_data_rep_msg(rank, seq, projection_id, theta, center, chunk))
            offset_rows += rows_here
        return msgs

    def flat_map(self, value):
        metadata, data = value

        if not self.seq_loaded:
            s = self.seq_state.value()
            self.seq = int(s) if s is not None else 0
            print(f"[DistOperator] start with seq state: {self.seq}")
            self.seq_loaded = True

         # Broadcast FIN to all SIRT ranks and include row_id so key_by works
        if isinstance(metadata, dict) and metadata.get("Type") == "WARMUP":
            for rank in range(int(self.args.num_sinograms)):
                yield [{"Type": "WARMUP", "row_id": str(rank)}, b""]
            return
        if metadata.get("Type") == "FIN":
            for rank in range(int(self.args.ntask_sirt)):
                yield [{"Type": "FIN", "row_id": str(rank)}, b""]
            self.running = False
            return
        if not self.running:
            return
        
        # print(f"DistOperator: Received msg: {metadata}, size {len(data)} bytes")

        sequence_id = metadata["seq_n"]
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
                                             self.args.num_sinograms, center, sequence_id)
            for i in range(len(msgs)):
                md = msgs[i][0]
                datalen = len(msgs[i][1])
                # print(f"Task {i}: seq_id {md['seq_n']} proj_id {md['projection_id']}, theta: {md['theta']} center: {md['center']}")
                print(f"DistOperator: Sent: {md}, data size: {datalen}")
                yield msgs[i]

        if read_image.Itype() is self.serializer.ITypes.White:
            self.white_imgs.extend(sub); self.tot_white_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.WhiteReset:
            self.white_imgs = []; self.white_imgs.extend(sub); self.tot_white_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.Dark:
            self.dark_imgs.extend(sub); self.tot_dark_imgs += 1
        if read_image.Itype() is self.serializer.ITypes.DarkReset:
            self.dark_imgs = []; self.dark_imgs.extend(sub); self.tot_dark_imgs += 1

        self.seq += 1
        self.seq_state.update(self.seq)
        print(f"[DistOperator] checkpointed with seq state: {self.seq_state.value()}")

# -------------------------
# Map: SIRT
# -------------------------
class SirtOperator(KeyedProcessFunction):
# class SirtOperator(ProcessFunction):
    def __init__(self, cfg, every_n: int = 1000):
        super().__init__()
        self.cfg = {
            "thread_count": int(cfg.thread_count),
            "window_step": int(cfg.window_step),
            "beg_sinogram": int(cfg.beg_sinogram),
            "center": int(cfg.center),
            "write_freq": int(cfg.write_freq),
            "window_iter": int(cfg.window_iter),
            "window_length": int(cfg.window_length),
            "num_sinogram_columns": int(cfg.num_sinogram_columns),
            "num_sinograms": int(cfg.num_sinograms),
        }
        self.args = cfg
        self.every_n = int(every_n)
        self.engine = None
        self.snap_state = None
        self.count_state = None
        self.processed_local = 0
        self._restored = False
        self.task_id = -1

    def open(self, ctx: RuntimeContext):
        print("SirtOperator initializing (keyed)...")
        # --- build engine ---
        try:
            import sirt_ops
            self.engine = sirt_ops.SirtEngine()
        except Exception as e:
            print("[SirtOperator.open] failed to import/create engine:", e, file=sys.stderr)
            traceback.print_exc()
            return

        # --- partitioning / setup ---
        try:
            self.task_id = ctx.get_index_of_this_subtask()
            num_tasks = ctx.get_number_of_parallel_subtasks()
            # total_sinograms = int(self.cfg["num_sinograms"])
            total_sinograms = 1
            n_sinograms = 1
            beg_sinogram = 0
            # nsino = total_sinograms // num_tasks
            # rem = total_sinograms % num_tasks
            # n_sinograms = nsino + (1 if self.task_id < rem else 0)
            # beg_sinogram = self.task_id * nsino + min(self.task_id, rem)

            tmetadata = {
                "task_id": self.task_id,
                "n_sinograms": n_sinograms,
                "n_rays_per_proj_row": int(self.cfg["num_sinogram_columns"]),
                "beg_sinogram": beg_sinogram,
                "tn_sinograms": total_sinograms,
                "window_step": int(self.cfg["window_step"]),
                "thread_count": int(self.cfg["thread_count"]),
                "window_length": int(self.cfg["window_length"])
            }
            import sirt_ops
            with sirt_ops.ostream_redirect():  # RAII context from pybind11
                self.engine.setup(tmetadata)
        except Exception as e:
            print("[SirtOperator.open] engine.setup failed:", e, file=sys.stderr)
            traceback.print_exc()
            return

        # --- managed state ---
        try:
            snap_desc = ValueStateDescriptor(
                "sirt_engine_snapshot_v1", Types.PICKLED_BYTE_ARRAY()
            )
            self.snap_state = ctx.get_state(snap_desc)
            count_desc = ValueStateDescriptor("processed_count_v1", Types.LONG())
            self.count_state = ctx.get_state(count_desc)
        except Exception as e:
            print("[SirtOperator.open] state init failed:", e, file=sys.stderr)
            traceback.print_exc()
            return

        print(f"SirtOperator initialized: every_n={self.every_n}, "
              f"restored_count=deferred, "
              f"thread_count={self.cfg['thread_count']}")
    
    def _maybe_restore(self):
        # Try until we either restored bytes or confirmed there's nothing to restore.
        if self._restored:
            return
        try:
            raw = self.snap_state.value()   # keyed ValueState for the current key
            cnt_state = self.count_state.value()
            # print(f"[SirtOperator]: restoring from checkpoint: count = {cnt_state}")
            # print(f"[SirtOperator]: restoring from checkpoint: self = {len(raw)}")
            if raw:
                raw_bytes = raw if isinstance(raw, (bytes, bytearray)) else bytes(raw)
                print(f"[SirtOperator]: found previous state: {len(raw_bytes)} bytes. Restoring")
                import sirt_ops
                with sirt_ops.ostream_redirect(): 
                    self.engine.restore(raw_bytes)
                print(f"[SirtOperator] restored {len(raw_bytes)} bytes from state")
                # also restore counter if present
                cnt = self.count_state.value()
                cnt = cnt_state
                self.processed_local = int(cnt) if cnt is not None else self.processed_local
            else:
                # No bytes yet for this key; don't flip the flag so we can retry
                print(f"[SirtOperator] cannot find previous state. Start from beginning")
                return
        except Exception as e:
            print("[SirtOperator] restore step failed:", e, file=sys.stderr)
            traceback.print_exc()
            # keep _restored = False to retry on the next element
        self._restored = True


    def _do_snapshot(self):
        """Snapshot engine & persist to Flink state. Crash if it fails so Flink restores."""
        try:
            import sirt_ops
            with sirt_ops.ostream_redirect(): 
                snap = self.engine.snapshot()
            snap_bytes = snap if isinstance(snap, (bytes, bytearray)) else bytes(snap)
            self.snap_state.update(snap_bytes)
            # self.snap_state.update(bytes([1, 2, 3]))
            self.count_state.update(self.processed_local)
            raw = self.snap_state.value()
            cnt = self.count_state.value()
            print(f"[SirtOperator] snapshot at {self.processed_local} tuples: {len(snap_bytes)} bytes: self = {len(raw)}, count = {cnt}")
        except Exception as e:
            print("[SirtOperator] engine.snapshot failed:", e, file=sys.stderr)
            traceback.print_exc()
            return

    def process_element(self, value, ctx):
        meta_in, payload = value
        # self._maybe_restore()
        print(f"SirtOperator: Received msg: {meta_in}, size {len(payload)} bytes")

        # FIN: persist one final snapshot then pass through
        if meta_in.get("Type") == "WARMUP":
            # print(f"SirtOperator: Received warm-up msg: {meta_in}, size {len(payload)} bytes")
            import sirt_ops
            with sirt_ops.ostream_redirect():  # RAII context from pybind11
                out_bytes, out_meta = self.engine.process(self.cfg, meta_in or {}, payload)
            yield value
            return
        if isinstance(meta_in, dict) and meta_in.get("Type") == "FIN":
            self._do_snapshot()
            yield value
            return

        # main processing
        try:
            # print(f"SirtOperator: Process: {meta_in}, first data float: {payload[0]}")
            if self.args.checksum:
                checksum = fnv1a32(payload)
                if checksum != meta_in["checksum"]:
                    print(f"SirtOperator: CORRUPTION -- checksum does not match: checksum = {checksum} --> {meta_in}, ")
                meta_in["checksum"] = str(checksum)
            import sirt_ops
            with sirt_ops.ostream_redirect():  # RAII context from pybind11
                out_bytes, out_meta = self.engine.process(self.cfg, meta_in or {}, payload)
        except Exception as e:
            print("[SirtOperator] engine.process failed. meta=", meta_in, file=sys.stderr)
            traceback.print_exc()
            return

        self.processed_local += 1

        # count-based snapshot
        if self.processed_local % self.every_n == 0:
            self._do_snapshot()

        if len(out_bytes):
            # print(f"SirtOperator: Emitting msg: {meta_in}, size {len(out_bytes)} bytes")
            # print(f"SirtOperator: Sent: {meta_in}, first data float: {out_bytes[0]}")
            iteration_stream = out_meta["iteration_stream"]
            row_id = out_meta["row_id"]
            import time
            now = time.time()
            print(f"[{now}] SirtOperator -- Task-{self.task_id}: Sent: row_id={row_id} stream={iteration_stream}")
            yield [dict(out_meta), bytes(out_bytes)]

# -------------------------
# Sink: Denoiser (yield-style)
# -------------------------
class DenoiserOperator(FlatMapFunction):
    def __init__(self, args):
        self.args = args
        self.serializer = None
        self.waiting_metadata = {}
        self.waiting_data = {}
        self.running = True
        self.waitting_state = None
        self._restored = False

    def open(self, ctx: RuntimeContext):
        self.serializer = TraceSerializer.ImageSerializer()
        self.waitting_state = ctx.get_state(
            ValueStateDescriptor("denoiser_waiting_state_v1", Types.PICKLED_BYTE_ARRAY())
        )

    def _maybe_restore(self):
        if self._restored:
            return
        snap = self.waitting_state.value()
        if snap:
            # snap is whatever you stored (dict via PICKLED), handle both dict/bytes if needed
            try:
                state_obj = snap  # already de-pickled by PICKLED_BYTE_ARRAY
                self.waiting_metadata = state_obj.get("metadata", {}) or {}
                self.waiting_data = state_obj.get("data", {}) or {}
                print(f"[DenoiserOperator]: Recover from checkpoint: metadata = {self.waiting_metadata}")
            except Exception:
                # if you had stored raw bytes earlier, optionally pickle.loads(snap)
                self.waiting_metadata, self.waiting_data = {}, {}
        self._restored = True

    def flat_map(self, value):
        self._maybe_restore()
        try:
            meta, data = value

            if meta.get("Type") == "WARMUP":
                print(f"DenoiserOperator: Received warm-up msg: {meta}, size {len(data)} bytes")
                yield (meta, data)
                return

            # Handle FIN first (FIN arrives with empty payload)
            if isinstance(meta, dict) and meta.get("Type") == "FIN":
                self.running = False
                yield ("FIN", None)
                return
            if not self.running:
                return

            if data is None or len(data) == 0:
                print("DenoiserOperator: empty/non-data message, skipping:", meta)
                return

            required = ("rank_dims_0","rank_dims_1","rank_dims_2","iteration_stream","rank")
            for k in required:
                if k not in meta:
                    print(f"DenoiserOperator: missing meta key '{k}', got:", meta)
                    return

            nproc_sirt = int(self.args.ntask_sirt)
            num_sinograms = int(self.args.num_sinograms)
            rank_dims = (int(meta["rank_dims_0"]), int(meta["rank_dims_1"]), int(meta["rank_dims_2"]))
            dd = np.frombuffer(data, dtype=np.float32, count=rank_dims[0]*rank_dims[1]*rank_dims[2]).reshape(rank_dims)

            iteration_stream = meta["iteration_stream"]
            rank = int(meta["rank"])
            row_id = int(meta["row_id"])

            if iteration_stream not in self.waiting_metadata:
                self.waiting_metadata[iteration_stream] = {}
                self.waiting_data[iteration_stream] = {}

            self.waiting_metadata[iteration_stream][row_id] = meta
            self.waiting_data[iteration_stream][row_id] = dd

            self.waitting_state.update({"metadata": self.waiting_metadata, "data": self.waiting_data})
            print(f"[DenoiserOperator]: Saved state: {self.waiting_metadata}")

            print(f"DenoiserOperator: receive data stream={iteration_stream}, count: {len(self.waiting_metadata[iteration_stream])}, need: {num_sinograms}")

            # if len(self.waiting_metadata[iteration_stream]) == nproc_sirt:
            if len(self.waiting_metadata[iteration_stream]) == num_sinograms:
                sorted_ranks = sorted(self.waiting_metadata[iteration_stream].keys())
                sorted_data = [self.waiting_data[iteration_stream][r] for r in sorted_ranks]
                os.makedirs(self.args.logdir, exist_ok=True)
                out_path = os.path.join(self.args.logdir, f"{iteration_stream}-denoised.h5")
                with h5py.File(out_path, 'w') as h5_output:
                    h5_output.create_dataset('/data', data=np.concatenate(sorted_data, axis=0))
                del self.waiting_metadata[iteration_stream]
                del self.waiting_data[iteration_stream]
                yield ("DENOISED", str(iteration_stream))
        except Exception as e:
            import sys, traceback
            print("[DenoiserOperator] exception:", e, file=sys.stderr)
            traceback.print_exc()
            raise

# -------------------------
# Ship local modules, excluding vendored flatbuffers
# -------------------------
def _ship_local_modules(env):
    """
    Add TraceSerializer.py and a filtered zip of 'common' that EXCLUDES:
      - common/local/flatbuffers/**  (so pip 'flatbuffers' is used)
    """
    # 1) Ship TraceSerializer.py (if present)
    if os.path.isfile(TRACE_SERIALIZER_PATH):
        env.add_python_file(TRACE_SERIALIZER_PATH)

    # 2) Ship a filtered zip of 'common'
    if os.path.isdir(COMMON_DIR):
        tmpdir = tempfile.mkdtemp(prefix="pyship_")
        zip_path = os.path.join(tmpdir, "common_filtered.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, dirs, files in os.walk(COMMON_DIR):
                # Skip the vendored flatbuffers tree
                rel_root = os.path.relpath(root, COMMON_DIR).replace("\\", "/")
                if rel_root.startswith(os.path.join("local", "flatbuffers").replace("\\", "/")):
                    continue
                if rel_root == "local/flatbuffers":
                    continue
                # Also skip any path containing /local/flatbuffers/ deeper down
                if "/local/flatbuffers/" in (rel_root + "/"):
                    continue
                for f in files:
                    abs_f = os.path.join(root, f)
                    rel_f = os.path.relpath(abs_f, COMMON_DIR).replace("\\", "/")
                    # Final guard against stray entries
                    if rel_f.startswith("local/flatbuffers/") or "/local/flatbuffers/" in rel_f:
                        continue
                    zf.write(abs_f, arcname=os.path.join("common", rel_f))
        env.add_python_file(zip_path)

# top-level key selector
def task_key_selector(value):
    # md = value[0] if isinstance(value, (list, tuple)) and value else {}
    [md, _] = value
    tid = int(md.get("row_id", 0))  # default 0 for FIN or unexpected msgs
    print(f"Key selector received meta: {md} --> key = {tid}")
    return int(tid)

class TaskIdPartitioner(Partitioner):
    def partition(self, key, num_partitions: int):
        # route directly to the target subtask = key
        t = int(key) % num_partitions
        print(f"SirtPartition: key = {key} --> patition = {t} (num_partitions = {num_partitions})")
        return t

class VersionProbe(MapFunction):
    def map(self, x):
        try:
            import sys, cloudpickle, google.protobuf, apache_beam, flatbuffers
            has_force = hasattr(getattr(flatbuffers, "Builder"), "ForceDefaults")
            print("[probe] python=", sys.executable,
                  " cloudpickle=", cloudpickle.__version__,
                  " protobuf=", google.protobuf.__version__,
                  " beam=", apache_beam.__version__,
                  " flatbuffers=", getattr(flatbuffers, "__version__", "unknown"),
                  " ForceDefaults?", has_force)
        except Exception as e:
            print("[probe] version check failed:", e)
        return x

class PrintProbe(MapFunction):
    def open(self, ctx: RuntimeContext):
        print(f"[PrintProbe] open subtask={ctx.get_index_of_this_subtask()} / "
              f"{ctx.get_number_of_parallel_subtasks()}")

    def map(self, value):
        meta, payload = value
        print(f"[PrintProbe] got Type={meta.get('Type')} task={meta.get('task_id')} "
              f"seq={meta.get('seq_n')} bytes={len(payload)}")
        return value


class TickerSource(SourceFunction):
    def __init__(self, period_ms: int, start: int = 0, max_count: int | None = None):
        self.period = period_ms / 1000.0
        self.start = start
        self.max_count = max_count
        self._running = True
    def run(self, ctx):
        i, sent = self.start, 0
        while self._running and (self.max_count is None or sent < self.max_count):
            ctx.collect(i)
            i += 1; sent += 1
            time.sleep(self.period)
    def cancel(self):
        self._running = False

PY_EXEC = "/opt/micromamba/envs/aps/bin/python"

def main():
    args = parse_arguments()
    # logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(message)s")

    os.environ.setdefault("PYTHONFAULTHANDLER", "1")
    os.environ.setdefault("OMP_NUM_THREADS", "1")  # reduce native threading surprises

    cfg = Configuration()
    cfg.set_string("python.client.executable", PY_EXEC)
    cfg.set_string("python.executable", PY_EXEC)
    
    cfg.set_boolean("python.fn-execution.debug.logging", True)
    os.environ.setdefault("PYTHONUNBUFFERED", "1")

    # Always stream (avoid batch blocking behavior)
    cfg.set_string("execution.runtime-mode", "STREAMING")

    # Make sure shuffles are pipelined in streaming
    # (AUTO is fine in streaming, but we lock it in)
    # cfg.set_string("execution.batch-shuffle-mode", "ALL_EXCHANGES_PIPELINED")

    ckpt_dir = "file:///mnt/ckpts/"
    cfg.set_string("state.backend.type", "rocksdb")
    # cfg.set_string("execution.checkpointing.storage", "filesystem")
    # cfg.set_string("state.backend.rocksdb.predefined-options", "SPINNING_DISK_OPTIMIZED")
    cfg.set_integer("state.backend.rocksdb.block.cache-size", 128 * 1024 * 1024)  # 128MB
    cfg.set_integer("state.backend.rocksdb.write-buffer-size", 128 * 1024 * 1024)  # 128MB
    cfg.set_integer("state.backend.rocksdb.max-write-buffer-number", 4)
    cfg.set_string("execution.checkpointing.dir", ckpt_dir)
    cfg.set_string("execution.checkpointing.savepoint-dir", ckpt_dir)
    # cfg.set_boolean("execution.checkpointing.unaligned.enabled", True)

    cfg.set_integer("execution.checkpointing.timeout", 60000)  # 1 minutes
    cfg.set_string("akka.ask.timeout", "60s")

    # How many records per Python bundle before sending to the JVM
    cfg.set_integer("python.fn-execution.bundle.size", 1)   # ↑ for throughput, ↓ for latency
    # Max time to accumulate a bundle (flush even if size not reached)
    cfg.set_integer("python.fn-execution.bundle.time", 100)    # milliseconds
    cfg.set_integer("python.fn-execution.arrow.batch.size", 1)  # smallest Arrow batch


    env = StreamExecutionEnvironment.get_execution_environment(cfg)

    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    ck = env.get_checkpoint_config()
    ck.set_checkpoint_timeout(15 * 60 * 1000)          # 15 min timeout
    # ck.set_max_concurrent_checkpoints(1)               # avoid overlaps
    # ck.set_min_pause_between_checkpoints(5 * 1000)     # 5s pause

    # Comment out because we use custom partitioning
    # ck.enable_unaligned_checkpoints(True)              # helps under backpressure
    # ck.set_aligned_checkpoint_timeout(Duration.of_seconds(0))        # switch to unaligned if align >3s

    # env.disable_operator_chaining()
    # env.set_buffer_timeout(100)

    _ship_local_modules(env)

    for whl in glob.glob("./dist/sirt_ops-0.2.0-*.whl"):
        env.add_python_file(whl)

    rows_per_second = max(1, int(round(1.0 / max(args.proj_sleep, 1e-9)))) 
    total_rows = args.d_iteration * args.num_sinogram_projections + 2 #  extra for warmup and FIN
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    print(f"row_per_second: {rows_per_second}")

    ddl = f"""
    CREATE TEMPORARY TABLE tick_src (
    seq BIGINT
    ) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '{rows_per_second}',
    'fields.seq.kind' = 'sequence',
    'fields.seq.start' = '0',
    'fields.seq.end' = '{total_rows - 1}'
    )
    """
    t_env.execute_sql(ddl)

    kick = t_env.to_data_stream(t_env.from_path("tick_src")) \
            .map(lambda row: int(row[0]), output_type=Types.LONG()) \
            .name("Ticker") \
            .set_parallelism(1) \
            # .slot_sharing_group("ticker")
    
    daq = kick.key_by(lambda _: 0, key_type=Types.INT()) \
        .flat_map(
        DaqEmitter(
            input_f=args.simulation_file,
            beg_sinogram=args.beg_sinogram,
            num_sinograms=args.num_sinograms,
            seq0=0,
            iteration_sleep=args.iteration_sleep,
            d_iteration=args.d_iteration,
            proj_sleep=args.proj_sleep,
            logdir=args.logdir,
            save_after_serialize=False
        ),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("DAQ Emitter").set_parallelism(1) \
        # .disable_chaining().start_new_chain() \
        # .slot_sharing_group("daq")

    # probe = daq.map(VersionProbe(), output_type=Types.PICKLED_BYTE_ARRAY()).name("Version Probe")
    # dist = probe.flat_map(
    dist = daq.key_by(lambda _: 0, key_type=Types.INT())  \
        .flat_map(
        DistOperator(args),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Data Distributor").set_parallelism(1) \
        # .disable_chaining().start_new_chain() \
        # .slot_sharing_group("dist")

    # probe = dist.key_by(
    #     task_key_selector,
    #     key_type=Types.INT()
    # ).map(
    #     PrintProbe(),
    #     output_type=Types.PICKLED_BYTE_ARRAY()
    # ).name("Probe after keyBy").disable_chaining()

    # # then feed SIRT from probe instead of directly from keyed
    # sirt = probe.map(
    #     SirtOperator(cfg=args),
    #     output_type=Types.PICKLED_BYTE_ARRAY()
    # ).name("SIRT Operator").set_parallelism(max(1, args.ntask_sirt)).disable_chaining()

    # # route by task_id so record goes to subtask = task_id
    # routed = dist.partition_custom(TaskIdPartitioner(), task_key_selector) \
    #         .name("route_by_task_id") \
    #         .set_parallelism(max(1, args.ntask_sirt))
    # route by task_id so record goes to subtask = task_id
    # sirt = dist.partition_custom(TaskIdPartitioner(), task_key_selector) \
    #         .name("route_by_task_id") \
    #         .set_parallelism(max(1, args.ntask_sirt)) \
    #         .process( \
    #             SirtOperator(cfg=args, every_n=int(args.ckpt_freq)), \
    #             output_type=Types.PICKLED_BYTE_ARRAY()) \
    #         .name("Sirt Operator") \
    #         .uid("sirt-operator") \
    #         .set_parallelism(max(1, args.ntask_sirt))


    # # # sirt = routed.key_by(lambda _: 0, key_type=Types.INT()) \
    # # #     .process(SirtOperator(cfg=args, every_n=int(args.ckpt_freq)),
    # # #                 output_type=Types.PICKLED_BYTE_ARRAY()) \
    # # sirt = routed.process(
    # #         SirtOperator(cfg=args, every_n=int(args.ckpt_freq)),
    # #         output_type=Types.PICKLED_BYTE_ARRAY()) \
    sirt = dist.key_by(task_key_selector, key_type=Types.INT()) \
        .process(SirtOperator(cfg=args, every_n=int(args.ckpt_freq)),
            output_type=Types.PICKLED_BYTE_ARRAY()) \
        .name("Sirt Operator") \
        .uid("sirt-operator") \
        .set_parallelism(max(1, args.ntask_sirt)) \
        # .set_max_parallelism(max(1, args.ntask_sirt)) \
        # .disable_chaining().start_new_chain() \
        # .slot_sharing_group("sirt")


    den = sirt.key_by(lambda _: 0, key_type=Types.INT())  \
        .flat_map(
        DenoiserOperator(args),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).name("Denoiser Operator").set_parallelism(1) \
        # .disable_chaining().start_new_chain() \
        # .slot_sharing_group("den")

    den.print().name("Denoiser Sink").set_parallelism(1)

    env.execute("APS Mini-Apps Pipeline")

if __name__ == '__main__':
    main()
