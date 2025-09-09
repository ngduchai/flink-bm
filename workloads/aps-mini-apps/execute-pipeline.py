import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'common'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'common/local'))
import argparse
import numpy as np
import time
import TraceSerializer
import h5py
# import dxchange
import tomopy as tp
import csv
import math
import logging

from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream.state import ListStateDescriptor
from pyflink.datastream import CheckpointingMode
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.functions import SourceFunction, FlatMapFunction, MapFunction, SinkFunction, RuntimeContext

import sirt_ops

def parse_arguments():
  parser = argparse.ArgumentParser(
          description='Data Acquisition Process Simulator')

  parser.add_argument("--image_pv", help="EPICS image PV name.")

  parser.add_argument('--ntask_sirt', type=int, default=0,
                      help='number of reconstruction tasks')

  parser.add_argument('--simulation_file', help='File name for mock data acquisition. ')

  parser.add_argument('--d_iteration', type=int, default=1,
                      help='Number of iteration on simulated data.')

  parser.add_argument('--iteration_sleep', type=float, default=0,
                      help='Delay data publishing for each iteration.')

  parser.add_argument('--proj_sleep', type=float, default=0.6,
                      help='Delay data publishing for each projection.')

  parser.add_argument('--beg_sinogram', type=int, default=0,
                      help='Starting sinogram for reconstruction.')

  parser.add_argument('--num_sinograms', type=int, default=0,
                      help='Number of sinograms to reconstruct.')

  parser.add_argument('--num_sinogram_columns', type=int,
                      help='Number of columns per sinogram.')

  parser.add_argument('--num_sinogram_projections', type=int, default=1440,
                      help='Number of projections per sinogram.')

  parser.add_argument('--logdir', type=str, default='.',
                      help='Path to save log files.')

  # Available pre-processing options
  parser.add_argument('--degree_to_radian', action='store_true', default=False,
              help='Converts rotation information to radian.')
  parser.add_argument('--mlog', action='store_true', default=False,
              help='Takes the minus log of projection data (projection data is divided by 50000 also).')
  parser.add_argument('--uint16_to_float32', action='store_true', default=False,
              help='Converts uint16 image byte sequence to float32.')
  parser.add_argument('--uint8_to_float32', action='store_true', default=False,
              help='Converts uint8 image byte sequence to float32.')
  parser.add_argument('--cast_to_float32', action='store_true', default=False,
              help='Casts incoming image byte sequence to float32.')
  parser.add_argument('--normalize', action='store_true', default=False,
              help='Normalizes incoming projection data with previously received dark and flat field.')
  parser.add_argument('--remove_invalids', action='store_true', default=False,
              help='Removes invalid measurements from incoming projections, i.e. negatives, nans and infs.')
  parser.add_argument('--remove_stripes', action='store_true', default=False,
              help='Removes stripes using fourier-wavelet method (in tomopy).')
  
  # SIRT configuration
  parser.add_argument('--write_freq', type=str, default="10000",
                        help='Write frequency')
  parser.add_argument('--window_length', type=str, default="32",
                      help='Number of projections that will be stored in the window')
  parser.add_argument('--window_step', type=str, default="1",
                      help='Number of projections that will be received in each request')
  parser.add_argument('--window_iter', type=str, default="1",
                      help='Number of iterations on received window')
  parser.add_argument('--thread_count', type=int, default="1",
                      help='Number of reconstruction threads')
  parser.add_argument('--center', type=float, default=0.0,
                      help='Rotation center for reconstruction. If 0.0, the center will be set to N/2.')
  

  return parser.parse_args()

def setup_simulation_data(input_f, beg_sinogram=0, num_sinograms=0):
  print("Loading tomography data: {}".format(input_f))
  t0=time.time()
  idata, flat, dark, itheta = dxchange.read_aps_32id(input_f)
  idata = np.array(idata, dtype=np.float32) #dtype('uint16'))

  # Make sure # sinograms does not exceed the data size
  if num_sinograms > 0:
    if idata.shape[1] < num_sinograms:
      print("num_sinograms = {} < loaded sinograms = {}. Filling by duplication.".format(num_sinograms, idata.shape[1]))
      n_copies = math.ceil(num_sinograms / idata.shape[1])
      duplicated = np.tile(idata, (1, n_copies, 1))
      if duplicated.shape[1] > num_sinograms:
        duplicated = duplicated[:, :num_sinograms, :]
      idata = duplicated

  if flat is not None: flat = np.array(flat, dtype=np.float32) #dtype('uint16'))
  if dark is not None: dark = np.array(dark, dtype=np.float32) #dtype('uint16'))
  if itheta is not None: itheta = np.array(itheta, dtype=np.float32) #dtype('float32'))
  # XXX: degree_to_radian is applied twice since the dataset is already normalized. Return it back to correct values.
  itheta = itheta*180/np.pi
  print("Projection dataset IO time={:.2f}; dataset shape={}; size={}; Theta shape={};".format(
                             time.time()-t0 , idata.shape, idata.size, itheta.shape))

  return idata, flat, dark, itheta

def serialize_dataset(idata, flat, dark, itheta, seq=0):
  data = []
  start_index=0
  time_ser=0.
  serializer = TraceSerializer.ImageSerializer()

  print("Starting serialization")
  if flat is not None:
    for uniqueFlatId, flatId in zip(range(start_index,
                             start_index+flat.shape[0]), range(flat.shape[0])):
      t_ser0 =  time.time()
      #builder.Reset()
      dflat = flat[flatId]
      itype = serializer.ITypes.WhiteReset if flatId==0 else serializer.ITypes.White
      serialized_data = serializer.serialize(image=dflat,
                                             uniqueId=uniqueFlatId,
                                             itype=itype,
                                             rotation=0,
                                             seq=seq)
      data.append(serialized_data)
      time_ser += time.time()-t_ser0
      seq+=1
    start_index+=flat.shape[0]

  # dark data
  if dark is not None:
    for uniqueDarkId, darkId in zip(range(start_index, start_index+dark.shape[0]),
                                    range(dark.shape[0])):
      t_ser0 =  time.time()
      #builder.Reset()
      dflat = dark[flatId]
      #serializer = TraceSerializer.ImageSerializer(builder)
      itype = serializer.ITypes.DarkReset if darkId==0 else serializer.ITypes.Dark
      serialized_data = serializer.serialize(image=dflat,
                                             uniqueId=uniqueDarkId,
                                             itype=itype,
                                             rotation=0,
                                             seq=seq) #, center=10.)
      time_ser += time.time()-t_ser0
      seq+=1
      data.append(serialized_data)
    start_index+=dark.shape[0]

  # projection data
  for uniqueId, projId, rotation in zip(range(start_index, start_index+idata.shape[0]),
                                        range(idata.shape[0]), itheta):
    t_ser0 =  time.time()
    #builder.Reset()
    proj =  idata[projId]
    #serializer = TraceSerializer.ImageSerializer(builder)
    itype = serializer.ITypes.Projection
    serialized_data = serializer.serialize(image=proj, uniqueId=uniqueId,
                                      itype=itype,
                                      rotation=rotation, seq=seq) #, center=10.)
    time_ser += time.time()-t_ser0
    seq+=1
    data.append(serialized_data)
  print("Serialization time={:.2f}".format(time_ser))
  return np.array(data, dtype=object)

def ordered_subset(max_ind, nelem):
  nsubsets = np.ceil(max_ind/nelem).astype(int)
  all_arr = np.array([])
  for i in np.arange(nsubsets):
    all_arr = np.append(all_arr, np.arange(start=i, stop=max_ind, step=nsubsets))
  return all_arr.astype(int)


class DaqOperator(SourceFunction):
  def __init__(self,  
                 batchsize,
                 input_f,
                 beg_sinogram=0,
                 num_sinograms=0,
                 seq=0,
                 slp=0,
                 iteration=1,
                 save_after_serialize=False,
                 prj_slp=0,
                 logdir="."):
    self.batchsize = batchsize
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

    serialized_data = None
    if self.input_f.endswith('.npy'):
      serialized_data = np.load(self.input_f, allow_pickle=True)
    else:
      idata, flat, dark, itheta = setup_simulation_data(self.input_f, self.beg_sinogram, self.num_sinograms)
      serialized_data = serialize_dataset(idata, flat, dark, itheta)
      if self.save_after_serialize: np.save("{}.npy".format(self.input_f), serialized_data)
      del idata, flat, dark
    tot_transfer_size=0
    time0 = time.time()
    nelems_per_subset = 16
    indices = ordered_subset(serialized_data.shape[0],
                                nelems_per_subset)
    i = 0
    for it in range(self.iteration): # Simulate data acquisition
      print("Current iteration over dataset: {}/{}".format(it+1, self.iteration))
      for index in indices:
        # Check if signal received
        if self.runnig == False:
          return
            
        print("Sending projection {}".format(index))
        time.sleep(self.prj_slp)
        # mofka send
        ts = time.perf_counter()
        ctx.collect()
        md = {"index": int(index), "Type" : "DATA", "sequence_id": seq}
        ctx.collect([md, serialized_data[index]])
        seq+=1
        i+=1
        tot_transfer_size += len(serialized_data[index])

    # Notify end of data
    md = {"Type" : "FIN"}
    ctx.collect([md, bytearray(1)])  
    
    time1 = time.time()

    elapsed_time = time1-time0
    tot_MiBs = (tot_transfer_size*1.)/2**20
    nproj = self.iteration*len(serialized_data)
    print("Sent number of projections: {}; Total size (MiB): {:.2f}; Elapsed time (s): {:.2f}".format(nproj, tot_MiBs, elapsed_time))
    print("Rate (MiB/s): {:.2f}; (msg/s): {:.2f}".format(tot_MiBs/elapsed_time, nproj/elapsed_time))


class DistOperator(FlatMapFunction):

  def __init__(self, args):
    self.running = True
    self.serializer = TraceSerializer.ImageSerializer()
    self.args = args

  def prepare_data_rep_msg(self, task_id: int, seq: int, projection_id: int, theta: float,
                          center: float, data_size: int,
                          data: np.ndarray) -> list:
      """Prepare the data reply message similar to the C function."""
      # Create the metadata/data part of the message
      msg_metadata = {"Type": "MSG_DATA_REP",
                      "task_id": task_id,
                      "seq_n": seq,
                      "data_size": data_size,
                      "projection_id": projection_id,
                      "theta": theta,
                      "center": center,
                      "dtype": str(data.dtype)}
      msg_data = bytearray(data)
      return [msg_metadata, msg_data]

  def generate_worker_msgs(self, data: np.ndarray, dims: list, projection_id: int, theta: float,
                         n_ranks: int, center: float, seq: int) -> list:
    nsin = dims[0] // n_ranks  # Sinograms per rank
    remaining = dims[0] % n_ranks  # Remaining sinograms
    msgs = []
    curr_sinogram_id = 0
    for i in range(n_ranks):
        r = 1 if remaining > 0 else 0
        remaining -= 1
        data_size = data.dtype.itemsize*(nsin + r) * dims[1]
        # Prepare the message for the worker
        msg = self.prepare_data_rep_msg(i, seq,
                                   projection_id,
                                   theta,
                                   center,
                                   data_size,
                                   data[curr_sinogram_id*dims[1]:(curr_sinogram_id+(nsin+r))*dims[1]]
        )
        msgs.append(msg)
        curr_sinogram_id += (nsin + r)

        # metadata = msg[0]
        # print(f"Rank {i}: seq_id {metadata['seq_n']} proj_id {metadata['projection_id']}, theta: {metadata['theta']} center: {metadata['center']}")

    return msgs

  def flat_map(self, value, collector):
    metadata = value[0]
    data = value[1]
    if metadata["Type"] == "FIN":
      # Notify end of data to downstream
      collector.collect(value)
      self.running = False
    
    if not self.running: return

    sequence_id = metadata["sequence_id"]
    total_received += 1
    total_size += len(data)
    

    # Deserialize msg to image
    read_image = self.serializer.deserialize(serialized_image=data)
    self.serializer.info(read_image) # print image information

    # Push image to reconstruction tasks (REQ/REP)
    my_image_np = read_image.TdataAsNumpy()
    if self.args.uint8_to_float32:
      my_image_np.dtype = np.uint8
      sub = np.array(my_image_np, dtype="float32")
    elif self.args.uint16_to_float32:
      my_image_np.dtype = np.uint16
      sub = np.array(my_image_np, dtype="float32")
    elif self.args.cast_to_float32:
      my_image_np.dtype=np.float32
      sub = my_image_np
    else: sub = my_image_np

    sub = sub.reshape((1, read_image.Dims().Y(), read_image.Dims().X()))
    # If incoming data is projection
    if read_image.Itype() is self.serializer.ITypes.Projection:
      rotation=read_image.Rotation()
      if self.args.degree_to_radian: rotation = rotation*math.pi/180.

      # Tomopy operations expect 3D data, reshape incoming projections.
      if self.args.normalize:
        # flat/dark fields' corresponding rows
        if tot_white_imgs>0 and tot_dark_imgs>0:
          # print("normalizing: white_imgs.shape={}; dark_imgs.shape={}".format(
                  #np.array(white_imgs).shape, np.array(dark_imgs).shape))
          sub = tp.normalize(sub, flat=white_imgs, dark=dark_imgs)
      if self.args.remove_stripes:
        #print("removing stripes")
        sub = tp.remove_stripe_fw(sub, level=7, wname='sym16', sigma=1, pad=True)
      if self.args.mlog:
        #print("applying -log")
        sub = -np.log(sub)
      if self.args.remove_invalids:
        #print("removing invalids")
        sub = tp.remove_nan(sub, val=0.0)
        sub = tp.remove_neg(sub, val=0.00)
        sub[np.where(sub == np.inf)] = 0.00

      # Send to downstream
      data = sub.flatten()
      ncols = sub.shape[2]
      theta = rotation
      projection_id = read_image.UniqueId()
      center = read_image.Center()
      row = self.args.num_sinograms
      col = ncols

      dims = [row, col]
      center = (dims[1] / 2.0) if center == 0.0 else center
      msgs = self.generate_worker_msgs(data,
                                  dims,
                                  projection_id,
                                  theta,
                                  self.args.ntask_sirt,
                                  center,
                                  sequence_id)
                                  # self.seq)
      
      for i in range(self.args.ntask_sirt):
        metadata = msgs[i][0]
        print(f"Task {i}: seq_id {metadata['seq_n']} proj_id {metadata['projection_id']}, theta: {metadata['theta']} center: {metadata['center']}")
        collector.collect([msgs[i][0], msgs[i][1]])

      

    # If incoming data is white field
    if read_image.Itype() is self.serializer.ITypes.White:
      #print("White field data is received: {}".format(read_image.UniqueId()))
      white_imgs.extend(sub)
      tot_white_imgs += 1

    # If incoming data is white-reset
    if read_image.Itype() is self.serializer.ITypes.WhiteReset:
      #print("White-reset data is received: {}".format(read_image.UniqueId()))
      white_imgs=[]
      white_imgs.extend(sub)
      tot_white_imgs += 1

    # If incoming data is dark field
    if read_image.Itype() is self.serializer.ITypes.Dark:
      #print("Dark data is received: {}".format(read_image.UniqueId()))
      dark_imgs.extend(sub)
      tot_dark_imgs += 1

    # If incoming data is dark-reset
    if read_image.Itype() is self.serializer.ITypes.DarkReset:
      #print("Dark-reset data is received: {}".format(read_image.UniqueId()))
      dark_imgs=[]
      dark_imgs.extend(sub)
      tot_dark_imgs += 1
    seq+=1


class SirtOperator(MapFunction):
    def __init__(self, cfg):
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
        self.state = None   # ListState[byte[]]

    # Called once to create/restore operator state
    def initialize_state(self, context):
        desc = ListStateDescriptor("sirt_state", Types.PRIMITIVE_ARRAY(Types.BYTE()))
        self.state = context.get_operator_state_store().get_list_state(desc)
        # NOTE: actual restore happens after we create the engine in open()

    def open(self, ctx):
        import sirt_ops
        self.engine = sirt_ops.SirtEngine()

        task_id = ctx.get_index_of_this_subtask()
        num_tasks = ctx.get_number_of_parallel_subtasks()
        total_sinograms = int(self.cfg["num_sinograms"])

        nsino = total_sinograms // num_tasks
        rem   = total_sinograms % num_tasks
        r     = 1 if task_id < rem else 0
        n_sinograms  = r + nsino
        beg_sinogram = (task_id * nsino + min(task_id, rem))

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

        # restore snapshot if present
        saved = list(self.state.get())
        if saved:
            snap = bytes(saved[0])        # first (and only) item
            self.engine.restore(snap)

    def snapshot_state(self, context):
        snap = self.engine.snapshot()      # returns bytes/bytearray
        self.state.clear()
        self.state.add(bytes(snap))        # store a single byte[] element

    def map(self, value):
        meta_in, payload = value
        out_bytes, out_meta = self.engine.process(self.cfg, meta_in or {}, payload)
        return [dict(out_meta), bytes(out_bytes)]


class DenoiserOperator(SinkFunction):
  def __init__(self, args):
    self.args = args
    self.serializer = TraceSerializer.ImageSerializer()
    waiting_metadata = {}
    waiting_data = {}
    self.running = True

  def invoke(self, value, context):
    meta = value[0]
    data = value[1]
    if meta["Type"] == "FIN":
      self.running = False
    
    if not self.running: return

    nproc_sirt = self.args.ntask_sirt
    recon_path = self.args.logdir

    dd = np.frombuffer(data, dtype=np.float32)
    dd = dd.reshape(meta["rank_dims"])
    iteration_stream = meta["iteration_stream"]
    rank = meta["rank"]

    if iteration_stream not in self.waiting_metadata:
        self.waiting_metadata[iteration_stream] = {}
        self.waiting_data[iteration_stream] = {}
    self.waiting_metadata[iteration_stream][rank] = meta
    self.waiting_data[iteration_stream][rank] = dd

    # Denoise if collected sufficient data
    if self.waiting_metadata[iteration_stream].shape[0] == nproc_sirt:
      # Sort data and metadata by rank
      sorted_metadata = [self.waiting_metadata[iteration_stream][r] for r in sorted(self.waiting_metadata[iteration_stream].keys())]
      sorted_data = [self.waiting_data[iteration_stream][r] for r in sorted(self.waiting_data[iteration_stream].keys())]
      print(sorted_metadata)
      denoise_input = np.concatenate(sorted_data, axis=0)
      #process_stream(model, denoise_input, sorted_metadata)
      output_path = recon_path + "/" + iteration_stream+'-denoised.h5'
      with h5py.File(output_path, 'w') as h5_output:
          h5_output.create_dataset('/data', data=denoise_input)
      # Clear waiting data
      del self.waiting_metadata[iteration_stream]
      del self.waiting_data[iteration_stream]

def main():
  args = parse_arguments()

  logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

  env = StreamExecutionEnvironment.get_execution_environment()
  env.enable_checkpointing(10_000, CheckpointingMode.EXACTLY_ONCE)
  env.add_python_file("./dist/sirt_ops-0.2.0-*.whl")

  # define DAQ source
  daq = env.add_source(
    DaqOperator(
      batchsize=args.batchsize,
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
    Types.LIST(Types.MAP(Types.STRING(), Types.GENERIC(object)), Types.BYTE_ARRAY())
  )

  # define data distribution
  dist = daq.flat_map(DistOperator(args),
    output_type=Types.LIST(Types.MAP(Types.STRING(), Types.GENERIC(object), Types.BYTE_ARRAY()))
  ).name("Data Distributor")

  # define reconstruction tasks
  sirt = dist.key_by(lambda x: x[0]["task_id"], key_type=Types.INT()).map(
      SirtOperator(cfg=args),
      output_type=Types.TUPLE([
        Types.PRIMITIVE_ARRAY(Types.BYTE()),
        Types.MAP(Types.STRING(), Types.STRING())
      ])
    ).name("SIRT Operator").set_parallelism(args.ntask_sirt)

  # define denoiser as sink
  sirt.add_sink(
    DenoiserOperator(args),
    "Denoiser Sink"
  ).set_parallelism(1)

  env.execute("APS Mini-Apps Pipeline")


if __name__ == '__main__':
    main()
