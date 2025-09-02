#include <iomanip>
#include "sirt/recon_engine.h"
#include "trace_h5io.h"
#include "data_region_base.h"
#include "tclap/CmdLine.h"
#include "disp_comm_mpi.h"
#include "disp_engine_reduction.h"
#include "sirt.h" // Include SIRTReconSpace
#include <cassert>
#include <time.h>
#include <string>
#include <fstream>
#include <iostream>
#include "trace_data.h"
#include <vector>
#include <unistd.h>
#include <charconv>
#include <csignal>



void SirtOperator::update(pybind11::dict tmetadata) {

  tmetadata = json::parse(pybind11::str(metadata));
  ::init(this->tmetadata);

  auto n_blocks = tmetadata["n_sinograms"].get<int64_t>();
  auto num_cols = tmetadata["n_rays_per_proj_row"].get<int64_t>();
  
  /// Reconstructed image
  DataRegionBareBase<float> recon_image(n_blocks*num_cols*num_cols);
  for(size_t i=0; i<recon_image.count(); ++i)
    recon_image[i]=0.; /// Initial values of the reconstructe image

  /// Number of requested ray-sum values by each thread poll
  int64_t req_number = num_cols;
  /// Required data structure for dumping image to h5 file
  trace_io::H5Metadata h5md;
  h5md.ndims=3;
  h5md.dims= new hsize_t[3];
  h5md.dims[1] = tmetadata["tn_sinograms"].get<int64_t>();
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = tmetadata["n_rays_per_proj_row"].get<int64_t>();
  size_t data_size = 0;

  
  auto main_recon_space = new SIRTReconSpace(
      n_blocks, 2*num_cols*num_cols);
  main_recon_space->Initialize(num_cols*num_cols);

  DataRegion2DBareBase<float> &main_recon_replica = main_recon_space->reduction_objects();
  float init_val=0.;

  /* Prepare processing engine and main reduction space for other threads */
  DISPEngineBase<SIRTReconSpace, float> *engine =
    new DISPEngineReductionSIRT(main_recon_space, config.thread_count);
  
}

void SirtOperator::process(pybind11::dict metadata, pybind11::array_t<int> rawdata) {

  curr_slices = ms.readSlidingWindow(recon_image, config.window_step, consumer);
  
  if(config.center!=0 && curr_slices!=nullptr)
    curr_slices->metadata().center(config.center);
  #ifdef TIMERON
  datagen_tot += (std::chrono::system_clock::now()-datagen_beg);
  #endif
  
  if (ms.isEndOfStream()) {
    std::cout << "[Task-" << task_id << "] End of stream. Exiting..." << std::endl;
    break;
  }
  if(curr_slices == nullptr) {
    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- No new data in the sliding window. Skip processing" << std::endl;
    continue;
  }
  /// Iterate on window
  for(int i=0; i<config.window_iter; ++i){

    int killed = kill_signal.load();
    if (killed != 0) {
      std::cout << "[Task-" << task_id << "] Received kill signal: " << killed << ". Exiting..." << std::endl;
      return killed;
    }

    #ifdef TIMERON
    auto recon_beg = std::chrono::system_clock::now();
    #endif
    engine->RunParallelReduction(*curr_slices, req_number);  /// Reconstruction

    #ifdef TIMERON
    recon_tot += (std::chrono::system_clock::now()-recon_beg);
    auto inplace_beg = std::chrono::system_clock::now();
    #endif
    engine->ParInPlaceLocalSynchWrapper();              /// Local combination
    #ifdef TIMERON
    inplace_tot += (std::chrono::system_clock::now()-inplace_beg);

    /// Update reconstruction object
    auto update_beg = std::chrono::system_clock::now();
    #endif
    main_recon_space->UpdateRecon(recon_image, main_recon_replica);
    #ifdef TIMERON
    update_tot += (std::chrono::system_clock::now()-update_beg);
    #endif
    engine->ResetReductionSpaces(init_val);
    curr_slices->ResetMirroredRegionIter();
  }

  passes++;
  /* Emit reconstructed data */
  if(!(passes%config.write_freq)){
    std::stringstream iteration_stream;
    iteration_stream << std::setfill('0') << std::setw(6) << passes;
    
    try {
      TraceMetadata &rank_metadata = curr_slices->metadata();

      int recon_slice_data_index = rank_metadata.num_neighbor_recon_slices()* rank_metadata.num_grids() * rank_metadata.num_grids();
      ADataRegion<float> &recon = rank_metadata.recon();

      hsize_t ndims = static_cast<hsize_t>(h5md.ndims);

      hsize_t rank_dims[3] = {
        static_cast<hsize_t>(rank_metadata.num_slices()),
        static_cast<hsize_t>(rank_metadata.num_cols()),
        static_cast<hsize_t>(rank_metadata.num_cols())};

      data_size = rank_dims[0]*rank_dims[1]*rank_dims[2];
      hsize_t app_dims[3] = {
        static_cast<hsize_t>(h5md.dims[1]),
        static_cast<hsize_t>(h5md.dims[2]),
        static_cast<hsize_t>(h5md.dims[2])};

      json md = json{
          {"Type", "DATA"},
          {"rank", task_id},
          {"iteration_stream", iteration_stream.str()},
          {"rank_dims", rank_dims},
          {"app_dims", app_dims},
          {"recon_slice_data_index", recon_slice_data_index}};

      ms.publishImage(md, &recon[recon_slice_data_index], data_size, producer);

    } catch(const mofka::Exception& ex) {
      // spdlog::critical("{}", ex.what());
      std::cerr << "[Task-" << task_id << "] Error during publishing image: " << ex.what() << std::endl;
      exit(-1);
    }
  // MPI_Barrier(MPI_COMM_WORLD);
  }
  //delete curr_slices->metadata(); //TODO Check for memory leak
  delete curr_slices;

}

