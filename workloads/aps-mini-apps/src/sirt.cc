#include <iomanip>
#include "sirt.h"
#include "sirt_recon_space.h"
#include "trace_h5io.h"
#include "data_region_base.h"
#include "disp_comm_mpi.h"
#include "disp_engine_reduction.h"
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



void SirtEngine::setup(const std::unordered_map<std::string, int64_t>& tmetadata) {

  const int64_t n_blocks = tmetadata.at("n_sinograms");
  const int64_t num_cols = tmetadata.at("n_rays_per_proj_row");
  
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
  h5md.dims[1] = tmetadata.at("tn_sinograms");
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = tmetadata.at("n_rays_per_proj_row");
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

void SirtEngine::process(const std::unordered_map<std::string, int>& config,
      const std::unordered_map<std::string, std::string>& metadata,
      const std::uint8_t* data,
      std::size_t len
    ) {

  curr_slices = ms.readSlidingWindow(recon_image, metadata, data);
  
  if(config.at("center") !=0 && curr_slices!=nullptr)
    curr_slices->metadata().center(config.at("center"));
  
  if (ms.isEndOfStream()) {
    std::cout << "[Task-" << task_id << "] End of stream. Exiting..." << std::endl;
    break;
  }
  if(curr_slices == nullptr) {
    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- No new data in the sliding window. Skip processing" << std::endl;
    continue;
  }
  /// Iterate on window
  for(int i=0; i<config.at("window_iter"); ++i){

    int killed = kill_signal.load();
    if (killed != 0) {
      std::cout << "[Task-" << task_id << "] Received kill signal: " << killed << ". Exiting..." << std::endl;
      return killed;
    }

    engine->RunParallelReduction(*curr_slices, req_number);  /// Reconstruction
    
    engine->ParInPlaceLocalSynchWrapper();              /// Local combination
   
    main_recon_space->UpdateRecon(recon_image, main_recon_replica);
    
    engine->ResetReductionSpaces(init_val);
    curr_slices->ResetMirroredRegionIter();
  }

  passes++;
  /* Emit reconstructed data */
  if(!(passes%config.at("write_freq"))){
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

