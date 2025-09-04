#include <iomanip>
#include "sirt.h"
#include "sirt_recon_space.h"
#include "trace_h5io.h"
#include "data_region_base.h"
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

  ds.n_sinograms = tmetadata.at("n_sinograms");
  ds.n_rays_per_proj_row = tmetadata.at("n_rays_per_proj_row");
  ds.beg_sinograms = tmetadata.at("beg_sinograms");
  ds.tn_sinograms = tmetadata.at("tn_sinograms");

  const int64_t n_blocks = not_sinograms;
  const int64_t num_cols = nrays_per_proj_row;

  h5md.ndims=3;
  h5md.dims= new hsize_t[3];
  h5md.dims[1] = tn_sinograms;
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = n_rays_per_proj_row;
  
  /// Reconstructed image
  recon_image = new DataRegionBareBase<float>(n_blocks*num_cols*num_cols);
  for(size_t i=0; i<recon_image->count(); ++i)
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

  
  main_recon_space = new SIRTReconSpace(
      n_blocks, 2*num_cols*num_cols);
  main_recon_space->Initialize(num_cols*num_cols);

  DataRegion2DBareBase<float> &main_recon_replica = main_recon_space->reduction_objects();
  float init_val=0.;

  /* Prepare processing engine and main reduction space for other threads */
  *engine = new DISPEngineReductionSIRT(main_recon_space, config.thread_count);
  
}

ProcessResult SirtEngine::process(const std::unordered_map<std::string, int>& config,
      const std::unordered_map<std::string, std::string>& metadata,
      const std::uint8_t* data,
      std::size_t len
    ) {

  ProcessResult result;
  int task_id = config.at("rank");
  float init_val=0.;
  int64_t req_number = ds.n_rays_per_proj_row;

  DataRegionBase<float, TraceMetadata> *curr_slices = ds.readSlidingWindow(*recon_image, metadata, data);
  

  if(config.at("center") !=0 && curr_slices!=nullptr)
    curr_slices->metadata().center(config.at("center"));
  
  if (ds.isEndOfStream()) {
    std::cout << "[Task-" << task_id << "] End of stream. Exiting..." << std::endl;
    return result;
  }
  if(curr_slices == nullptr) {
    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- No new data in the sliding window. Skip processing" << std::endl;
    return result;
  }
  /// Iterate on window
  for(int i=0; i<config.at("window_iter"); ++i){

    engine->RunParallelReduction(*curr_slices, req_number);  /// Reconstruction
    
    engine->ParInPlaceLocalSynchWrapper();              /// Local combination
   
    main_recon_space->UpdateRecon(*recon_image, main_recon_replica);
    
    engine->ResetReductionSpaces(init_val);
    curr_slices->ResetMirroredRegionIter();
  }

  passes++;
  /* Emit reconstructed data */
  if(!(passes%config.at("write_freq"))){
    std::stringstream iteration_stream;
    iteration_stream << std::setfill('0') << std::setw(6) << passes;
    
    TraceMetadata &rank_metadata = curr_slices->metadata();

    int recon_slice_data_index = rank_metadata.num_neighbor_recon_slices()* rank_metadata.num_grids() * rank_metadata.num_grids();
    ADataRegion<float> &recon = rank_metadata.recon();

    // hsize_t ndims = static_cast<hsize_t>(h5md.ndims);

    hsize_t rank_dims[3] = {
      static_cast<hsize_t>(rank_metadata.num_slices()),
      static_cast<hsize_t>(rank_metadata.num_cols()),
      static_cast<hsize_t>(rank_metadata.num_cols())};

    size_t data_size = rank_dims[0]*rank_dims[1]*rank_dims[2];
    hsize_t app_dims[3] = {
      static_cast<hsize_t>(h5md.dims[1]),
      static_cast<hsize_t>(h5md.dims[2]),
      static_cast<hsize_t>(h5md.dims[2])};

      std::unordered_map<std::string, std::string> md = {
        {"Type", "DATA"},
        {"rank", std::to_string(task_id)},
        {"iteration_stream", iteration_stream.str()},
        {"rank_dims_0", std::to_string(rank_dims[0])},
        {"rank_dims_1", std::to_string(rank_dims[1])},
        {"rank_dims_2", std::to_string(rank_dims[2])},
        {"app_dims_0", std::to_string(app_dims[0])},
        {"app_dims_1", std::to_string(app_dims[1])},
        {"app_dims_2", std::to_string(app_dims[2])},
        {"recon_slice_data_index", std::to_string(recon_slice_data_index)}
    };

    // result.data = &recon[recon_slice_data_index];
    result.data.insert(result.data.end(), 
        reinterpret_cast<std::uint8_t*>(&recon[recon_slice_data_index]), 
        reinterpret_cast<std::uint8_t*>(&recon[recon_slice_data_index]) + 
        data_size * sizeof(float));
    result.meta = md;
    // MPI_Barrier(MPI_COMM_WORLD);
  }
  //delete curr_slices->metadata(); //TODO Check for memory leak
  delete curr_slices;

  return result;

}

SirtEngine::~SirtEngine() {
  if (main_recon_space != nullptr) {
    delete main_recon_space;
  }
  if (engine != nullptr) {
    delete engine;
  }
  if (h5md.dims != nullptr) {
    delete[] h5md.dims;
  }
  if (recon_image != nullptr) {
    delete recon_image;
  }
}

