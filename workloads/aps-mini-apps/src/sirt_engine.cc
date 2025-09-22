#include <iomanip>
#include "sirt.h"
#include "sirt_engine.h"
#include "sirt_recon_space.h"
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
#include "sirt_common.h"

#include "hdf5.h"

#include <boost/serialization/export.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>

// Define an alias for the instantiated template class
using DISPEngineReductionSIRT = DISPEngineReduction<SIRTReconSpace, float>;
using DISPEngineBaseSIRT = DISPEngineBase<SIRTReconSpace, float>;
using AReductionSpaceBaseSIRT = AReductionSpaceBase<SIRTReconSpace, float>;

int saveAsHDF5(const char* fname, float* recon, hsize_t* output_dims) {
  hid_t output_file_id = H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  if (output_file_id < 0) {
      return 1;
  }
  hid_t output_dataspace_id = H5Screate_simple(3, output_dims, NULL);
  hid_t output_dataset_id = H5Dcreate(output_file_id, "/data", H5T_NATIVE_FLOAT, output_dataspace_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  H5Dwrite(output_dataset_id, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT, recon);
  H5Dclose(output_dataset_id);
  H5Sclose(output_dataspace_id);
  H5Fclose(output_file_id);
  return 0;
}

void SirtEngine::setup(const std::unordered_map<std::string, int64_t>& tmetadata) {

  task_id               = require_int(tmetadata, "task_id");
  window_step           = require_int(tmetadata, "window_step");
  int thread_count      = require_int(tmetadata, "thread_count");
  ds.n_sinograms        = require_int(tmetadata, "n_sinograms");
  ds.n_rays_per_proj_row= require_int(tmetadata, "n_rays_per_proj_row");
  ds.beg_sinograms      = require_int(tmetadata, "beg_sinogram");
  ds.tn_sinograms       = require_int(tmetadata, "tn_sinograms");
  ds.window_len         = require_int(tmetadata, "window_length");

  const int64_t n_blocks = ds.n_sinograms;
  const int64_t num_cols = ds.n_rays_per_proj_row;

  h5md.ndims=3;
  h5md.dims= new hsize_t[3];
  h5md.dims[1] = ds.tn_sinograms;
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = ds.n_rays_per_proj_row;
  
  /// Reconstructed image
  recon_image = new DataRegionBareBase<float>(n_blocks*num_cols*num_cols);
  for(size_t i=0; i<recon_image->count(); ++i)
    (*recon_image)[i]=0.; /// Initial values of the reconstructe image

  /// Required data structure for dumping image to h5 file
  h5md.ndims=3;
  h5md.dims= new hsize_t[3];
  h5md.dims[1] = ds.n_sinograms;
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = ds.n_rays_per_proj_row;

  
  main_recon_space = new SIRTReconSpace(
      n_blocks, 2*num_cols*num_cols);
  main_recon_space->Initialize(num_cols*num_cols);

  main_recon_replica = &main_recon_space->reduction_objects();
  
  /* Prepare processing engine and main reduction space for other threads */
  engine = new DISPEngineReductionSIRT(main_recon_space, thread_count);
  
}

ProcessResult SirtEngine::process(
  const std::unordered_map<std::string, int64_t>& config,
  const std::unordered_map<std::string, std::string>& metadata,
  const float* data,
  std::size_t len
) {

  ProcessResult result;
  float init_val=0.;
  int64_t req_number = ds.n_rays_per_proj_row;

  DataRegionBase<float, TraceMetadata> *curr_slices = ds.readSlidingWindow(*recon_image, window_step, metadata, data, len);
  
  int center = require_int(config, "center");
  int window_iter = require_int(config, "window_iter");
  int write_freq = require_int(config, "write_freq");

  // std::cout << "[Task-" << task_id << "] passes = " << passes << " -- Processing window with " 
  //           << (curr_slices ? curr_slices->metadata().num_projs() : 0) 
  //           << " projections, center=" << center 
  //           << ", window_iter=" << window_iter 
  //           << ", write_freq=" << write_freq 
  //           << ", data len=" << len
  //           << std::endl;
  if (data && len > 0) {
    auto p = reinterpret_cast<const unsigned char*>(data);
    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- Processing window with " 
            << (curr_slices ? curr_slices->metadata().num_projs() : 0) 
            << " projections, center=" << center 
            << ", window_iter=" << window_iter 
            << ", write_freq=" << write_freq 
            << ", data len=" << len
            << "First float value: " << data[0] << " First value: " << static_cast<unsigned>(p[0]) << std::endl;
  }


  if(center !=0 && curr_slices!=nullptr)
    curr_slices->metadata().center(center);
  
  if (ds.isEndOfStream()) {
    std::cout << "[Task-" << task_id << "] End of stream. Exiting..." << std::endl;
    return result;
  }
  if(curr_slices == nullptr) {
    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- No new data in the sliding window. Skip processing" << std::endl;
    return result;
  }
  /// Iterate on window

  // DataRegion2DBareBase<float> &recon_replica = main_recon_space->reduction_objects();

  for(int i=0; i<window_iter; ++i){

    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- Iteration " << i+1 << "/" << window_iter << " on current window" << std::endl;

    engine->RunParallelReduction(*curr_slices, req_number);  /// Reconstruction
    // std::cout << "[Task-" << task_id << "] ---- Complete parallel reduction ---- " << std::endl;
    
    engine->ParInPlaceLocalSynchWrapper();              /// Local combination
    // std::cout << "[Task-" << task_id << "] ---- Complete par in-place local synch ---- " << std::endl;
   
    main_recon_space->UpdateRecon(*recon_image, *main_recon_replica);
    // main_recon_space->UpdateRecon(*recon_image, recon_replica);
    // std::cout << "[Task-" << task_id << "] ---- Complete updating reconstruction ---- " << std::endl;
    
    engine->ResetReductionSpaces(init_val);
    // std::cout << "[Task-" << task_id << "] ---- Complete resetting reduction spaces ---- " << std::endl;

    curr_slices->ResetMirroredRegionIter();
    // std::cout << "[Task-" << task_id << "] ---- Complete resetting mirrored region iter ---- " << std::endl;
  }
  /* Emit reconstructed data */
  if(!(passes%write_freq)){

    std::cout << "[Task-" << task_id << "] passes = " << passes << " -- Emitting reconstructed image" << std::endl;

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

    std::cout << "[Task-" << task_id << "] ---- Reconstructed image data index: " << recon_slice_data_index << std::endl;

    // result.data = &recon[recon_slice_data_index];
    result.data.insert(result.data.end(), 
        &recon[recon_slice_data_index], 
        &recon[recon_slice_data_index] +  data_size);
    result.meta = md;
    // MPI_Barrier(MPI_COMM_WORLD);
    
    std::string outputpath = iteration_stream.str() + "-recon.h5";
    saveAsHDF5(outputpath.c_str(), 
        &recon[recon_slice_data_index], app_dims);
  }

  passes++;

  //delete curr_slices->metadata(); //TODO Check for memory leak
  delete curr_slices;

  return result;

}

std::vector<std::uint8_t> SirtEngine::snapshot() const {
  std::vector<std::uint8_t> saved_ckpt;
  // TODO: replace these with actual boost serialization
  return saved_ckpt;
}

void SirtEngine::restore(const std::vector<std::uint8_t>& snapshot) {
  // TODO: replace these with actual boost deserialization
}


SirtEngine::~SirtEngine() {
  // if (main_recon_space != nullptr) {
  //   delete main_recon_space;
  // }
  // if (engine != nullptr) {
  //   delete engine;
  // }
  // // if (h5md.dims != nullptr) {
  // //   delete[] h5md.dims;
  // // }
  // if (recon_image != nullptr) {
  //   delete recon_image;
  // }
}

