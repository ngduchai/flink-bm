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
  sirt_metadata.task_id            = require_int(tmetadata, "task_id");
  sirt_metadata.window_step        = require_int(tmetadata, "window_step");
  sirt_metadata.thread_count       = require_int(tmetadata, "thread_count");
  sirt_metadata.n_sinograms        = require_int(tmetadata, "n_sinograms");
  sirt_metadata.n_rays_per_proj_row= require_int(tmetadata, "n_rays_per_proj_row");
  sirt_metadata.beg_sinograms      = require_int(tmetadata, "beg_sinogram");
  sirt_metadata.tn_sinograms       = require_int(tmetadata, "tn_sinograms");
  sirt_metadata.window_len         = require_int(tmetadata, "window_length");
}

void SirtProcessor::setup(int row_id, const SirtMetadata& tmetadata) {

  this->row_id          = row_id;
  ds.row_id             = row_id;
  task_id               = tmetadata.task_id;
  window_step           = tmetadata.window_step;
  int thread_count      = tmetadata.thread_count;
  ds.n_sinograms        = tmetadata.n_sinograms;
  ds.n_rays_per_proj_row= tmetadata.n_rays_per_proj_row;
  ds.beg_sinograms      = tmetadata.beg_sinograms;
  ds.tn_sinograms       = tmetadata.tn_sinograms;
  ds.window_len         = tmetadata.window_len;
  ds.setRank(task_id);

  const int64_t n_blocks = ds.n_sinograms;
  const int64_t num_cols = ds.n_rays_per_proj_row;

  h5md.ndims=3;
  h5md.dims= new hsize_t[3];
  h5md.dims[1] = ds.tn_sinograms;
  // h5md.dims[1] = 1;
  h5md.dims[0] = 0;   /// Number of projections is unknown
  h5md.dims[2] = ds.n_rays_per_proj_row;
  
  /// Reconstructed image
  this->recon_image = new DataRegionBareBase<float>(n_blocks*num_cols*num_cols);
  for(size_t i=0; i<this->recon_image->count(); ++i)
    (*(this->recon_image))[i]=0.; /// Initial values of the reconstructe image

  // /// Required data structure for dumping image to h5 file
  // h5md.ndims=3;
  // h5md.dims= new hsize_t[3];
  // h5md.dims[1] = ds.n_sinograms;
  // // h5md.dims[1] = 1;
  // h5md.dims[0] = 0;   /// Number of projections is unknown
  // h5md.dims[2] = ds.n_rays_per_proj_row;

  
  main_recon_space = new SIRTReconSpace(
      n_blocks, 2*num_cols*num_cols);
  main_recon_space->Initialize(num_cols*num_cols);

  main_recon_space->row_id = row_id;
  main_recon_space->task_id = task_id;

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
  int row_id = std::stoi(require_str(metadata, "row_id"));
  auto type = require_str(metadata, "Type");
  if (type == "WARMUP") {
    auto [ins_it, _] = sirt_processors.emplace(row_id, SirtProcessor{});
    auto it = ins_it;
    it->second.setup(row_id, this->sirt_metadata);
    ProcessResult result;
    return result;
  }else{
    return sirt_processors[row_id].process(config, metadata, data, len);
  }

  // auto it = sirt_processors.find(row_id);
  // if (it == sirt_processors.end()) {
  //   auto [ins_it, _] = sirt_processors.emplace(row_id, SirtProcessor{});
  //   it = ins_it;
  //   it->second.setup(row_id, this->sirt_metadata);
  // }
  // return it->second.process(config, metadata, data, len);
}

ProcessResult SirtProcessor::process(
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
    std::cout << "[Row-" << row_id << "/" << task_id << "] passes = " << passes << " -- Processing window with " 
            << (curr_slices ? curr_slices->metadata().num_projs() : 0) 
            << " projections, center=" << center 
            << ", window_iter=" << window_iter 
            << ", write_freq=" << write_freq 
            << ", data len=" << len
            << ", First float value: " << data[0] << " First value: " << static_cast<unsigned>(p[0]) << std::endl;
  }


  if(center !=0 && curr_slices!=nullptr)
    curr_slices->metadata().center(center);
  
  if (ds.isEndOfStream()) {
    std::cout << "[Row-" << row_id << "/" << task_id << "] End of stream. Exiting..." << std::endl;
    return result;
  }
  if(curr_slices == nullptr) {
    std::cout << "[Row-" << row_id << "/" << task_id << "] passes = " << passes << " -- No new data in the sliding window. Skip processing" << std::endl;
    return result;
  }
  /// Iterate on window

  // DataRegion2DBareBase<float> &recon_replica = main_recon_space->reduction_objects();

  ADataRegion<float> &recon_data = curr_slices->metadata().recon();

  if (passes == 0) {
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Initializing recon_data ----" << std::endl;
    for(size_t i=0; i<recon_data.count(); ++i)
      recon_data[i]=0.; /// Initial values of the reconstructe image
  }

  for(int i=0; i<window_iter; ++i){

    std::cout << "[Row-" << row_id << "/" << task_id << "] passes = " << passes << " -- Iteration " << i+1 << "/" << window_iter << " on current window" << std::endl;

    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Before reconstruction ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;

    engine->RunParallelReduction(*curr_slices, req_number);  /// Reconstruction
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Complete parallel reduction ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;
    
    engine->ParInPlaceLocalSynchWrapper();              /// Local combination
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Complete par in-place local synch ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;
   
    main_recon_space->UpdateRecon(*recon_image, *main_recon_replica);
    // main_recon_space->UpdateRecon(*recon_image, recon_replica);
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Complete updating reconstruction ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;
    
    engine->ResetReductionSpaces(init_val);
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Complete resetting reduction spaces ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;

    curr_slices->ResetMirroredRegionIter();
    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Complete resetting mirrored region iter ---- Checksum: " << fnv1a32(recon_data.get_data(), recon_data.count()) <<  std::endl;
  }
  /* Emit reconstructed data */
  if(!(passes%write_freq)){

    std::cout << "[Row-" << row_id << "/" << task_id << "] passes = " << passes << " -- Emitting reconstructed image" << std::endl;

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
      {"row_id", std::to_string(row_id)},
      {"iteration_stream", iteration_stream.str()},
      {"rank_dims_0", std::to_string(rank_dims[0])},
      {"rank_dims_1", std::to_string(rank_dims[1])},
      {"rank_dims_2", std::to_string(rank_dims[2])},
      {"app_dims_0", std::to_string(app_dims[0])},
      {"app_dims_1", std::to_string(app_dims[1])},
      {"app_dims_2", std::to_string(app_dims[2])},
      {"recon_slice_data_index", std::to_string(recon_slice_data_index)}
    };

    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- Reconstructed image data index: " << std::endl;

    // result.data = &recon[recon_slice_data_index];
    result.data.insert(result.data.end(), 
        &recon[recon_slice_data_index], 
        &recon[recon_slice_data_index] +  data_size);
    result.meta = md;
    // MPI_Barrier(MPI_COMM_WORLD);

    // std::cout << "[Row-" << row_id << "/" << task_id << "] ---- DataEmit: iteration_stream: " << iteration_stream.str() << " checksum: " << fnv1a32(result.data) << " Reconstruction checksum: " << fnv1a32(recon.get_data(), recon.count()) << std::endl;
    
    // std::string outputpath = iteration_stream.str() + "-" + std::to_string(row_id) + "-recon.h5";
    // saveAsHDF5(outputpath.c_str(), 
    //     &recon[recon_slice_data_index], app_dims);
  }

  passes++;

  //delete curr_slices->metadata(); //TODO Check for memory leak
  delete curr_slices;

  return result;

}

std::vector<std::uint8_t> SirtEngine::snapshot() const {
  SirtCkpt ckpt;
  for (const auto& processor : this->sirt_processors) {
    ckpt.add_processor(processor.first, processor.second.passes, processor.second.recon_image);
  }

  std::vector<std::uint8_t> saved_ckpt = ckpt.to_bytes();

  std::cout << "testing if the serialization works..." << std::endl;
  SirtCkpt test_ckpt(saved_ckpt);
  for (size_t i = 0; i < ckpt.row_ids.size(); ++i) {
    assert(test_ckpt.row_ids[i] == ckpt.row_ids[i]);
    assert(test_ckpt.progresses[i] == ckpt.progresses[i]);
    assert(test_ckpt.recon_images[i]->count() == ckpt.recon_images[i]->count());
    for (size_t j = 0; j < ckpt.recon_images[i]->count(); ++j) {
      assert((*test_ckpt.recon_images[i])[j] == (*ckpt.recon_images[i])[j]);
    }
  }
  std::cout << "serialization test passed!" << std::endl;

  return saved_ckpt;
}

void SirtEngine::restore(const std::vector<std::uint8_t>& snapshot) {
  // TODO: replace these with actual boost deserialization
  SirtCkpt ckpt(snapshot);
  for (int i = 0; i < ckpt.progresses.size(); ++i) {
    int row_id = ckpt.row_ids[i];
    int passes = ckpt.progresses[i];
    DataRegionBareBase<float>* recon_image = ckpt.recon_images[i];

    auto it = sirt_processors.find(row_id); 
    if (it == sirt_processors.end()) {
      SirtProcessor processor(row_id, this->sirt_metadata);
      processor.setup(row_id, this->sirt_metadata);
      processor.passes = passes;
      processor.recon_image = recon_image;
      sirt_processors.insert({row_id, processor});
    }else{
      it->second.passes = passes;
      it->second.recon_image = recon_image;
    }
    std::cout << "Restored SirtProcessor for row_id " << row_id << " with passes " << passes << std::endl;
  }
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

SirtProcessor::~SirtProcessor() {}


