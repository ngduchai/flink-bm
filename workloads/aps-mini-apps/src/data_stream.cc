#include "data_stream.h"
#include "sirt_common.h"

// void DataStream::addTomoMsg(DataStreamEvent event){
//   auto metadata = event.metadata;
//   auto data = event.data;
  
//   pending_events.push_back(event);
//   vmeta.push_back(metadata); /// Setup metadata
//   vtheta.push_back(required_str(metadata, "theta"));

//   size_t n_rays_per_proj = n_sinograms * n_rays_per_proj_row;
//   vproj.insert(vproj.end(), event.data, event.data + n_rays_per_proj);
// }

void DataStream::addTomoMsg(DataStreamEvent& event){
  // pending_events.push_back(event);
  // vmeta.push_back(event.metadata); /// Setup metadata
  vcenters.push_back(event.center);
  vtheta.push_back(event.theta);
  // spdlog::info("Received data {}", metadata.string());

  size_t n_rays_per_proj = n_sinograms * n_rays_per_proj_row;
  assert(n_rays_per_proj == event.data_size && "Data size does not match n_rays_per_projection");

  // auto p = reinterpret_cast<const unsigned char*>(&(event.data[0]));
  // std::cout << "SirtDataCheck: "
  //   << "First float value: " << event.data[0]
  //   << " First value: " << static_cast<unsigned>(p[0]) << std::endl;

  // vproj.insert(vproj.end(), event.data, event.data + n_rays_per_proj);
  vproj.insert(vproj.end(), event.data.begin(), event.data.end());
}

/* Erase streaming message to buffers
*/
void DataStream::eraseBegTraceMsg(){
  progress++; // Update progress = # processed messages
  std::cout << "[Row-" << getRow() << "/" << getRank() << "]: Advancing sliding window: Progress: " << progress << std::endl;
  vtheta.erase(vtheta.begin());
  size_t n_rays_per_proj = n_sinograms * n_rays_per_proj_row;
  vproj.erase(vproj.begin(),vproj.begin()+n_rays_per_proj);
  // vmeta.erase(vmeta.begin());
  vcenters.erase(vcenters.begin());
}


/* Generates a data region that can be processed by Trace
* @param recon_image: reconstruction image

  return: DataRegionBase
*/
DataRegionBase<float, TraceMetadata>* DataStream::setupTraceDataRegion(
  DataRegionBareBase<float> &recon_image){

  // std::cout << "[Row-" << getRow() << "/" << getRank() << "]: Setting up data region from sliding window with " << vtheta.size() << " projections" << std::endl;
  
  // int center = std::stoi(require_str(vmeta.back(), "center"));
  int center = vcenters.back();

  TraceMetadata *mdata = new TraceMetadata(
    vtheta.data(),
    0,                                // metadata().proj_id(),
    beg_sinograms,                    // metadata().slice_id(),
    0,                                // metadata().col_id(),
    // tn_sinograms,                     // metadata().num_total_slices(),
    n_sinograms,
    vtheta.size(),                    // int const num_projs,
    n_sinograms,                      // metadata().num_slices(),
    n_rays_per_proj_row,              // metadata().num_cols(),
    n_rays_per_proj_row,              // * metadata().n_rays_per_proj_row, // metadata().num_grids(),
    center);                          // use the last incoming center for recon.);

  mdata->recon(recon_image);

  // Will be deleted at the end of main loop
  float *data=new float[mdata->count()];
  for(size_t i=0; i<mdata->count(); ++i) data[i]=vproj[i];
  auto curr_data = new DataRegionBase<float, TraceMetadata> (
      data,
      mdata->count(),
      mdata);
  curr_data->ResetMirroredRegionIter();
  
  // auto p = reinterpret_cast<const unsigned char*>(data);
  // std::cout << "SirtReconCheck: "
  //   << "First float value: " << data[0]
  //   << " First value: " << static_cast<unsigned>(p[0]) << std::endl;
  
  std::string checksum = std::to_string(fnv1a32(data, mdata->count()));
  std::string info = "[Row-" + std::to_string(getRow()) + "/" + std::to_string(getRank()) + "]: Preparing with " + std::to_string(vtheta.size()) + " projections, center=" + std::to_string(center) + ", checksum: " + checksum;
  info += ", theta=" + std::to_string(vtheta.front());
  for (size_t i = 1; i < vtheta.size(); ++i) {
    info += "," + std::to_string(vtheta[i]);
  }
  std::cout << info << std::endl;

  return curr_data;
}

DataStream::DataStream(uint32_t window_len, int rank, int progress) // Removed default argument
  : comm_rank {rank}, progress {progress}, window_len {window_len} {}


/* Create a data region from sliding window
  * @param recon_image Initial values of reconstructed image
  * @param step        Sliding step. Waits at least step projection
  *                    before returning window back to the reconstruction
  *                    engine
  *
  * Return:  nullptr if there is no message and sliding window is empty
  *          DataRegionBase if there is data in sliding window
  */

DataRegionBase<float, TraceMetadata>* DataStream::readSlidingWindow(
  DataRegionBareBase<float> &recon_image, int step,
  const std::unordered_map<std::string, std::string>& metadata,
  const float *data, std::size_t data_size) {
  // Dynamically meet sizes
  while(vtheta.size() > window_len) {
    eraseBegTraceMsg();
  }

  // Load metadata
  std::string type = require_str(metadata, "Type"); 
  if (type == "FIN") {
    setEndOfStream(true);
    std::cout << "[Row-" << getRow() << "/" << getRank() << "]: End of stream detected" << std::endl;
    return nullptr;
  }
  
  int sequence_id = std::stoi(require_str(metadata, "seq_n"));
  int proj_id = std::stoi(require_str(metadata, "projection_id"));
  double theta = std::stod(require_str(metadata, "theta"));
  double center = std::stod(require_str(metadata, "center"));
  auto checksum = require_str(metadata, "checksum");
  std::cout << "[Row-" << getRow() << "/" << getRank() << "]: seq_id: " << sequence_id << " projection_id: " << proj_id << " theta: " << theta << " checksum: " << checksum <<  " center: " << center << ", progress = " << progress << std::endl;
  pending_events.push_back(DataStreamEvent(metadata, sequence_id, proj_id, theta, center, data, data_size));

  // std::cout << "[Row-" << getRow() << "/" << getRank() << "]: Queue len: pending_events: " << pending_events.size() << " vtheta: " << vtheta.size() << std::endl;

  if (pending_events.size() < (size_t)step) {
    return nullptr; // Not collecting enough messages to process
  }

  /// End of the processing
  if(pending_events.size()==0 && vtheta.size()==0){
    return nullptr;
  }
  /// End of messages, but there is data to be processed in window
  else if(pending_events.size()==0 && vtheta.size()>0){
    for(int i=0; i<step; ++i){  // Delete step size element
      if(vtheta.size()>0) eraseBegTraceMsg();
      else break;
    }
    // std::cout << "End of messages, but there might be data in window:" << vtheta.size() << std::endl;
    if(vtheta.size()==0) return nullptr;
  }
  /// New message(s) arrived, there is space in window
  else if(pending_events.size()>0 && vtheta.size()<window_len){
    // std::cout << "New message(s) arrived, there is space in window: " << window_len - vtheta.size() << std::endl;
    for(auto msg : pending_events){
      addTomoMsg(msg);
      ++counter;
    }
    std::cout << "After adding # items in window: " << vtheta.size() << std::endl;
  }
  /// New message arrived, there is no space in window
  else if(pending_events.size()>0 && vtheta.size()>=window_len){
    // std::cout << "New message arrived, there is no space in window: " << vtheta.size() << std::endl;
    for(int i=0; i<step; ++i) {
      if(vtheta.size()>0) eraseBegTraceMsg();
      else break;
    }
    for(auto msg : pending_events){
      addTomoMsg(msg);
      ++counter;
    }
  }
  else std::cerr << "Unknown state in ReadWindow!" << std::endl;

  /// Clean-up vector
  pending_events.clear();

  /// Generate new data and metadata
  DataRegionBase<float, TraceMetadata>* data_region =
    setupTraceDataRegion(recon_image);

  return data_region;
}

int DataStream::getRank() {return comm_rank;}
void DataStream::setRank(int rank) { this->comm_rank = rank; }

uint32_t DataStream::getCounter(){ return counter;}

void DataStream::windowLength(uint32_t wlen){ window_len = wlen;}


