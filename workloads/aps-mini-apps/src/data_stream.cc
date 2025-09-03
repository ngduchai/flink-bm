#include "data_stream.h"

void MofkaStream::addTomoMsg(mofka::Event event){
  mofka::Metadata metadata = event.metadata();
  
  mofka::Data data = event.data();
  
  pending_events.push_back(event);
  vmeta.push_back(metadata.json()); /// Setup metadata
  vtheta.push_back(metadata.json()["theta"].get<float_t>());

  size_t n_rays_per_proj =
    getInfo()["n_sinograms"].get<int64_t>() *
    getInfo()["n_rays_per_proj_row"].get<int64_t>();
  size_t ptr_size = data.segments()[0].size / sizeof(float);
  assert(n_rays_per_proj == ptr_size && "Pointer size does not match n_rays_per_projection");

  float* start = static_cast<float*>(data.segments()[0].ptr);
  float* end = static_cast<float*>(data.segments()[0].ptr)+ n_rays_per_proj;
  if (start == nullptr || end == nullptr) {
    throw std::runtime_error("Invalid pointer arithmetic in insertion");
  }
  vproj.insert(vproj.end(), start, end);
}

/* Erase streaming message to buffers
*/
void MofkaStream::eraseBegTraceMsg(){
  progress++; // Update progress = # processed messages
  std::cout << "[Task-" << getRank() << "]: Advancing sliding window: Progress: " << progress << std::endl;
  vtheta.erase(vtheta.begin());
  size_t n_rays_per_proj =
    getInfo()["n_sinograms"].get<int64_t>() *
    getInfo()["n_rays_per_proj_row"].get<int64_t>();
  vproj.erase(vproj.begin(),vproj.begin()+n_rays_per_proj);
  vmeta.erase(vmeta.begin());
}


/* Generates a data region that can be processed by Trace
* @param recon_image: reconstruction image

  return: DataRegionBase
*/
DataRegionBase<float, TraceMetadata>* MofkaStream::setupTraceDataRegion(
  DataRegionBareBase<float> &recon_image){
    TraceMetadata *mdata = new TraceMetadata(
    vtheta.data(),
    0,                                                  // metadata().proj_id(),
    getInfo()["beg_sinogram"].get<int64_t>(),           // metadata().slice_id(),
    0,                                                  // metadata().col_id(),
    getInfo()["tn_sinograms"].get<int64_t>(),           // metadata().num_total_slices(),
    vtheta.size(),                                      // int const num_projs,
    getInfo()["n_sinograms"].get<int64_t>(),            // metadata().num_slices(),
    getInfo()["n_rays_per_proj_row"].get<int64_t>(),    // metadata().num_cols(),
    getInfo()["n_rays_per_proj_row"].get<int64_t>(),    // * metadata().n_rays_per_proj_row, // metadata().num_grids(),
    vmeta.back()["center"].get<float>());               // use the last incoming center for recon.);

  mdata->recon(recon_image);

  // Will be deleted at the end of main loop
  float *data=new float[mdata->count()];
  for(size_t i=0; i<mdata->count(); ++i) data[i]=vproj[i];
  auto curr_data = new DataRegionBase<float, TraceMetadata> (
      data,
      mdata->count(),
      mdata);

  curr_data->ResetMirroredRegionIter();
  return curr_data;
}

DataStream::DataStream(uint32_t window_len, int rank, int progress) // Removed default argument
  : window_len {window_len}, comm_rank {rank}, progress {progress} {}


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
  DataRegionBareBase<float> &recon_image,
  json metadata, float *data) {
  // Dynamically meet sizes
  while(vtheta.size()> window_len)
    eraseBegTraceMsg();

  // Receive new message
  std::vector<mofka::Event> mofka_events;

  for(int i=0; i<step; ++i) {
    // mofka messages

    auto start = std::chrono::high_resolution_clock::now();
    mofka::Future<mofka::Event> future_event = consumer.pull();
    while (!future_event.completed()) {
      // sleep for 1 ms to avoid busy waiting
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      if (interrupt_signal) {
        std::cout << "[Task-" << getRank() << "]: Interrupt signal received, stopping pull." << std::endl;
        return nullptr; // Exit if interrupt signal is received
      }
    }
    auto event = future_event.wait();
    // auto event = consumer.pull().wait();
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    setConsumerTimes("wait_t", 1, elapsed.count());

    //if endMsg break
    if (event.metadata().json()["Type"].get<std::string>() == "FIN") {
      setEndOfStream(true);
      std::cout << "[Task-" << getRank() << "]: End of stream detected" << std::endl;
      return nullptr;
    }
    
    int sequence_id = event.metadata().json()["seq_n"].get<int>();
    int proj_id = event.metadata().json()["projection_id"].get<int>();
    double theta = event.metadata().json()["theta"].get<float>();
    double center = event.metadata().json()["center"].get<float>();
    std::cout << "[Task-" << getRank() << "]: seq_id: " << sequence_id << " projection_id: " << proj_id << " theta: " << theta << " center: " << center << ", progress = " << progress << std::endl;

    // // Only add the event if its sequence_id higher than the progress
    // int sequence_id = event.metadata().json()["seq_n"].get<int>();
    // std::cout << "[Task-" << getRank() << "]: seq_id: " << sequence_id << ", progress = " << progress << std::endl;
    // if (sequence_id < progress) {
    //   std::cout << "[Task-" << getRank() << "]: Skipping seq_id: " << sequence_id << " < " << progress << " = progress" << std::endl;
    //   continue; // Skip this event
    // }
    mofka_events.push_back(event);
  }
  // TODO: After receiving message corrections might need to be applied

  /// End of the processing
  if(mofka_events.size()==0 && vtheta.size()==0){
    //std::cout << "End of the processing: " << vtheta.size() << std::endl;
    return nullptr;
  }
  /// End of messages, but there is data to be processed in window
  else if(mofka_events.size()==0 && vtheta.size()>0){
    for(int i=0; i<step; ++i){  // Delete step size element
      if(vtheta.size()>0) eraseBegTraceMsg();
      else break;
    }
    //std::cout << "End of messages, but there might be data in window:" << vtheta.size() << std::endl;
    if(vtheta.size()==0) return nullptr;
  }
  /// New message(s) arrived, there is space in window
  else if(mofka_events.size()>0 && vtheta.size()<window_len){
    //std::cout << "New message(s) arrived, there is space in window: " << window_len_ - vtheta.size() << std::endl;
    for(auto msg : mofka_events){
      addTomoMsg(msg);
      ++counter;
    }
    std::cout << "After adding # items in window: " << vtheta.size() << std::endl;
  }
  /// New message arrived, there is no space in window
  else if(mofka_events.size()>0 && vtheta.size()>=window_len){
    //std::cout << "New message arrived, there is no space in window: " << vtheta.size() << std::endl;
    for(int i=0; i<step; ++i) {
      if(vtheta.size()>0) eraseBegTraceMsg();
      else break;
    }
    for(auto msg : mofka_events){
      addTomoMsg(msg);
      ++counter;
    }
  }
  else std::cerr << "Unknown state in ReadWindow!" << std::endl;

  /// Clean-up vector
  mofka_events.clear();

  /// Generate new data and metadata
  DataRegionBase<float, TraceMetadata>* data_region =
    setupTraceDataRegion(recon_image);

  return data_region;
}

json MofkaStream::getInfo(){ return info;}

int MofkaStream::getRank() {return comm_rank;}

int MofkaStream::getBufferSize() {return buffer.size();}

uint32_t MofkaStream::getBatch() {return batch;}

uint32_t MofkaStream::getCounter(){ return counter;}

void MofkaStream::setInfo(json &j) {info = j;}

void MofkaStream::windowLength(uint32_t wlen){ window_len = wlen;}


