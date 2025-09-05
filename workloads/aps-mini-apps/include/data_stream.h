#ifndef DATA_STREAM_H
#define DATA_STREAM_H

#include "data_region_base.h"
#include <cassert>
#include <time.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <utility>
#include "trace_data.h"
#include <csignal>
#include <functional>

class DataStreamEvent {
  public:
    std::unordered_map<std::string, std::string> metadata;
    int sequence_id;
    int projection_id;
    double theta;
    double center;
    float* data; // Pointer to the data segment

    DataStreamEvent(std::unordered_map<std::string, std::string> metadata,
      int seq_id, int proj_id, double th, double cen, float* dat)
      : metadata(metadata), sequence_id(seq_id), projection_id(proj_id),
      theta(th), center(cen), data(dat) {}
};

class DataStream
{
  private:
    uint32_t window_len;
    uint32_t counter;
    int comm_rank;
    // int comm_size;

    int progress;
    std::vector<DataStreamEvent> pending_events;
    bool end_of_stream = false;

    std::vector<float> vproj;
    std::vector<float> vtheta;
    // std::vector<const std::unordered_map<std::string, std::string>> vmeta;
    std::vector<std::reference_wrapper<const std::unordered_map<std::string,std::string>>> vmeta;

    /* Add streaming message to buffers
    * @param event: mofka event containing data and metadata
    */
    void addTomoMsg(DataStreamEvent event);


    /* Erase streaming message to buffers
    */
    void eraseBegTraceMsg();


    /* Generates a data region that can be processed by Trace
    * @param recon_image: reconstruction image

      return: DataRegionBase
    */
    DataRegionBase<float, TraceMetadata>* setupTraceDataRegion(
      DataRegionBareBase<float> &recon_image);

  public:

    int64_t n_sinograms = 0;
    int64_t n_rays_per_proj_row = 0;
    int64_t beg_sinograms = 0;
    int64_t tn_sinograms = 0;

    DataStream(uint32_t window_len, int rank, int progress=0);

    /* Create a data region from sliding window
    * @param recon_image Initial values of reconstructed image
    * @param step        Sliding step. Waits at least step projection
    *                    before returning window back to the reconstruction
    *                    engine
    *
    * Return:  nullptr if there is no message and sliding window is empty
    *          DataRegionBase if there is data in sliding window
    */
    DataRegionBase<float, TraceMetadata>* readSlidingWindow(
      DataRegionBareBase<float> &recon_image, int step,
      const std::unordered_map<std::string, std::string>& metadata, const float *data);

    int getRank();

    int getBufferSize();

    uint32_t getCounter();

    void windowLength(uint32_t wlen);

    int getProgress() { return progress; }
    void updateProgress(int progress) { this->progress = progress; } // Update progress for streaming control

    bool isEndOfStream() { return end_of_stream; }
    void setEndOfStream(bool eos) { end_of_stream = eos; } // Update end of stream flag
  

};
#endif // DATA_STREAM_H