#ifndef DATA_STREAM_H
#define DATA_STREAM_H

#include "data_region_base.h"
#include <cassert>
#include <fmt/format.h>
#include <time.h>
#include <string>
#include <iostream>
#include <fstream>
#include <vector>
#include <utility>
#include <nlohmann/json.hpp>
#include "trace_data.h"
#include <csignal>

using json = nlohmann::json;
namespace tl = thallium;

class DataStream
{
  private:
    uint32_t window_len;
    uint32_t counter;
    int comm_rank;
    // int comm_size;

    int progress;
    std::vector<mofka::Event> pending_events;
    bool end_of_stream = false;

    std::vector<float> vproj;
    std::vector<float> vtheta;
    std::vector<json> vmeta;
    json info;

    /* Add streaming message to buffers
    * @param event: mofka event containing data and metadata
    */
    void addTomoMsg(mofka::Event event);


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
      DataRegionBareBase<float> &recon_image,
      json metadata, float *data);

    json getInfo();

    int getRank();

    int getBufferSize();

    uint32_t getBatch();

    uint32_t getCounter();

    void setInfo(json &j);

    void windowLength(uint32_t wlen);

    int getProgress() { return progress; }
    void updateProgress(int progress) { this->progress = progress; } // Update progress for streaming control

    bool isEndOfStream() { return end_of_stream; }
    void setEndOfStream(bool eos) { end_of_stream = eos; } // Update end of stream flag
  

};
#endif // DATA_STREAM_H