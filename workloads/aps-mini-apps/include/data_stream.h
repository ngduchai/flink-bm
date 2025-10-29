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
#include <algorithm>
#include <sirt_common.h>

class DataStreamEvent {
  public:
    std::unordered_map<std::string, std::string> metadata;
    int sequence_id;
    int projection_id;
    double theta;
    double center;
    // const float* data; // Pointer to the data segment
    std::vector<float> data; // Pointer to the data segment



    // DataStreamEvent(std::unordered_map<std::string, std::string> metadata,
    //   int seq_id, int proj_id, double th, double cen, const float* dat, std::size_t size)
    //   : metadata(metadata), sequence_id(seq_id), projection_id(proj_id),
    //   theta(th), center(cen), data(dat), data_size(size) {}
    DataStreamEvent(std::unordered_map<std::string, std::string> metadata,
      int seq_id, int proj_id, double th, double cen, const float* dat, std::size_t size)
      : metadata(metadata), sequence_id(seq_id), projection_id(proj_id),
      theta(th), center(cen) {

        if (dat != nullptr && size > 0) {
          // 1) compute checksum from the raw pointer + count
          const uint32_t checksum = fnv1a32(dat, size);
  
          // 2) if metadata carries an expected checksum, compare after parsing
          if (auto it = this->metadata.find("checksum"); it != this->metadata.end()) {
              // accept hex (0x...) or decimal
              const std::string& s = it->second;
              uint32_t expected = 0;
              try {
                  size_t pos = 0;
                  int base = 10;
                  if (s.rfind("0x", 0) == 0 || s.rfind("0X", 0) == 0) base = 16;
                  expected = static_cast<uint32_t>(std::stoul(s, &pos, base));
                  // optional: ensure whole string parsed
                  // if (pos != s.size()) { /* handle trailing chars */ }
              } catch (const std::exception&) {
                  // If parse fails, you can log or assert; here we assert
                  assert(false && "metadata['checksum'] is not a valid integer");
              }
              if (expected != 0) {
                assert(expected == checksum && "Checksum mismatch for DataStreamEvent");
              }
          }
  
          // 3) copy payload into the vector
          data.clear();
          data.insert(data.end(), dat, dat + size);
  
          // 4) re-check after copy using the vector overload
          const uint32_t checksum_data = fnv1a32(data);
          assert(checksum_data == checksum && "Checksum mismatch after copy");

          auto row_id = require_str(metadata, "row_id");
          std::cout << "[Row-" << row_id << "]: seq_id: " << sequence_id << " projection_id: " << proj_id << " theta: " << theta << " checksum: " << checksum_data <<  " center: " << center << std::endl;
      }
    }

    // ~DataStreamEvent() {
    //   if (data != nullptr) {
    //     delete [] data;
    //   }
    // }
};

class DataStream
{
  private:
    uint32_t counter;
    int comm_rank;
    // int comm_size;

    int progress;
    std::vector<DataStreamEvent> pending_events;
    bool end_of_stream = false;

    std::vector<float> vproj;
    std::vector<float> vtheta;
    // std::vector<const std::unordered_map<std::string, std::string>> vmeta;
    // std::vector<std::unordered_map<std::string,std::string>> vmeta;
    std::vector<float> vcenters;

    /* Add streaming message to buffers
    * @param event: mofka event containing data and metadata
    */
    void addTomoMsg(DataStreamEvent& event);


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
    uint32_t window_len = 0;
    int row_id = -1;

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
      const std::unordered_map<std::string, std::string>& metadata,
      const float *data, std::size_t data_size);

    int getRank();
    void setRank(int rank);

    int getRow() { return row_id; }

    int getBufferSize();

    uint32_t getCounter();

    void windowLength(uint32_t wlen);

    int getProgress() { return progress; }
    void updateProgress(int progress) { this->progress = progress; } // Update progress for streaming control

    bool isEndOfStream() { return end_of_stream; }
    void setEndOfStream(bool eos) { end_of_stream = eos; } // Update end of stream flag
  

};
#endif // DATA_STREAM_H