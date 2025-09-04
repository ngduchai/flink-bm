#ifndef SIRT_H
#define SIRT_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "data_stream.h"
#include "trace_h5io.h"
#include "disp_engine_reduction.h"
#include "disp_engine_base.h"
#include "reduction_space_a.h"
#include "sirt_recon_space.h"

struct ProcessResult {
    std::vector<std::uint8_t> data;  // output bytes
    std::unordered_map<std::string, std::string> meta; // output metadata
};

class SirtEngine {

private:
    int task_id = -1;
    DataStream ds;
    trace_io::H5Metadata h5md;
    int passes = 0;
    SIRTReconSpace* main_recon_space = nullptr;
    DataRegion2DBareBase<float>* main_recon_replica = nullptr;
    DISPEngineBase<SIRTReconSpace, float> *engine = nullptr;
    DataRegionBareBase<float> *recon_image = nullptr;
    int window_step = 1;

public:

    SirtEngine()
        : ds(0, 0, 0), main_recon_space(nullptr), engine(nullptr),
          recon_image(nullptr), main_recon_replica(nullptr), window_step(0), passes(0) {
        h5md.ndims = 0;
        h5md.dims  = nullptr;
    }

    ProcessResult process(
        const std::unordered_map<std::string, int>& config,
        const std::unordered_map<std::string, std::string>& metadata,
        const float* data,
        std::size_t len
    );

    void setup(const std::unordered_map<std::string, int64_t>& tmetadata);

    // checkpointing
    std::vector<std::uint8_t> snapshot() const;
    void restore(const std::vector<std::uint8_t>& snapshot);

    ~SirtEngine();

};

#endif // SIRT_H
