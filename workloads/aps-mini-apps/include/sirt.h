#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include "data_stream.h"
#include "trace_h5io.h"

struct ProcessResult {
    std::vector<std::uint8_t> data;  // output bytes
    std::unordered_map<std::string, std::string> meta; // output metadata
};

class SirtEngine {

private:
    DataStream ds;
    trace_io::H5Metadata h5md;
    int passes = 0;
    SIRTReconSpace* main_recon_space = nullptr;
    DISPEngineBase<SIRTReconSpace, float> *engine = nullptr;
    DataRegionBareBase<float> *recon_image = nullptr;

public:
    ProcessResult process(
        const std::unordered_map<std::string, int>& config,
        const std::unordered_map<std::string, std::string>& metadata,
        const std::uint8_t* data,
        std::size_t len
    );

    void setup(const std::unordered_map<std::string, int64_t>& tmetadata);

    // checkpointing
    std::vector<std::uint8_t> snapshot() const;
    std::vector<std::uint8_t> restore(const std::vector<std::uint8_t>& snapshot);

    ~SirtEngine();

};