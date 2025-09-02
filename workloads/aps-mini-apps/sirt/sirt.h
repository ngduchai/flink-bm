#pragma once
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

struct ProcessResult {
    std::vector<std::uint8_t> data;  // output bytes
    std::unordered_map<std::string, std::string> meta; // output metadata
};

class SirtEngine {
public:
    ProcessResult process(
        const std::unordered_map<std::string, int>& config,
        const std::unordered_map<std::string, std::string>& metadata,
        const std::uint8_t* data,
        std::size_t len
    );

    // checkpointing
    std::vector<std::uint8_t> snapshot() const;
    std::vector<std::uint8_t> restore(const std::vector<std::uint8_t>& snapshot);
};