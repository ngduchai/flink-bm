#ifndef SIRT_ENGINE_H
#define SIRT_ENGINE_H

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>
#include <sstream>

#include "data_stream.h"
#include "disp_engine_reduction.h"
#include "disp_engine_base.h"
#include "reduction_space_a.h"
#include "sirt_recon_space.h"

#include <boost/archive/binary_iarchive.hpp>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/base_object.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/export.hpp>

// Forward declare your existing class template
template <typename T> class DataRegionBareBase;

struct ProcessResult {
    std::vector<float> data;
    std::unordered_map<std::string, std::string> meta;
};

class SirtEngine {
private:
    int task_id = -1;
    DataStream ds;
    trace_io::H5Metadata h5md;
    SIRTReconSpace* main_recon_space = nullptr;
    DISPEngineBase<SIRTReconSpace, float>* engine = nullptr;
    DataRegionBareBase<float>* recon_image = nullptr;
    DataRegion2DBareBase<float>* main_recon_replica = nullptr;
    int window_step = 1;
    int passes = 0;

public:
    SirtEngine()
        : ds(0, 0, 0), main_recon_space(nullptr), engine(nullptr),
          recon_image(nullptr), main_recon_replica(nullptr), window_step(0), passes(0) {
        h5md.ndims = 0;
        h5md.dims  = nullptr;
    }

    ProcessResult process(
        const std::unordered_map<std::string, int64_t>& config,
        const std::unordered_map<std::string, std::string>& metadata,
        const float* data,
        std::size_t len
    );

    void setup(const std::unordered_map<std::string, int64_t>& tmetadata);

    std::vector<std::uint8_t> snapshot() const;
    void restore(const std::vector<std::uint8_t>& snapshot);

    ~SirtEngine();
};

struct SirtCkpt {
    int progress{0};
    DataRegionBareBase<float>* recon_image{nullptr};  // << init to nullptr

    SirtCkpt() = default;
    SirtCkpt(int progress_, DataRegionBareBase<float>* img)
        : progress(progress_), recon_image(img) {}

    explicit SirtCkpt(const std::vector<std::uint8_t>& snapshot) {
        from_bytes_(snapshot);
    }

    std::vector<std::uint8_t> to_bytes() const {
        std::ostringstream oss(std::ios::binary);
        {
            boost::archive::binary_oarchive oa(oss);
            oa << *this;
        }
        const std::string& s = oss.str();
        return std::vector<std::uint8_t>(s.begin(), s.end());
    }

private:
    friend class boost::serialization::access;

    template <class Archive>
    void save(Archive& ar, const unsigned) const {
        ar & progress;
        ar & recon_image;   // pointer (polymorphic)
    }
    template <class Archive>
    void load(Archive& ar, const unsigned) {
        ar & progress;
        recon_image = nullptr;
        ar & recon_image;   // Boost allocates correct derived (needs EXPORT)
    }
    BOOST_SERIALIZATION_SPLIT_MEMBER();

    void from_bytes_(const std::vector<std::uint8_t>& snapshot) {
        std::istringstream iss(
            std::string(reinterpret_cast<const char*>(snapshot.data()), snapshot.size()),
            std::ios::binary);
        boost::archive::binary_iarchive ia(iss);
        ia >> *this;
    }
};

#endif // SIRT_ENGINE_H