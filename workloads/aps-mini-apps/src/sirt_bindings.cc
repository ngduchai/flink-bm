#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include "sirt_engine.h"
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>
#include <cstdlib>   // std::strtoull, std::strtoll
#include <pybind11/iostream.h>

namespace py = pybind11;

// Normalizes any bytes/bytearray/memoryview/ndarray(float32, C) to an aligned float vector.
static std::vector<float> to_float_vector(py::object obj) {
    // Fast path: numpy array of float32, C contiguous
    if (py::isinstance<py::array>(obj)) {
        py::array arr = py::cast<py::array>(obj);
        // forcecast will convert e.g. float64 -> float32; c_style ensures contiguous
        auto a = arr.cast<py::array_t<float, py::array::c_style | py::array::forcecast>>();
        auto* p = a.data();
        const auto n = static_cast<std::size_t>(a.size());
        return std::vector<float>(p, p + n);
    }

    // Generic buffer (bytes/bytearray/memoryview)
    py::buffer buf = py::reinterpret_borrow<py::buffer>(obj);
    py::buffer_info info = buf.request(false);
    if (!info.ptr) throw std::runtime_error("Null buffer pointer.");

    const std::size_t itemsize = static_cast<std::size_t>(info.itemsize);
    const std::size_t nitems   = static_cast<std::size_t>(info.size);

    if (itemsize == 1) {
        // Raw bytes: length must be multiple of 4
        if (nitems % sizeof(float) != 0)
            throw std::runtime_error("Byte payload length is not a multiple of 4 (float32).");
        const auto nfloat = nitems / sizeof(float);
        std::vector<float> out(nfloat);
        std::memcpy(out.data(), info.ptr, nitems);
        return out;
    }
    if (itemsize == sizeof(float)) {
        const auto* p = static_cast<const float*>(info.ptr);
        return std::vector<float>(p, p + nitems);
    }
    throw std::runtime_error("Unsupported payload: expected bytes/bytearray/memoryview or float32 ndarray.");
}

static std::size_t expect_len_from_meta(const std::unordered_map<std::string, std::string>& meta) {
    auto it_cols = meta.find("n_rays_per_proj_row");
    auto it_rows = meta.find("n_sinograms_rank");
    if (it_cols == meta.end() || it_rows == meta.end()) return 0; // can’t validate
    const std::size_t cols = std::strtoull(it_cols->second.c_str(), nullptr, 10);
    const std::size_t rows = std::strtoull(it_rows->second.c_str(), nullptr, 10);
    return cols * rows;
}

PYBIND11_MODULE(sirt_ops, m) {

    py::add_ostream_redirect(m, "ostream_redirect");

    py::class_<SirtEngine>(m, "SirtEngine")
        .def(py::init<>())                         // <-- only default ctor
        .def("setup", &SirtEngine::setup)          // expose setup so you can init later

        .def("process",
            [](SirtEngine& self,
               const std::unordered_map<std::string, int64_t>& cfg,
               const std::unordered_map<std::string, std::string>& meta_in,
               py::object payload) {
                py::buffer_info info;

                py::scoped_ostream_redirect out(std::cout, py::module_::import("sys").attr("stdout"));
                py::scoped_ostream_redirect err(std::cerr, py::module_::import("sys").attr("stderr"));

                try {
                    // Normalize input to an aligned, owned vector
                    auto data = to_float_vector(payload);
                    // if (data.empty()) throw std::runtime_error("process(): empty payload");

                    // Shape sanity-check (catches most “misinterpretation” issues)
                    if (const auto expect = expect_len_from_meta(meta_in); expect && expect != data.size()) {
                        throw std::runtime_error(
                            "process(): payload length mismatch: got " + std::to_string(data.size()) +
                            " floats, expected " + std::to_string(expect) +
                            " (= n_sinograms_rank * n_rays_per_proj_row).");
                    }
            
                    ProcessResult r;
                    {   // release GIL only while calling into C++
                        py::gil_scoped_release nogil;
                        r = self.process(cfg, meta_in, data.data(), data.size());
                    }
            
                    // GIL is re-acquired here (nogil dtor ran)
            
                    // Build Python objects safely
                    const char* bytes_ptr = reinterpret_cast<const char*>(r.data.data());
                    const std::size_t nbytes = r.data.size() * sizeof(float);
            
                    py::bytes out = (nbytes == 0)
                        ? py::bytes()                                  // empty bytes
                        : py::bytes(bytes_ptr, nbytes);                // copy into Python
            
                    return py::make_tuple(out, r.meta);
                } catch (const std::exception& e) {
                    throw std::runtime_error(std::string("SirtEngine.process failed: ") + e.what());
                } catch (...) {
                    PyErr_SetString(PyExc_RuntimeError, "Unknown C++ exception");
                    throw py::error_already_set();
                }
            },
            py::arg("config"), py::arg("metadata"), py::arg("payload"))

        .def("snapshot",
            [](const SirtEngine& self, int selected_id) {
                py::gil_scoped_release g;
                auto v = self.snapshot(selected_id);
                return py::bytes(reinterpret_cast<const char*>(v.data()), v.size());
            })

        .def("restore",
            [](SirtEngine& self, py::object snap_bytes) {
                py::buffer_info info;
                py::buffer buf = py::reinterpret_borrow<py::buffer>(snap_bytes);
                info = buf.request(false);

                if (info.itemsize != 1)
                    throw std::runtime_error("restore expects a bytes-like object.");

                const std::size_t nbytes =
                    static_cast<std::size_t>(info.size) * static_cast<std::size_t>(info.itemsize);

                std::vector<std::uint8_t> snap(nbytes);
                std::memcpy(snap.data(), info.ptr, nbytes);

                py::gil_scoped_release g;
                self.restore(snap);
                // auto ret = self.restore(snap);
                // return py::bytes(reinterpret_cast<const char*>(ret.data()), ret.size());
            });
}
