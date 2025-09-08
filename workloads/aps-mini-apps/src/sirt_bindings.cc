#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/numpy.h>
#include "sirt_engine.h"
#include <cstdint>
#include <cstring>
#include <stdexcept>

namespace py = pybind11;

static std::pair<const float*, std::size_t>
as_ro_float_buffer(py::object obj, py::buffer_info& info) {
    py::buffer buf = py::reinterpret_borrow<py::buffer>(obj);
    info = buf.request(false);

    const void* ptr = info.ptr;
    if (!ptr) throw std::runtime_error("Null buffer pointer.");

    const std::size_t itemsize = static_cast<std::size_t>(info.itemsize);
    const std::size_t nitems   = static_cast<std::size_t>(info.size);

    if (itemsize == sizeof(float)) {
        return { static_cast<const float*>(ptr), nitems };
    }
    if (itemsize == 1) {
        const std::size_t nbytes = nitems;
        if (nbytes % sizeof(float) != 0)
            throw std::runtime_error("Byte payload length is not a multiple of 4 (float32).");
        return { reinterpret_cast<const float*>(ptr), nbytes / sizeof(float) };
    }
    throw std::runtime_error("Unsupported payload: expected bytes/bytearray or float32 array.");
}

PYBIND11_MODULE(sirt_ops, m) {
    py::class_<SirtEngine>(m, "SirtEngine")
        .def(py::init<>())                         // <-- only default ctor
        .def("setup", &SirtEngine::setup)          // expose setup so you can init later

        .def("process",
            [](SirtEngine& self,
               const std::unordered_map<std::string, int>& cfg,
               const std::unordered_map<std::string, std::string>& meta_in,
               py::object payload) {
                py::buffer_info info;
                auto [ptr, n] = as_ro_float_buffer(payload, info);
                py::object keep_alive = payload;    // keep exporter alive

                py::gil_scoped_release g;
                ProcessResult r = self.process(cfg, meta_in, ptr, n);

                return py::make_tuple(
                    py::bytes(reinterpret_cast<const char*>(r.data.data()), r.data.size()),
                    r.meta
                );
            },
            py::arg("config"), py::arg("metadata"), py::arg("payload"))

        .def("snapshot",
            [](const SirtEngine& self) {
                py::gil_scoped_release g;
                auto v = self.snapshot();
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
