#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "sirt.h"
#include <cstring>

namespace py = pybind11;

static std::pair<const std::float32_t*, std::size_t>
as_ro_buffer(py::object obj, py::buffer_info& info) {
    py::buffer buf = py::reinterpret_borrow<py::buffer>(obj);
    info = buf.request(false);
    if (info.itemsize != 1) throw std::runtime_error("Expected a byte-like buffer (itemsize=1).");
    return { static_cast<const std::float32_t*>(info.ptr), static_cast<std::size_t>(info.size) };
}

PYBIND11_MODULE(sirt_ops, m) {
    py::class_<SirtEngine>(m, "SirtEngine")
        .def(py::init<>())

        // (cfg, meta_in, payload) -> (out_bytes, out_meta)
        .def("process",
            [](SirtEngine& self,
               const std::unordered_map<std::string, int>& cfg,
               const std::unordered_map<std::string, std::string>& meta_in,
               py::object payload) {
                py::buffer_info info;
                auto [ptr, n] = as_ro_buffer(payload, info);

                py::gil_scoped_release g;
                ProcessResult r = self.process(cfg, meta_in, ptr, n);

                py::bytes out_bytes(reinterpret_cast<const char*>(r.data.data()), r.data.size());
                return py::make_tuple(out_bytes, r.meta);
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
                auto [ptr, n] = as_ro_buffer(snap_bytes, info);
                std::vector<std::uint8_t> snap(n);
                std::memcpy(snap.data(), ptr, n);

                py::gil_scoped_release g;
                auto ret = self.restore(snap);
                return py::bytes(reinterpret_cast<const char*>(ret.data()), ret.size());
            });
}