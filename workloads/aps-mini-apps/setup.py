from setuptools import setup, Extension
import sys, pybind11, os

# TODO: Adjust these paths as necessary
here = "/home/ndhai/diaspora/flink-bm/workloads/aps-mini-apps/"
localview = "/home/ndhai/diaspora/spack-aps/var/spack/environments/APS/.spack-env/view/"
locallib = localview + "lib"
localinclude = localview + "include"

cxxargs = ["-O3"]
if sys.platform.startswith("linux"):
    cxxargs += ["-fPIC"]

ext = Extension(
    "sirt_ops",
    sources=[
        "src/sirt_bindings.cc",
        "src/sirt.cc",
        # "src/data_stream.cc",
        "src/tracelib/trace_comm.cc",
        "src/tracelib/trace_h5io.cc",
        "src/tracelib/trace_utils.cc"
    ],
    include_dirs=[
        pybind11.get_include(),
        os.path.join(here, "include"),
        os.path.join(here, "include/tracelib"),
        localinclude
    ],
    # If you link external libs:
    libraries=["hdf5", "boost"],    # etc.
    library_dirs=["/usr/local/lib", locallib],
    # extra_link_args=["-Wl,-rpath,/opt/lib"],   # helpful when shipping .so's
    extra_compile_args=cxxargs,
    language="c++",
)

setup(
    name="sirt-ops",
    version="0.2.0",
    description="Pybind11 bindings for SirtEngine (checkpointable)",
    ext_modules=[ext],
)
