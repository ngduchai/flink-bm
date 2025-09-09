from setuptools import setup, Extension
import sys, pybind11, os


# TODO: Adjust these paths as necessary
# here = "/home/ndhai/diaspora/flink-bm/workloads/aps-mini-apps/"
# localview = "/home/ndhai/diaspora/spack-aps/var/spack/environments/APS/.spack-env/view/"
here = "/home/ndhai/src/flink-bm/workloads/aps-mini-apps"
# localview = "/home/ndhai/src/spack-aps/var/spack/environments/APS/.spack-env/view/"
# here = "/opt/workloads/aps-mini-apps"
localview = "/home/ndhai/src/spack-aps/var/spack/environments/APS/.spack-env/view/"
locallib = localview + "lib"
localinclude = localview + "include"

cxxargs = ["-O3", "-std=c++17"]
if sys.platform.startswith("linux"):
    cxxargs += ["-fPIC"]

ext = Extension(
    "sirt_ops",
    sources=[
        "src/sirt_bindings.cc",
        "src/sirt_engine.cc",
        "src/data_stream.cc",
        "src/tracelib/trace_utils.cc",
        "src/tracelib/sirt.cc",
        "src/sirt_common.cc"
    ],
    include_dirs=[
        pybind11.get_include(),
        pybind11.get_include(user=True),   # <— add user include path too
        os.path.join(here, "include"),
        os.path.join(here, "include/tracelib"),
        "/usr/include/hdf5/serial",
        localinclude
    ],
    # Minimal, but correct: use Boost.Serialization's actual lib name
    libraries=[
        "hdf5",
        "boost_serialization",  # <— was "boost"
        "pthread",              # <— needed on Linux
        # Add MPI only if you don't build with mpicxx:
        # "mpi", "mpi_cxx",
    ],
    library_dirs=[
        "/usr/local/lib",
        "/usr/lib/x86_64-linux-gnu/hdf5/serial/",
        locallib
    ],
    # Keep runtime search path so the module finds Spack libs at import time
    extra_link_args=[
        f"-Wl,-rpath,{locallib}"
    ],
    extra_compile_args=cxxargs,
    language="c++",
)

setup(
    name="sirt-ops",
    version="0.2.0",
    description="Pybind11 bindings for SirtEngine (checkpointable)",
    ext_modules=[ext],
)
