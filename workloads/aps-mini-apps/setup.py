from setuptools import setup, Extension
import sys, pybind11

cxxargs = ["-O3"]
if sys.platform.startswith("linux"):
    cxxargs += ["-fPIC"]

ext = Extension(
    "sirt_ops",
    sources=[
        "sirt/sirt_bindings.cpp",
        "sirt/sirt.cpp",   # add all your engine .cpp files here
    ],
    include_dirs=[
        pybind11.get_include(),
        "cpp",                  # path where SirtEngine.hpp lives
        # add include dirs for DataRegionBase/TraceMetadata if needed
    ],
    # libraries=["adios2", "hdf5", ...],      # if you link external libs
    # library_dirs=["/opt/libs", ...],
    extra_compile_args=cxxargs,
    language="c++",
)

setup(
    name="sirt-ops",
    version="1.0.0",
    description="Pybind11 bindings for SirtEngine with checkpointing",
    ext_modules=[ext],
)