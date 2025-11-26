#!/usr/bin/env bash
set -euo pipefail

# Paths *inside the running container*
SRC=${SRC:-/opt/workloads/aps-mini-apps}     # repo path inside container (shared, read-only-ish)
WHEEL_DIR=${WHEEL_DIR:-/tmp/wheels}          # writable; can be shared for cache
PYBIN=${PYBIN:-python3}

# If you have host libs mounted in the running container at /opt/libs, keep it on the path
export LD_LIBRARY_PATH="/opt/libs:/usr/local/lib:/usr/lib/x86_64-linux-gnu/hdf5/serial:${LD_LIBRARY_PATH:-}"

mkdir -p "$WHEEL_DIR"
cd "$SRC"

# Decide if rebuild is needed (based on shared sources)
HASH_FILE="$WHEEL_DIR/sources.sha256"
CUR_HASH=$(
  { [ -d include ] && find include -type f -print0; \
    [ -d src ]     && find src -type f -print0; \
    [ -f setup.py ] && printf "%s\0" setup.py; \
    [ -f pyproject.toml ] && printf "%s\0" pyproject.toml; } \
  | xargs -0 sha256sum | sha256sum | awk '{print $1}'
)

NEED_BUILD=1
if [ -f "$HASH_FILE" ]; then
  OLD_HASH=$(cat "$HASH_FILE" || true)
  if [ "${OLD_HASH:-}" = "$CUR_HASH" ] && ls "$WHEEL_DIR"/sirt_ops-*.whl >/dev/null 2>&1; then
    echo "[install] Sources unchanged; using cached wheel in $WHEEL_DIR"
    NEED_BUILD=0
  fi
fi

if [ $NEED_BUILD -eq 1 ]; then
  echo "[install] Building wheel in per-process temp dir..."

  # Per-process build directory on node-local FS (avoid races on shared SRC)
  BUILD_ROOT="${BUILD_ROOT:-${TMPDIR:-/tmp}/sirt_ops_build_$$}"
  rm -rf "$BUILD_ROOT"
  mkdir -p "$BUILD_ROOT"

  # Copy minimal sources into the local build tree
  # (shared SRC is read-only for build; all writes land in BUILD_ROOT)
  [ -d include ] && cp -r include "$BUILD_ROOT"/
  [ -d src ] && cp -r src "$BUILD_ROOT"/
  [ -f setup.py ] && cp setup.py "$BUILD_ROOT"/
  [ -f pyproject.toml ] && cp pyproject.toml "$BUILD_ROOT"/
  [ -f MANIFEST.in ] && cp MANIFEST.in "$BUILD_ROOT"/

  cd "$BUILD_ROOT"

  # Clean any local build artifacts (not touching shared SRC)
  rm -rf build/ dist/
  "$PYBIN" -m pip -q install --upgrade pip build pybind11 setuptools wheel
  "$PYBIN" -m build

  WHEEL_PATH=$(ls dist/sirt_ops-*.whl | head -n1)
  [ -n "$WHEEL_PATH" ] || { echo "[install] No wheel produced in dist/"; exit 1; }

  # Copy wheel to shared cache dir
  cp -f "$WHEEL_PATH" "$WHEEL_DIR"/
  echo "$CUR_HASH" > "$HASH_FILE"
  echo "[install] Wheel placed in $WHEEL_DIR"

  # Optionally clean up local build dir (comment out if you want to inspect it)
  # rm -rf "$BUILD_ROOT"
fi

# Install wheel into container’s Python (cwd no longer matters)
WHEEL_TO_INSTALL=$(ls -t "$WHEEL_DIR"/sirt_ops-*.whl | head -n1)
echo "[install] Installing: $WHEEL_TO_INSTALL"
"$PYBIN" -m pip install --force-reinstall -U "$WHEEL_TO_INSTALL"

echo "[install] Verifying import..."
"$PYBIN" - <<'PY'
import sys, sirt_ops
print("Python:", sys.version)
print("sirt_ops:", sirt_ops.__file__)
PY
