#!/usr/bin/env bash
set -euo pipefail

# Paths *inside the running container*
SRC=${SRC:-/opt/workloads/aps-mini-apps}     # repo path inside container
WHEEL_DIR=${WHEEL_DIR:-/tmp/wheels}          # writable; persists only if container mounts shared tmp
PYBIN=${PYBIN:-python3}

# If you have host libs mounted in the running container at /opt/libs, keep it on the path
export LD_LIBRARY_PATH="/opt/libs:/usr/local/lib:/usr/lib/x86_64-linux-gnu/hdf5/serial:${LD_LIBRARY_PATH:-}"

mkdir -p "$WHEEL_DIR"
cd "$SRC"

# Decide if rebuild is needed
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
  echo "[install] Building wheel..."
  # rm -rf build/ dist/
  "$PYBIN" -m pip -q install --upgrade pip build pybind11 setuptools wheel
  "$PYBIN" -m build
  WHEEL_PATH=$(ls dist/sirt_ops-*.whl | head -n1)
  [ -n "$WHEEL_PATH" ] || { echo "[install] No wheel produced in dist/"; exit 1; }
  cp -f "$WHEEL_PATH" "$WHEEL_DIR"/
  echo "$CUR_HASH" > "$HASH_FILE"
  echo "[install] Wheel placed in $WHEEL_DIR"
fi

# Install wheel into container’s Python
WHEEL_TO_INSTALL=$(ls -t "$WHEEL_DIR"/sirt_ops-*.whl | head -n1)
echo "[install] Installing: $WHEEL_TO_INSTALL"
"$PYBIN" -m pip install --force-reinstall -U "$WHEEL_TO_INSTALL"

echo "[install] Verifying import..."
"$PYBIN" - <<'PY'
import sys, sirt_ops
print("Python:", sys.version)
print("sirt_ops:", sirt_ops.__file__)
PY
