set -euo pipefail

# Paths inside the container
SRC=/opt/workloads/aps-mini-apps   # repo root bind-mounted from host
WHEEL_DIR=/tmp/wheels              # persistent wheel cache (bind-mounted)
VENV=/opt/venv                     # build/run venv
PYBIN=python3

# Optional: libs your C++ depends on
export LD_LIBRARY_PATH="/opt/libs:/usr/local/lib:/usr/lib/x86_64-linux-gnu/hdf5/serial/:${LD_LIBRARY_PATH}"   # adjust if you mounted external .so here

mkdir -p "$WHEEL_DIR"

# # 1) Create venv and install build deps
# if [ ! -d "$VENV" ]; then
#   $PYBIN -m venv "$VENV"
# fi
# source "$VENV/bin/activate"
# python3 -m pip install --upgrade pip
# python3 -m pip install --upgrade build pybind11 setuptools wheel

# 2) Compute a quick hash of sources to decide if we need rebuild
cd "$SRC"
HASH_FILE="$WHEEL_DIR/sources.sha256"
CUR_HASH=$( ( [ -d include ] && find include -type f -print0; [ -d src ] && find src -type f -print0; [ -f setup.py ] && printf "%s\0" setup.py; [ -f pyproject.toml ] && printf "%s\0" pyproject.toml ) \
  | xargs -0 sha256sum | sha256sum | awk "{print \$1}" )

NEED_BUILD=1
if [ -f "$HASH_FILE" ]; then
  OLD_HASH=$(cat "$HASH_FILE")
  if [ "$OLD_HASH" = "$CUR_HASH" ] && ls "$WHEEL_DIR"/sirt_ops-*.whl >/dev/null 2>&1; then
    echo "[build] Sources unchanged; using cached wheel."
    NEED_BUILD=0
  fi
fi

# 3) Build wheel if needed
if [ $NEED_BUILD -eq 1 ]; then
  echo "[build] Building wheel..."
  # Clean previous artifacts
  rm -rf build/ dist/
  python3 -m build
  cp -f dist/sirt_ops-*.whl "$WHEEL_DIR"/
  echo "$CUR_HASH" > "$HASH_FILE"
  echo "[build] Wheel placed in $WHEEL_DIR"
fi

# 4) Export runtime env so PyFlink can import the wheel if needed
export PYTHONPATH="$WHEEL_DIR:${PYTHONPATH:-}"

# 5) (Optional) show the wheel we will ship
echo "[build] Available wheel(s):"
ls -lh "$WHEEL_DIR"/sirt_ops-*.whl || true
