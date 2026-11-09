#!/bin/sh
set -eu

# Check image contents without a daemon, network, elevated identity or host mounts.
# This does not establish the runtime requirements described in README.md.
for command in cc c++ curl dbus-daemon dbus-run-session file git jq make perl \
    pkg-config python3 sudo systemctl unshare unzip xz zip; do
    command -v "$command" >/dev/null
done
test -x /usr/lib/systemd/systemd
mise --version
docker --version
docker buildx version
docker compose version
file --version
python3 - <<'PY'
import ctypes
import ctypes.util

library = ctypes.util.find_library("magic")
assert library is not None, "libmagic is required by native provenance observation"
ctypes.CDLL(library)
PY
printf '%s\n' 'Qualification image contents verified; runtime qualification is separate.'
