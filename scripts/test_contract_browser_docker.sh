#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mise_bin="${MISE_BIN:-mise}"
cd "$repo_root"
versions="$("$mise_bin" x -- python - <<'PY'
import re
import tomllib
from pathlib import Path

tools = tomllib.loads(Path("mise.toml").read_text(encoding="utf-8"))["tools"]
project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
uv_version = tools["uv"]
browser = project["dependency-groups"]["browser"]
if len(browser) != 1 or not re.fullmatch(r"playwright==\d+\.\d+\.\d+", browser[0]):
    raise SystemExit("browser fallback requires one exact Playwright version")
if not re.fullmatch(r"\d+\.\d+\.\d+", uv_version):
    raise SystemExit("browser fallback requires an exact mise-owned uv version")
print(uv_version, browser[0].split("==", 1)[1])
PY
)"
read -r uv_version playwright_version <<< "$versions"

docker run --rm --init --shm-size=1g \
  --mount "type=bind,src=${repo_root},dst=/source,readonly" \
  --env "RIVERHOG_UV_VERSION=${uv_version}" \
  "mcr.microsoft.com/playwright/python:v${playwright_version}-noble" \
  bash -euo pipefail -c '
    mkdir -p /workspace
    tar -C /source \
      --exclude=.git --exclude=.venv --exclude=.mypy_cache \
      --exclude=.pytest_cache --exclude=.ruff_cache --exclude=__pycache__ \
      -cf - . | tar -C /workspace -xf -
    cd /workspace
    python -m pip install --disable-pip-version-check --root-user-action=ignore \
      --quiet "uv==${RIVERHOG_UV_VERSION}"
    UV_PROJECT_ENVIRONMENT=/tmp/riverhog-browser-venv \
      uv run --locked --all-packages --group dev --group browser \
      python -m pytest -q tests/browser
  '
