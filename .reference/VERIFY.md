# Focused receiving and reproduction

Reconcile the numeric-evaluation delta after the accepted lookup-map slice against the receiving source. The baseline reader and lookup tests are retained exactly from `0e9bef9445213aadadb7218fc6e268cc324f35f3`; the audited integration base is `824383a8a8143aac033f86dd1562a5979f6ec4da`. Preserve unrelated work and existing reader budgets.

Producer executions identify clean implementation `c4bf7532b7a0e91ac9ec384ff5893e81eae7bf01`. The final handoff appends only `.reference`; reader/test bytes remain identical. A receiving execution must identify its own exact source, including a newer evidence-only commit, rather than relabeling the producer SHA. The latest public integration checkpoint remains `824383a`; audit the next published checkpoint before integration because it was not available for this handoff's source review.

Use the locked environment: Python 3.12.3, uv 0.11.24, pytest 9.1.1, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0. Temporary directories, pytest basetemp, receipts, and caches belong outside the checkout.

## Focused owner verification

The frozen driver is `evidence/verify.py.txt`. With pinned `uv` on PATH, an exact clean checkout, and new external destinations:

```bash
cp .reference/evidence/verify.py.txt /absolute/external/numeric-evaluation-verify.py
source_sha=$(git rev-parse HEAD)
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B \
  /absolute/external/numeric-evaluation-verify.py \
  --repo "$PWD" \
  --expected-sha "$source_sha" \
  --out /absolute/external/numeric-evaluation-verification
```

The driver selects only `test_growth_numeric_evaluation.py`, `test_growth_declarations.py`, and `test_growth_bounds.py`, with the maintained pytest timing plugin. It then runs repository Ruff, formatting, REUSE, and the maintained mypy entrypoint sequentially through the maintained child/timing wrappers. It checks exact source/tool pins, preserves original exits/logs/controller records, and removes only its owned temporary tree. This does not rerun the broad unit suite or actual caller fixtures.

## Retained replay

Use the exact retained 100-declaration projection, 122 independent roots, and 1,078 total sample nodes across roots and descendants. Preserve the original 46-group order from `evidence/prior-replay-run.json`, including overlapping descendant samples and every root occurrence. Their complete reachable subprojections contain 829 declaration occurrences. Each public call must parse and validate afresh.

The frozen comparison script is `evidence/reader-profile.py.txt`. It runs one separate diagnostic pass and one unmodified elapsed observation per reader, baseline before proposed. Complete calls include parsing, index construction, unit/graph checks, numeric checks, declaration digests, actual delegate records, and composition. Input decoding, behavioral comparisons, diagnostic preparation, equality checks, and reporting remain outside the unmodified timers.

### Restore original inputs and complete receipts

Run from the repository root with a new external destination. Restoration preserves original bytes without reserializing JSON, including malformed float types and large integers in adversarial cases.

```bash
.venv/bin/python - /absolute/external/numeric-evaluation-retained <<'PY'
import base64, gzip, hashlib, json, sys
from pathlib import Path

repo = Path.cwd().resolve()
destination = Path(sys.argv[1]).resolve()
assert not destination.is_relative_to(repo) and not destination.exists()
evidence = repo / ".reference/evidence"
restored = {}
for directory, manifest, prefix in (
    (evidence, "retained-inputs.json", "inputs"),
    (evidence / "profile", "retained-receipts.json", "profile"),
):
    for name, row in json.loads((directory / manifest).read_bytes()).items():
        assert Path(name).name == name
        assert Path(row["stored_path"]).name == row["stored_path"]
        encoded = (directory / row["stored_path"]).read_bytes()
        assert len(encoded) == row["stored_bytes"]
        if "stored_sha256" in row:
            assert hashlib.sha256(encoded).hexdigest() == row["stored_sha256"]
        raw = gzip.decompress(base64.b64decode(encoded))
        assert len(raw) == row["raw_bytes"]
        assert hashlib.sha256(raw).hexdigest() == row["sha256"]
        restored[Path(prefix) / name] = raw
destination.mkdir(parents=True)
for relative, raw in restored.items():
    output = destination / relative
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(raw)
print(destination)
PY
```

Inputs appear under `inputs/`; the full original run record and adversarial case document appear under `profile/`. Hashes establish identity. Fresh readers still establish numeric/composition acceptance. The derived `evidence/profile/summary.json` is navigation only and never replaces the complete original receipt.

### Execute the frozen paired comparison

The baseline Git object must be present because the script independently verifies the canonical owner's source against that accepted commit. If absent in a shallow checkout, fetch the exact source without changing the working tree:

```bash
git fetch origin 0e9bef9445213aadadb7218fc6e268cc324f35f3
```

Use a new empty external TMPDIR and new output/lane paths. These hashes pin the supplied reader/test bytes; reconcile changed receiving source explicitly rather than silently changing the baseline or accounting protocol.

```bash
export TMPDIR=/absolute/external/numeric-evaluation-profile-tmp
mkdir "$TMPDIR"
source_sha=$(git rev-parse HEAD)
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -B scripts/ci_timing.py run \
  --lane numeric-evaluation-reader \
  --output /absolute/external/numeric-evaluation-profile-lane.json \
  -- \
  .venv/bin/python -B .reference/evidence/reader-profile.py.txt \
  --checkout "$PWD" \
  --baseline-reader .reference/evidence/baseline-reader.py.txt \
  --projection /absolute/external/numeric-evaluation-retained/inputs/projection.json \
  --samples /absolute/external/numeric-evaluation-retained/inputs/samples.json \
  --groups-receipt .reference/evidence/prior-replay-run.json \
  --output /absolute/external/numeric-evaluation-profile-output \
  --expected-source "$source_sha" \
  --expected-reader-sha a6fa11e73a3886dbf950bba34e310ab27857c5e87af6ca77bb6cad756a059f40 \
  --expected-context-tests-sha 2650d46fa1e23137bf1958adc8dffe4f02e3da83ffbb2e129b8b9984bb724870 \
  --expected-numeric-tests-sha 8e12500aeccd012920c8333a6fb4ae36359dc0ce99471ab60c6efb9c0aae0226 \
  --uv /absolute/path/to/pinned/uv
```

Retain the original wrapper exit, stdout/stderr, lane, complete output, and source boundaries. The script removes its owned scratch and requires TMPDIR empty at exit; after the wrapper joins, remove that empty external directory. The exact producer invocation, file hashes, original exits, and actual cleanup are preserved in [execution-receipt.json](evidence/profile/execution-receipt.json).

Construction counts describe metadata work. Live checks, numeric expressions, fresh parses, canonical calls, samples, and delegate obligations must remain equal. Do not substitute diagnostic timings for unmodified observations, sum nested durations as additive costs, or treat cumulative entries as live memory. No timing acceptance threshold is introduced.

No additional actual caller, journal census, product publication, broad unit suite, or large trial is required to reproduce this packet. It does not qualify an unpublished integration checkpoint or replace normal integration validation.
