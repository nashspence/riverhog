# Focused receiving and reproduction

Reconcile the growth-reader diff from audited `6e80898a793532beff7a1718effc0618e2472d58` against the receiving source. The reader at inspected PR #979 source `824383a8a8143aac033f86dd1562a5979f6ec4da` matches that base exactly. Preserve the receiver's unrelated observer changes; this reference does not supply them as replacements.

Producer executions identify clean implementation `e4f6ccfc42d90cb69791ea1c507b6c79d21edc47`. The final handoff appends only `.reference`, leaving the reader and owning tests byte-identical to that implementation. A receiving run must identify its own exact clean source; it must not reuse the producer SHA for another checkout.

Use the repository's locked Python/uv environment. The producer uses Python 3.12.3, uv 0.11.24, pytest 9.1.1, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0. Use explicit writable TMPDIR and pytest basetemp outside the checkout so temporary files cannot invalidate a clean-source receipt.

## Focused checks

```bash
export TMPDIR=/absolute/external/context-lookups-tmp
mkdir -p "$TMPDIR"
.venv/bin/python -m pytest -q \
  --basetemp "$TMPDIR/focused" \
  tests/unit/test_growth_context_lookups.py \
  tests/unit/test_growth_declarations.py \
  tests/unit/test_growth_bounds.py \
  tests/unit/test_growth_observer_batch.py \
  tests/unit/test_validation_witnesses.py \
  tests/unit/test_private_growth_discovery.py \
  -k 'not actual_caller_and_unit_turns_share_a_bounded_observer_batch'
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m reuse lint
.venv/bin/python scripts/run_mypy.py
```

The actual caller fixture is excluded from this finite producer selection because the request uses retained evidence and requires no new physical/product trial. Existing resource-budget tests remain included. Their synthetic metadata/sample boundaries are reader regression cases, not product campaigns. No maintained integration test or gate is removed.

The exact verification driver is `evidence/verify.py.txt`. Copy it to a new external Python file, then run it with the checkout's `.venv/bin/python`, pinned uv on PATH, and `--repo`, `--expected-sha`, and a new external `--out`. It invokes the five commands sequentially through the maintained timing wrapper, enables the maintained pytest timing plugin, verifies source/tool pins, preserves original exits/logs, and removes only its owned external temporary tree. The original completed receipt and controller record are under `evidence/verification`.

## Retained inputs and comparison

The comparison must use the exact retained 100-declaration projection, actual 122-root sample inventory, and ordered 46-group transcript. Restore the archived lossless inputs outside the checkout and verify their original hashes. Preserve every declaration row and every group/root occurrence, including overlap between independent children and parent trees.

Each source/workload receives one separate diagnostic pass and one uninstrumented observation. Measure complete public calls, including fresh parsing, lookup-map construction, unit validation, and sample/digest/composition checks where applicable. Keep input decoding/copying and output-equivalence checks outside the paired timer. State exactly when an instrumented temporary reader is used; do not present its timings as unmodified performance.

The packet includes exact scripts, source/input hashes, original exit/source/cleanup receipts, adversarial equivalence results, and reproducible command arguments. A timing difference remains an observation rather than an acceptance threshold. The map entry counts are allocation-work accounting rather than a memory profile.

### Restore original inputs and full receipts

Run from the repository root with a new external destination. The procedure restores the original bytes without reserializing JSON; in particular, malformed float types in the adversarial cases remain intact.

```bash
.venv/bin/python - /absolute/external/context-lookups-retained <<'PY'
import base64, gzip, hashlib, json, sys
from pathlib import Path

repo = Path.cwd().resolve()
destination = Path(sys.argv[1]).resolve()
assert not destination.is_relative_to(repo) and not destination.exists()
reference = repo / ".reference/evidence"
restored = {}
for directory, manifest, prefix in (
    (reference, "retained-inputs.json", "inputs"),
    (reference / "profile", "retained-receipts.json", "profile"),
):
    for name, row in json.loads((directory / manifest).read_bytes()).items():
        encoded = (directory / row["stored_path"]).read_bytes()
        assert len(encoded) == row["stored_bytes"]
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

The two normative replay inputs are restored under `inputs/`. The complete original profile run record and the exact adversarial-case document are restored under `profile/`. Hashes establish identity; the fresh public readers still determine numeric/composition acceptance. `evidence/profile/summary.json` is only a compact navigation aid for the complete original run record.

### Run the frozen paired comparison

The frozen script is `evidence/reader-profile.py.txt`. It loads the exact archived baseline reader and the current clean checkout reader into separate modules. Diagnostic AST copies count the original dictionary constructions and ordered semantic visits; separate unmodified modules supply the elapsed observations. It also runs the maintained owning test file's 67 JSON cases against both readers outside the timed intervals.

After restoring inputs, run the following from the clean checkout. The output and lane receipt should use new external paths; TMPDIR must exist outside the repository.

```bash
export TMPDIR=/absolute/external/context-lookups-profile-tmp
mkdir -p "$TMPDIR"
source_sha=$(git rev-parse HEAD)
.venv/bin/python -B scripts/ci_timing.py run \
  --lane context-lookup-reader-profile \
  --output /absolute/external/context-lookups-profile-lane.json \
  -- \
  .venv/bin/python -B .reference/evidence/reader-profile.py.txt \
  --checkout "$PWD" \
  --baseline-reader .reference/evidence/baseline-reader.py.txt \
  --baseline-audit .reference/evidence/metadata-audit-run.json \
  --projection /absolute/external/context-lookups-retained/inputs/projection.json \
  --samples /absolute/external/context-lookups-retained/inputs/samples.json \
  --groups-receipt .reference/evidence/prior-replay-run.json \
  --output /absolute/external/context-lookups-profile-output \
  --expected-source "$source_sha" \
  --uv /absolute/path/to/pinned/uv
```

The exact producer argv/environment, original child/wrapper/lane exits, source boundaries, cleanup observations, and output hashes are in [execution-receipt.json](evidence/profile/execution-receipt.json). The input manifest pins every complete reachable subprojection and its sample-token group. Each independent reader call parses freshly; the benchmark does not pass previously accepted contexts between calls.

Do not substitute diagnostic timings for the unmodified observations or treat cumulative map entries as live memory. The fixed pass order and one observation per reader/workload support a finite comparison, not an extrapolation to a product campaign. No additional actual caller, publication fixture, census, or full CI run is required to reproduce this packet's bounded evidence.
