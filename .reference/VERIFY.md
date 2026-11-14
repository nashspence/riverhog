# Focused receiving and reproducibility

Start from the final reference diff relative to issue-authorized `6e80898a793532beff7a1718effc0618e2472d58`. Reconcile the test-recorder ownership against current integration source. Product/package implementation and declaration paths are unchanged; the public growth reader remains the independent authority for every received sample.

## Pinned environment and focused checks

The producer uses Python 3.12.3, uv 0.11.24, Ruff 0.16.0, and mypy 2.3.0 from the repository's locked environment. No dependency/lock changes are proposed. Follow the repository's normal locked setup when receiving.

Use an existing writable `TMPDIR` and explicit pytest `--basetemp` outside the checkout. Otherwise this workspace's unavailable default `/tmp` can cause pytest to create scratch within the repository and invalidate source-clean receipts.

```bash
export TMPDIR=/absolute/external/observer-lifetime-tmp
mkdir -p "$TMPDIR"
.venv/bin/python -m pytest -q \
  --basetemp "$TMPDIR/focused" \
  tests/unit/test_growth_observer_lifetime.py \
  tests/unit/test_growth_observer_batch.py \
  tests/unit/test_growth_declarations.py \
  tests/unit/test_validation_witnesses.py \
  tests/unit/test_private_growth_discovery.py \
  -k 'not actual_caller_and_unit_turns_share_a_bounded_observer_batch'
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m reuse lint
.venv/bin/python scripts/run_mypy.py
```

The old three-turn adopted-scope test is excluded from this focused command because this reference performs its actual turns through the explicit source-bound capture, preserving all root/child inventories and original cleanup receipts. Its small regression cases remain included. This is a finite producer selection, not removal or weakening of any maintained integration gate.

## Restore exact retained replay inputs

Run from the repository root with a new external destination:

```bash
.venv/bin/python - /absolute/external/lifetime-input <<'PY'
import base64, gzip, hashlib, json, sys
from pathlib import Path
from riverhog_canonical_json import canonical_json_bytes

root = Path.cwd().resolve()
destination = Path(sys.argv[1]).resolve()
assert not destination.is_relative_to(root) and not destination.exists()
expected = {
    "samples.json": "a9e69163ba9aac6c5cefd4f582c609997217421528840694449f24007f6c2b20",
    "events.json": "5e559ea611938996d6b47df6588e4a9f2fb7c5f5623a0942b13cf1f144333a25",
}
restored = {}
for name, digest in expected.items():
    source = root / ".reference/evidence" / ("baseline-" + name + ".gz.b64")
    raw = gzip.decompress(base64.b64decode(source.read_bytes()))
    assert hashlib.sha256(raw).hexdigest() == digest
    assert canonical_json_bytes(json.loads(raw)) == raw
    restored[name] = raw
destination.mkdir(parents=True)
for name, raw in restored.items():
    (destination / name).write_bytes(raw)
print(destination)
PY
```

Hashes establish input identity, not numeric acceptance. The unchanged public reader must still validate every supplied root and nested sample during replay.

## Actual capture boundary

The exact baseline script is `evidence/capture-baseline.py.txt`. It requires clean exact source and import ownership, runs exactly three real small caller turns, and retains original exit/source/cleanup receipts through `scripts/ci_timing.py run`. Its serializer-only failed predecessor is retained separately.

The candidate script is `evidence/capture-candidate.py.txt`; `--extended` selects descendant admission with a 64-root pending limit and requires all 122 independent root occurrences and 1,078 nested checks. It otherwise keeps the same three-turn geometry. Run only the finite receiving work needed; do not invoke a full census, `advance_cases`, `session_cases`, or implicit trajectory generators.

Both scripts accept `--checkout`, a new external `--output`, `--expected-source`, and the absolute pinned `--uv` binary. Put the maintained lane receipt and TMPDIR outside the audited repository. Preserve actual original exit status, including failures. Do not relabel a replay-only scratch cleanup as a new product-fixture cleanup.

## Literal helper replay boundary

The direct replay must use the frozen 122-root transcript, actual helper construction/emission/receiving paths, and the entire owner lifetime from admission through final flush/release. Preserve all independent child and parent occurrences and their 1,078 nested checks. Native completion stays pure under its wrappers; receiving stays at the existing outer sites.

Separate an instrumented count/category pass from the uninstrumented timing observation. Category timers must account for nesting instead of summing overlapping inclusive values. Active primitive product operations are not re-executed in this replay. A faster replay is evidence-handling improvement, not product throughput or full-path growth qualification. No timing threshold is an acceptance gate.
