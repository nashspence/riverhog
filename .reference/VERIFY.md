# Focused receiving and reproduction

Reconcile the growth-reader diff from audited `6e80898a793532beff7a1718effc0618e2472d58` against the receiving source. The reader at inspected PR #979 source `824383a8a8143aac033f86dd1562a5979f6ec4da` matches that base exactly. Preserve the receiver's unrelated observer changes; this reference does not supply them as replacements.

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

## Retained inputs and comparison

The comparison must use the exact retained 100-declaration projection, actual 122-root sample inventory, and ordered 46-group transcript. Restore the archived lossless inputs outside the checkout and verify their original hashes. Preserve every declaration row and every group/root occurrence, including overlap between independent children and parent trees.

Each source/workload receives one separate diagnostic pass and one uninstrumented observation. Measure complete public calls, including fresh parsing, lookup-map construction, unit validation, and sample/digest/composition checks where applicable. Keep input decoding/copying and output-equivalence checks outside the paired timer. State exactly when an instrumented temporary reader is used; do not present its timings as unmodified performance.

The final packet includes exact scripts, source/input hashes, original exit/source/cleanup receipts, adversarial equivalence results, and reproducible command arguments. A timing difference remains an observation rather than an acceptance threshold. The map entry counts are allocation-work accounting rather than a memory profile.
