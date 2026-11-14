# Focused receiving and reproduction

Reconcile against current authoritative source before integrating. The base is `8c80903818b386f50ddce8cd316cb3d93ec8cc51`; the proposed source changes test support and recorder wiring only. The public growth-contract package is unchanged. Do not transplant `.reference` evidence as qualification of a different source.

## Pinned tools and external scratch

The producer used the repository's Python 3.12.3 environment, uv 0.11.24, Ruff 0.16.0, and mypy 2.3.0. Follow the repository's locked setup in the receiving environment. The producer had no `mise` binary and therefore used direct commands from the locked `.venv` rather than claiming the Make frontend ran.

Set `TMPDIR` to an existing writable directory outside the repository, and put pytest `--basetemp` and all receipt outputs outside the checkout. The producer's workspace lacks a writable default `/tmp`; otherwise Python may fall back to creating pytest scratch inside the repository and invalidate a clean-source receipt.

```bash
export TMPDIR=/absolute/external/observer-tmp
mkdir -p "$TMPDIR"
.venv/bin/python -m pytest -q \
  --basetemp "$TMPDIR/focused-pytest" \
  tests/unit/test_growth_observer_batch.py
```

The new module includes the small three-turn actual caller fixture. It uses zero unrelated rows and the existing real fixture, checks all six caller/unit roots, compares their sample bytes through the immediate reader, closes the generator, and checks zero catalog connections.

The focused receiving suite additionally includes:

```bash
.venv/bin/python -m pytest -q \
  --basetemp "$TMPDIR/reader-pytest" \
  tests/unit/test_growth_declarations.py \
  tests/unit/test_validation_witnesses.py \
  tests/unit/test_private_growth_discovery.py
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/python -m reuse lint
.venv/bin/python scripts/run_mypy.py
```

Use the existing `scripts/ci_timing.py run --lane <name> --output <external-file> -- <command>` wrapper to retain actual original-child exits and elapsed receipts. A receipt output must not dirty the source under test. These commands do not substitute for broader qualification required by the integration rail.

## Finite baseline and proposed-source profile

The exact successful script is `evidence/profile-script.py.txt`; Python can execute that filename directly. For a fresh baseline, use a clean checkout of the audited base and a pinned environment importing that checkout. Supply a new external output directory:

```bash
.venv/bin/python scripts/ci_timing.py run \
  --lane observer-compilation --output /absolute/external/baseline-lane.json -- \
  .venv/bin/python /absolute/reference/evidence/profile-script.py.txt \
  --checkout /absolute/baseline-checkout \
  --output /absolute/external/baseline-input \
  --expected-source 8c80903818b386f50ddce8cd316cb3d93ec8cc51 \
  --uv /absolute/pinned/uv
```

The script checks exact clean source and import ownership, captures at most three small actual caller turns (the recorded baseline needed one), closes/removes its fixture, retains canonical `projection.json` and `sample.json`, and records four fixed phases. It instruments counts separately from uninstrumented timing and does not set a timing target.

For a matched comparison, execute the same script on the exact clean candidate source using `--reference-input /absolute/external/baseline-input`. The script requires identical source projection data and uses the exact retained actual sample. Set `--expected-source` to the candidate SHA, use a new output path, and make sure imports resolve into that candidate checkout.

The three serial validations and the three-entry batch both repeat the same observed sample. Proposal replay does not execute a fresh fixture. Compare the matched phase rows only; baseline and replay lane totals include different setup work. One observation per phase demonstrates measured behavior here, not a statistical speedup claim or full-census attribution.

If using the archived sample without rerunning the baseline fixture, regenerate `projection.json` from the exact caller operation, require the recorded canonical SHA-256, and copy the archived `sample.json` to a new external directory. The reader must independently validate that sample before replay. Do not construct metrics from declared bounds or treat a hash match as numerical validation.

The retained finite regeneration script performs those checks, including standalone sample validation and exact clean source identity. Run it from the repository root, passing the receiving SHA explicitly:

```bash
.venv/bin/python -B .reference/evidence/regenerate-input.py.txt \
  /absolute/external/replay-input <exact-clean-candidate-sha>
```

It writes the three inputs required by replay: `projection.json`, the archived `sample.json`, and the original baseline `run.json`. Then pass that directory to the exact profile script's `--reference-input` option.

## Receipt interpretation and cleanup

`run.json` binds the profile to exact clean source at start and end, exact script/projection/sample digests, tool versions, imports, workload counts, per-phase measurements, exit status, and cleanup. The lane receipt reports the original child status and total elapsed time. Preserve failed receipts separately.

The recorded successful baseline created and removed its private fixture, closed the generator, and checked zero catalog connections. Replay creates no product fixture; its `fixture_created=false` / `fixture_removed=true` pair comes from the script's no-fixture replay marker and must not be reported as creation or removal of a fresh product fixture. Remove only owned external test directories after the original child has exited. The batch tests assert release of retained projection/source/sample references on successful and failing exits.
