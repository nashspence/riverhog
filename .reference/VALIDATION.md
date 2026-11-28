# Validation and reproduction

## What ran

**270 distinct maintained cases passed across the recorded sources:**

| Check | Cases | Source and scope |
| --- | ---: | --- |
| Archive Store and member-history contracts | 70 | The final Store owner is unchanged from this run. Fresh independent calls, complete-set admission and original source receiving are covered. |
| Existing neutral, client, independent recovery and Core callers | 173 | Closure `e76c10642fdd8e371d83bae7a994026be7f18c9988065892c632b08a98151f00`; 739.84s. All 18 encrypted recovery cases and actual Core imported-history receiving/growth/cache-pressure witnesses passed. |
| Final closure selection/export and two Core live fences | 27 | Exact final source, 90.34s. All 25 new closure cases, key revocation after I/O and stream revocation after cached membership passed. |

The broader 173-case run preceded the final export guard. Its **only subsequent executable change** is `self._require_open()` after a fresh bounded structure read; a separate docstring clarification describes detached membership/reader custody. The validation builder reconstructs the earlier file and checks its SHA-256. The broader run is not represented as testing final bytes. The final source's 40-observation comparison and four-observation source-change probe also passed with exact source hashes.

An earlier closed-owner witness failed on the pre-guard implementation because it yielded a buffered object after the callback closed its owner. The final maintained case passes with a real file source and confirmed original close. Earlier preliminary/failed/interrupted development observations are retained separately and excluded from the totals.

One generic unchanged `test_journals_larger_than_history_validation_cache_remain_accepted` case was deliberately deselected from the broader run. Actual imported-history cache-pressure witnesses, including the greater-than-4-MiB source case, were retained and passed. This finite selection is not the complete unit suite.

Pinned Ruff check/format passed for all eight changed files, focused mypy passed for both production owners, and `git diff --check` passed. The patch was applied to an isolated checkout of the audited main, and every changed output was compared byte-for-byte. Proffer records validate against `nashspence/proffer` commit `2ce5e15c3191fcdc2be70213c0170f57c852a883`.

## Runtime and limits

The experiment/check runtime is Linux x86_64, CPython 3.12.3, SQLite 3.45.1, uv 0.11.24, unchanged `uv.lock`, Ruff 0.16.0 and mypy 2.3.0. Official age and batchpass both report 1.3.1. The downloaded Linux archive matches `mise.lock` SHA-256 `bdc69c09cbdd6cf8b1f333d372a1f58247b3a33146406333e30c0f26e8f51377`.

Encrypted fixture KDF is `log_n=1`. The neutral primitive tests use authenticated plaintext boundaries and real canonical validators, not an encrypted provider. The selected growth tests bind the existing unchanged `qualification_numeric_policy()`; no numeric resource or production allowance was edited. Validation/probe elapsed times are advisory observations with instrumentation and some concurrent checks, not matched performance results.

Full `make lint`, complete unit/build/dist suites, exact tested-merge PR CI, installed/platform/provider qualification, matched production KDF18 publication/recovery timings, canonical-pilot acceptance and approved shared GitHub Actions budget/profile evidence were not performed. They are not inferred from local passes. Integration owns the current-source checks required by its existing rail; this reference adds no gate or hold.

## Reproduce the focused patch

Use an isolated current integration checkout. Verify the source relationship first; do not merge the reference branch or apply it mechanically to a changed owner. The product tree in the reference remains the audited main. Apply its patch and its maintained tests/fixture together:

```bash
git apply --check /path/to/reference/.reference/proposed/neutral-history.patch
git apply /path/to/reference/.reference/proposed/neutral-history.patch
```

Use repository-pinned tools. In the original experiment an external virtual environment was created with:

```bash
UV_PROJECT_ENVIRONMENT=../venv uvx --from uv==0.11.24 uv sync --locked --all-packages --group dev --python 3.12.3
```

The focused final cases are:

```bash
python -m pytest -q -o junit_family=legacy \
  packages/riverhog-provenance/tests/test_history_selection_work.py \
  packages/riverhog-provenance/tests/test_history_structure_identity.py \
  'tests/unit/test_published_provenance_access.py::test_live_read_fences_prevent_disclosure_after_archive_io[key-revoked-stream]' \
  tests/unit/test_published_provenance_access.py::test_stream_continuation_rechecks_revocation_after_cached_member_closure
```

The Store contract checks are:

```bash
python -m pytest -q \
  packages/riverhog-archive-contracts/tests/test_history_heads_admission.py \
  packages/riverhog-archive-contracts/tests/test_member_history.py
```

The 173-case broader selection comprised these existing files:

```text
packages/riverhog-provenance/tests/test_history.py
packages/riverhog-provenance/tests/test_history_worklist_admission.py
packages/riverhog-provenance/tests/test_history_snapshot_admission.py
packages/riverhog-provenance/tests/test_history_membership_admission.py
packages/riverhog-provenance/tests/test_history_pending_growth.py
packages/riverhog-client/tests/test_processing_provenance.py
some-implementations/riverhog/recovery/tests/test_recovery.py
some-implementations/riverhog/recovery/tests/test_prepared_provenance.py
tests/unit/test_member_history_finalization.py
tests/unit/test_member_history_discovery.py
tests/unit/test_imported_history_receiving.py
tests/unit/test_imported_history_finalization.py
tests/unit/test_imported_history_growth.py
```

For these source-owned numeric growth checks, invoke pytest in the same process after binding the unchanged existing policy:

```python
import os
import pytest
from scripts.validation_numeric import bind_numeric_environment, qualification_numeric_policy

bind_numeric_environment(os.environ, qualification_numeric_policy())
raise SystemExit(pytest.main([
    "-q", "-o", "junit_family=legacy",
    "-k", "not test_journals_larger_than_history_validation_cache_remain_accepted",
    # Supply the existing file list above.
]))
```

## Reproduce finite experiments

Use the installed candidate environment, with its checkout root and test support importable and repository-pinned age/batchpass on PATH. Each output path must be new so earlier observations remain intact. `history_probe.py` fetches exact baseline file content from the local Git object database and loads it under separate module names; it does not mutate production source.

```bash
python /path/to/reference/.reference/experiments/history_probe.py \
  --repo /absolute/path/to/candidate \
  --output /new/path/history-counts.json \
  --mode all

python /path/to/reference/.reference/experiments/history_export_gap_probe.py \
  --repo /absolute/path/to/candidate \
  --output /new/path/history-export-gap.json
```

`--mode primitive` is available for the cheap structural slice without official decryptors. The complete report requires `--mode all`; no elapsed acceptance threshold is encoded. Preserve failed/interrupted observations and actual original receiving. The final four-observation mutation probe compares original and final owners; development evidence separately retains the rejected intermediate's six observations.

Decompress the retained raw count/property files with any gzip reader. Their exact stored identities and the proposed source/patch hashes are available in the published tree and `SOURCES.json`. After current-source integration, record a finite Proffer disposition in #991 identifying what was reused/adapted/rejected and which ordinary qualifications remain.
