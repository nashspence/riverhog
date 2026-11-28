# Checks performed and qualification remaining

All successful product checks below ran against the proposal whose four exact output hashes are in [SOURCES.json](SOURCES.json), based on main `be483fd6d567616d5fcd355202d61f1e682efe75`. Product edits are in the supplied patch, not applied to this reference branch's product tree. Do not interpret a green reference commit as qualification of those proposed edits.

## Completed checks

| Check | Actual result | Scope |
| --- | --- | --- |
| New `test_prepared_provenance.py` | 33 passed, 1.58 s | Root/sequence admission, independent axes, selected access, actual staging, corruption, interruption, partial streams and resource receiving/retry. No age availability skip. |
| Existing independent `test_recovery.py` | 18 passed, 148.35 s | Real official age/batchpass encrypted full-recovery compatibility, supplementary/inherited history, missing selected structures, head mutation, trusted pins, no-clobber, failure/retry. |
| Existing reader/publication plus three ownership tests | 15 passed, 4.18 s | Core reader precedent, independent publication behavior and selected source ownership/dependency-direction rules. |
| Focused Ruff lint and format check | Passed | Recovery source package plus new maintained test module. |
| Focused mypy | Passed | Recovery source package. |
| `git diff --check` | Passed | Proposed patch whitespace. |
| Patch check and actual application on inspected #1008 | Passed | Isolated checkout of `382870b8ce3072c962f57a72cd9842984474002c`; all four applied outputs match the tested hashes. |
| Finite old/new reader and actual-stage probe | 78 observations completed | Final candidate, fixture and probe hashes; source unchanged throughout; exact bytes/receiving/count assertions passed. No elapsed time measured. |

Environment: Linux, Python 3.12.3; dependencies resolved with the existing `uv.lock` via pinned uv 0.11.24 and `uv sync --locked --all-packages --group dev --python 3.12.3`. Ruff 0.16.0 and mypy 2.3.0 came from that locked environment. The lockfile and project dependency declarations were not edited.

Official age 1.3.1 and `age-plugin-batchpass` were supplied by the repository-pinned Linux archive. Its SHA256 matched `mise.lock`: `bdc69c09cbdd6cf8b1f333d372a1f58247b3a33146406333e30c0f26e8f51377`. The existing encrypted fixture uses `log_n=1`; these are correctness/compatibility checks, not production KDF-18 timings.

## Reusable focused commands

Run from the integration checkout with the repository-pinned interpreter and official age/batchpass available on PATH:

```bash
python -m pytest -q some-implementations/riverhog/recovery/tests/test_prepared_provenance.py
python -m pytest -q some-implementations/riverhog/recovery/tests/test_recovery.py

python -m pytest -q \
  tests/unit/test_provenance_archive_read.py \
  some-implementations/riverhog/recovery/tests/test_publication.py \
  tests/unit/test_workspace_boundaries.py::test_implementation_projects_do_not_cross_owner_boundaries \
  tests/unit/test_workspace_boundaries.py::test_every_implementation_project_and_module_has_exactly_one_owner \
  tests/unit/test_workspace_boundaries.py::test_shared_packages_do_not_import_implementation_projects

python -m ruff check \
  some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool \
  some-implementations/riverhog/recovery/tests/test_prepared_provenance.py
python -m ruff format --check \
  some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool \
  some-implementations/riverhog/recovery/tests/test_prepared_provenance.py
python -m mypy some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool
git diff --check
```

These commands identify the focused checks; they do not replace normal current-source PR qualification. The full probe reproduction is in [EVIDENCE.md](EVIDENCE.md).

## Interrupted and unperformed work

A broader exploratory pytest command containing the entire workspace-boundary module was interrupted with Ctrl-C, exit 130, during the existing generated-contract/documentation dependency fixture used by `test_projects_declare_their_exact_direct_runtime_dependencies`. It produced no complete suite result. The three relevant source ownership checks listed above subsequently completed separately. No whole-module pass is claimed.

Not run for this proposed source: full `make lint`, full repository unit/build/distribution qualification, installed/platform suites, PR merge-checkout CI, or a matched production-condition encrypted wall-clock comparison. The new tests are maintained primitive/count/resource regressions; they do not constitute the repository's complete declared-growth qualification. No new operation registration is introduced for this private helper.

The actual staging probe covers complete semantic journal/corpus staging plus binding iteration. Selected structures are exercised through the actual reader and existing full-recovery tests. Complete mapping/history growth, whole-recovery peak memory/descriptor limits, hostile operating-system resource exhaustion, and all combined cleanup failures remain outside this finite qualification. The original-owner retry tests cover the specifically injected index cursor/connection failures; they are not a universal whole-recovery cleanup proof.

Integration should reconcile any source changes, run normal required checks on the actual integrated source, then repeat one matched real recovery diagnostic using the prior #991 conditions. Preserve the existing metadata-head fences and separate operation success, source receiving, publication, and elapsed-time evidence. Continue the separate publication/finalization accounting even if this recovery change succeeds.

The [Proffer record](proffer.json) is validated against the inspected Proffer schema. That validates record structure, not the truth of its claims or authority to integrate. Publication identity and inventory verification are recorded separately in the owning GitHub issue after the commit exists.
