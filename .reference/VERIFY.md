# Reproduce this finite reference

Start with [HANDOFF.md](HANDOFF.md). Use a separate reference checkout at the exact
SHA published in #964. Do not run this in the integration agent's active worktree.
The source baseline is `f35123c4dcb79de171627257c72633b9d72378f0`; product paths here
contain proposed edits relative to that exact commit.

## First useful check

The focused follow-up starts with real process-death and exact original receiving:

```sh
mise x -- uv run --locked --all-packages --group dev python -m pytest -q \
  tests/unit/test_primary_facts_lifecycle.py \
  tests/unit/test_primary_facts_lifecycle_work.py
```

Use the repository-pinned toolchain (`mise.toml`, `mise.lock`, `uv.lock`):

```sh
mise x -- uv sync --locked --all-packages --group dev
mise x -- uv run --locked --all-packages --group dev python -m pytest -q \
  tests/unit/test_shared_primary_admission.py::test_real_member_turns_share_primary_admission_with_independent_record_extent
```

The fixture creates four real distinct artifact/association bindings. Shared
fanout varies independently from producer source-context bytes and unrelated
Occurrence/observation text. It drives actual catalog owners, separates bounded
preparation/member work from terminal validation, verifies every member and
retains original receiving through cleanup. The fixture is small and uses
repository injection; it is not HTTP publication, a provider trial or throughput
qualification.

## Focused suite

```sh
mise x -- uv run --locked --all-packages --group dev python -m pytest -q -n 4 --dist=load \
  packages/riverhog-provenance/tests/test_corpus_owned_placement.py \
  packages/riverhog-provenance/tests/test_corpus_primary_selection.py \
  packages/riverhog-provenance/tests/test_corpus_reclamation.py \
  packages/workspace-reclamation/tests/test_reclamation.py \
  tests/unit/test_primary_production_work.py \
  tests/unit/test_shared_primary_admission.py \
  tests/unit/test_primary_facts_capacity_retirement.py \
  tests/unit/test_primary_facts_lifecycle.py \
  tests/unit/test_primary_facts_lifecycle_work.py \
  tests/unit/test_provenance_binding.py \
  tests/unit/test_archive_catalog_finalizer.py \
  tests/unit/test_archive_upload_terminal_retirement.py \
  tests/unit/test_selected_upload_journal_prefix.py \
  tests/unit/test_imported_history_finalization.py
```

The new Native tests compare selected historical facts against fresh raw-prefix
oracles, including retirement gaps and replacement; exercise detached pages,
sparse negatives, narrow documentary projections and the existing SHA/framing
domain proof; and provide actual declared selector/delegate samples.

The producer tests compare streaming checks with the existing validator over
fragmented records and corruption. They record source-owned bounded atom samples.
The actual-caller tests cover shared/sparse primaries, selected corrections,
per-member identity/fixity/context/hint errors, source/job changes, producer and
completion rejection, raw tampering at the terminal barrier, lost acknowledgements,
workspace contention, uncertain original receiving, restart and retirement.
The capacity regression drives two real uploads through the physical worker and
finalizer registry with a single retained construction and with saturated SQLite
grants, proving a received no-effect refusal does not strand a prepared owner.
The focused lifecycle suite sends real SIGKILL at controlled creation and
reclamation boundaries and restarts from the committed catalog. It also checks
current identity, exclusive live-owner protection, unknown entries, actual
close receiving and bounded peer progress. The work witness varies retained
catalog population and file extents independently and records source-owned
observations; no directory enumeration or source replay supplies cleanup.
The existing selected-prefix and imported-history witnesses remain in the
focused regression suite; they drain real preparation turns before asserting
member cursor progress or injecting a member-commit acknowledgement failure.

## Maintained growth collection

Run the witness-bearing files on a **clean committed tree**, with the existing
timing plugin. Keep output and temporary directories outside the source tree.

```sh
RIVERHOG_CI_RUN_ID=primary-member-reference \
RIVERHOG_CI_TIMING_DIR=/absolute/path/to/evidence \
TMPDIR=/absolute/path/to/scratch \
mise x -- uv run --locked --all-packages --group dev python -m pytest -q \
  -n 4 --dist=load -p scripts.ci_timing \
  --basetemp=/absolute/path/to/pytest-scratch \
  --junitxml=/absolute/path/to/evidence/results.xml -o junit_family=xunit1 \
  packages/riverhog-provenance/tests/test_corpus_primary_selection.py \
  packages/riverhog-provenance/tests/test_corpus_reclamation.py \
  packages/workspace-reclamation/tests/test_reclamation.py \
  tests/unit/test_primary_production_work.py \
  tests/unit/test_shared_primary_admission.py \
  tests/unit/test_primary_facts_lifecycle_work.py
```

Collect and save the actual declaration evidence with the small reference wrapper:

```sh
mise x -- uv run --locked --all-packages --group dev python .reference/collect.py \
  --evidence /absolute/path/to/evidence \
  --run-id primary-member-reference \
  --output /absolute/path/to/evidence/growth-verified.json
```

The wrapper discovers only the witnesses owned by the six files above and calls
`collect_test_witnesses`. The maintained collector validates source cleanliness at
both ends, complete test outcomes, owner digests, declarations, operation
projections and actual samples. It rejects stale, incomplete or partial evidence.
The source-owned positive and negative checks remain the evidence owners; this
wrapper adds no gate, registry or normative growth claim.
Require the pytest command itself to exit zero as well: witness collection does
not substitute for the outcome of other tests in the focused suite. Preserve the
raw worker JSON alongside the collected bundle; it contains the independent
`growth_observations` phase totals as well as declared samples. Use a fresh run ID
and evidence directory for a rerun so the collector cannot mix two executions.

For an integration checkpoint, continue to use the normal repository `make lint`
and focused-owner rail, then exact-source required CI. This reference's local
results do not replace those gates. Correctness, growth, evidence integrity and
elapsed time are separate results. No new numerical timing target is proposed.
