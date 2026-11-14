# Validation and measured work

This receipt concerns the finite imported-primary reference requested in
[#964 comment 6863269082](https://github.com/nashspence/riverhog/issues/964#issuecomment-6863269082).
It supplies concrete input under [#903](https://github.com/nashspence/riverhog/issues/903),
not integration or whole-path G13 acceptance. Start with [HANDOFF.md](HANDOFF.md).

## Exact source and successful gates

The audited base is `1ff0a9ce70c23f18aa94d5b52ed036376132021a`. The tested producer implementation is
`63843b479986486298d47e4347226a74ee197570`, with full tree `00f68336b22d9862a42bdba39f530cd8cf2cc529`.
The packet's GitHub implementation mirror preserves that exact tree and the
audited base; its actual identity is in HANDOFF. The final reference commit adds
only `.reference/` material. Its exact published SHA is recorded in the owning
issue, with publication-tree verification.

| Gate | Result | Elapsed time | Scope |
| --- | --- | ---: | --- |
| `make lint` | Passed | 4.074 s | Complete repository lint, clean source at start and end; qualification eligible |
| Focused `make unit` | 168 passed, no failure or skip | 975.719 s | Exact retained selection, clean source at start and end; selection override, not full unit qualification |
| Pytest within that unit gate | Passed | 950.810 s | Same execution; four workers, `--dist=load` |

The make, recipe, pytest controller/worker, JUnit and original log records are
retained under [evidence/](evidence/). `results.json` preserves primitive work
observations, declaration-bound helper/delegate samples, per-case phase times,
and terminal profile times. The artifact index records byte counts and SHA-256
digests. The collector checks receipt identity, source cleanliness, phase
completion, JUnit/controller agreement, exact witness source hashes and declared
growth hashes before producing the packet.
[normative-growth-verification.json](evidence/normative-growth-verification.json)
keeps root declaration hashes distinct from reachable projection hashes and
records the maintained source-owned sample validator's results. Worker collection
inventories are reconciled with actual executed nodes and the make witness list;
the xdist controller itself has no collection inventory. No failed development
receipt is counted as passing evidence.

The run used the repository-pinned Python 3.12.3, uv 0.11.24, Ruff 0.16.0 and
SQLAlchemy 2.0.51 on Linux x86_64. SQLite reported runtime version 3.45.1.
Lint completed before unit began; no other validation run
overlapped this final unit gate. Four worker processes share the machine, so
wall times are observations under those conditions. `PYTHONUNBUFFERED=1`
affects diagnostic delivery, not the tested source or algorithm.

### Reproduction

Use the repository's pinned toolchain, a fresh timing output directory and a
temporary directory outside the source checkout. The retained command uses the
producer's concrete workspace paths; replace those output paths on another
machine. `evidence/unit-invocation.json` preserves the supplied command and
environment separately from the make receipt, whose `parameters` mapping does
not retain the override values.

```bash
make \
  unit \
  'UV_RUN=mise x python uv -- uv run --locked --all-packages --group dev' \
  'UNIT_PYTEST_ARGS=-n 4 --dist=load --instafail --durations=20 --durations-min=0.25 -p scripts.ci_timing --junitxml=/workspace/scratch/320596a7d459/imports964/qualification/unit-finalized/pytest.xml -o junit_family=xunit1' \
  'TESTS=tests/unit/test_imported_history_finalization.py tests/unit/test_imported_history_growth.py tests/unit/test_imported_history_receiving.py tests/unit/test_selected_upload_journal_prefix.py packages/riverhog-provenance/tests/test_corpus_source_endpoint.py packages/riverhog-provenance/tests/test_corpus_historical_source.py packages/riverhog-provenance/tests/test_history.py packages/riverhog-provenance/tests/test_history_snapshot_admission.py packages/riverhog-provenance/tests/test_history_worklist_admission.py tests/unit/test_upload_journal_result.py tests/unit/test_archive_catalog_finalizer.py::test_pending_original_cannot_advance_a_replacement_claim[provenance-closure] tests/unit/test_archive_catalog_finalizer.py::test_changed_pending_cursor_is_not_a_fresh_batch[provenance-closure] tests/unit/test_archive_catalog_finalizer.py::test_terminal_selection_allows_original_receiving_but_not_a_new_catalog_batch[False-provenance-closure] tests/unit/test_archive_catalog_finalizer.py::test_terminal_selection_allows_original_receiving_but_not_a_new_catalog_batch[True-provenance-closure] tests/unit/test_workspace_boundaries.py'
```

Run `make lint` with the same `UV_RUN` override. The maintained witness functions
enter the existing unit gate; there is no separate product profiling command.
The four-member fixtures are contract/caller witnesses, not throughput scenarios.

## Complete terminal witness

The measurement encloses the actual terminal `ArchiveCatalogFinalizer.advance`
turn after ordinary member turns complete. Four member histories, their real
source-binding/archive/provenance-root proof, all five staged journals, the
current writer/claim/epoch and complete retained-history resolution participate.
Within each prefix-extent pair, F=1 and F=4 reuse the exact same shared canonical
source, historical primary anchor and source proof. Documentary text varies
independently by 0 or 4,096 bytes between pairs.

The shared source has an old valid primary and a later correction that withdraws
its delivery association. Retained history selects both anchors. The new import
point proof must select the old effective graph and exact original State address;
the final graph cannot stand in for it.

| Primary prefix bytes | Full source bytes | Imports | Shared provider reads | Shared provider bytes | Import catalog SQL | Import Native metadata SQL |
| --- | --- | --- | --- | --- | --- | --- |
| 6404 | 7902 | 1 | 3 | 22208 | 3 | 10 |
| 6404 | 7902 | 4 | 3 | 22208 | 12 | 40 |
| 10500 | 11998 | 1 | 3 | 34496 | 3 | 10 |
| 10500 | 11998 | 4 | 3 | 34496 | 12 | 40 |

All four rows have three shared reads: one complete current raw pass, one old
primary snapshot and one complete retained snapshot. Snapshot traversal order
is determined by root hashes; the witness checks both exact extents without
assuming chronology. Total shared provider bytes are exactly
`2 * full_source_bytes + primary_prefix_bytes`, independent of fanout. Full raw
reads reach provider exhaustion and confirm original close. Exact-prefix reads
consume the admitted bytes and confirm original close at that extent.

Each terminal run validates all five distinct raw sources once and indexes their
actual frames once. Imports add zero source-provider reads, zero Native validation
advances and zero retained Native openings or workspace leases. The existing
distinct historical snapshot work still runs. The whole terminal attempt has
eleven Native validation calls in this fixture; import fanout does not change
that count or total Native advances.

### Actual cost, including remaining work

| Prefix bytes | Imports | Raw admission SQL | Import admission SQL | Other catalog SQL | Total catalog SQL |
| --- | --- | --- | --- | --- | --- |
| 6404 | 1 | 15 | 3 | 128 | 146 |
| 6404 | 4 | 15 | 12 | 170 | 197 |
| 10500 | 1 | 15 | 3 | 128 | 146 |
| 10500 | 4 | 15 | 12 | 170 | 197 |

The changed admission work is `3J + 3F`: two current locked source/job selections
and one postcheck for each of J raw sources, and the same three catalog selections
for each import. Other catalog work includes real proof/history/snapshot/finalizer
operations and remains visible. The complete total is not described as just
`3J + 3F`. Each import also performs one endpoint query and nine historical-delivery
queries in the Native corpus; metadata widths and index seek work are real costs.

Native metadata projections and parameters total 3,444 and 3,446 bytes per import
for the smaller and larger prefixes, respectively. Those counts exclude the
server catalog rows. The independently measured catalog rows, parameters and
VM work are reported below, so the narrow Native count is not presented as the
whole helper's metadata cost.

| Prefix bytes | Imports | Terminal wall seconds | Terminal thread CPU seconds | All Native advances | Corpus frames | Corpus bytes |
| --- | --- | --- | --- | --- | --- | --- |
| 6404 | 1 | 6.451793 | 6.365718 | 2766 | 10 | 52726 |
| 6404 | 4 | 7.454460 | 7.328059 | 2766 | 10 | 52726 |
| 10500 | 1 | 6.664549 | 6.573414 | 2766 | 10 | 56822 |
| 10500 | 4 | 6.519238 | 6.439155 | 2766 | 10 | 56822 |

These times cover the observed terminal turn, including the current catalog
transaction, raw validation, histories and corpus completion. They exclude
fixture construction and preceding member turns. `results.json` also retains
each complete test's call-phase time and pytest setup/teardown times. Fixture
construction and preceding member turns are included in the call phase; their
individual costs are not isolated. Parallel test phase times are not additive
gate elapsed time. No numerical timing threshold is a correctness
or growth gate.

The comment's baseline has a different one-snapshot source geometry and measured
the neutral closure rather than this terminal owner. Its absolute bytes, read
counts and wall times are not a matched before/after comparison. The valid result
here is the same-source F=1/F=4 work comparison. No elapsed-time speedup or large-
collection throughput ratio is claimed.

## Point-operation growth evidence

The real server helper is measured at actual catalog populations of 1 and 18
source/job rows. SQL observers include statements, rows, bytes, parameters,
SQLite VM instructions, cursor closure and mutations. Authorizers and explicit
hooks reject source payload access, unrelated projections, source-frame reads,
Native advances and retained-result openings/leases. The composed helper sample
is checked against its source-owned declaration and endpoint/delivery delegates;
current metadata prepare/postcheck samples are checked separately and reconciled.

| Measured helper metric | Catalog size 1 | Catalog size 18 |
| --- | --- | --- |
| `catalog_statements` | 3 | 3 |
| `catalog_returned_rows` | 3 | 3 |
| `catalog_metadata_octets` | 2419 | 2419 |
| `catalog_parameter_octets` | 2605 | 2605 |
| `catalog_vm_instructions` | 451 | 451 |
| `catalog_open_cursors` | 0 | 0 |
| `catalog_changed_rows` | 0 | 0 |
| `catalog_payload_reads` | 0 | 0 |
| `native_validation_advances` | 0 | 0 |

The endpoint witness also varies source count and frame extent independently
(1/4 sources and 1/17 frames) and rejects an earlier valid prefix, every changed
endpoint coordinate, an absent/incomplete index and a closed owner. It validates
the actual indexed scalar-query sample against the declared endpoint operation.
One indexed query is not a constant-time disk promise: tree depth and metadata
widths remain explicit dimensions.

## Correctness, capacity and receiving

The retained selection covers:

- Exact source proof, full input State address, artifact and bound/retained
  extents; historical delivery after a later withdrawal; every member binding
  resolved at its requested extent and all selected journals preserved.
- A valid same-length changed documentary tail beyond an unchanged primary,
  rejected by the new full sealed-anchor/exact-tail barrier before closure;
  the earlier selected-prefix byte-corruption regression remains enforced.
- Source metadata and validation-job epoch changes both before callback
  admission and after actual historical support returns, exercising the final
  currentness check while closure remains false.
- The real 32-summary eviction boundary and a primary exceeding the real 4 MiB
  cache budget. These are neutral closure-plus-corpus compositions with genuine
  proofs and histories; they are not complete server staging or large publication
  campaigns. Imports do not regain prefix replay behind either cache regime.
- Commit acknowledgement loss at the imported terminal turn. A committed result
  is received through the original operation without replay; a rolled-back
  attempt rebuilds current raw work. Original closed connections, removed
  directories, summaries and finalizer pending resources are checked.
- Existing generic fresh-provider/changed-byte, snapshot/worklist uncertainty,
  selected-prefix reader/lease receiving, changed claim/cursor and terminal
  ownership regressions. The new callback itself creates no retained result or
  lease. Neutral package boundaries remain enforced.

| Test owner | Passed cases |
| --- | --- |
| `packages/riverhog-provenance/tests/test_corpus_historical_source.py` | 28 |
| `packages/riverhog-provenance/tests/test_corpus_source_endpoint.py` | 3 |
| `packages/riverhog-provenance/tests/test_history.py` | 14 |
| `packages/riverhog-provenance/tests/test_history_snapshot_admission.py` | 19 |
| `packages/riverhog-provenance/tests/test_history_worklist_admission.py` | 33 |
| `tests/unit/test_archive_catalog_finalizer.py` | 4 |
| `tests/unit/test_imported_history_finalization.py` | 8 |
| `tests/unit/test_imported_history_growth.py` | 1 |
| `tests/unit/test_imported_history_receiving.py` | 2 |
| `tests/unit/test_selected_upload_journal_prefix.py` | 15 |
| `tests/unit/test_upload_journal_result.py` | 17 |
| `tests/unit/test_workspace_boundaries.py` | 24 |

## Development failures and source reconciliation

Development attempts are not final qualification. Corrections included an
omitted typing cast, JUnit `xunit1` selection for property receipts, a fixture
expectation that incorrectly assumed historical hash order, an observer that
confused exact-prefix close with full-source EOF, and a stale-metadata injector
that needed to increment the decoded integer instead of using SQL string
concatenation. These were corrected before the successful source was frozen.

The later `04e9eedbeffef691c617ee79198db4b15caeb1eb` run completed with 167 passes
and one failed assertion in 1,234.91 pytest seconds (1,259.010 make seconds).
Its existing corruption test expected a historical-prefix mismatch; the earlier
raw pass correctly rejected the broken predecessor chain first. The final source
changes that expectation and explains the order. Product code is unchanged by
that correction. The final successful run qualifies the corrected source; it was
not repeated merely to obtain a more favorable timing result.

PR #979 was separately inspected at
`fecde31e05fb166636cae06772bd519a9d6bb457`. Its delta from the requested base is
only `tests/unit/test_incremental_collection_producer.py`, a retention-fixture
correction. Read-only `git merge-tree` reconciliation succeeds with tree
`bceb25315f74e475e38552f46da9648007754d27`. Neither that merge tree nor a newer
integration head was runtime-tested by this packet. Reconcile again on the
agent's actual integration source and owning-issue decisions.

## Material limits

The terminal raw/corpus/history pass remains synchronous within the existing
catalog transaction. It rebuilds its ephemeral index on rollback or process
restart; this reference establishes no durable bounded/resumable terminal
checkpoint. Each imported-primary point operation is declared and measured, but
whole-path G13 acceptance still belongs to the owning issue. Early raw validation
also incurs work for a staged source later rejected as unrelated and can report
its corruption before the later membership error.

Current-byte authority comes from fresh raw validation within server custody and
the admitted source/job locks. Metadata checks alone do not prove byte contents.
The completed job record is checked without opening old retained workspace
files; those files supply no semantic authority here. Independent readers retain
their fresh provider checks. PostgreSQL row-lock behavior is source-reviewed but
not execution-tested by this SQLite packet.

No full canonical unit, Linux qualification, image/build/dist-smoke, client
platform matrix, live deployment, complete publication/recovery workflow or
1,000/10,000/100,000 campaign was run for this reference. The selection-overridden
unit receipt explicitly has `qualification_eligible: false`. #991 observer
closure reuse and the remaining ordinary primary/proof/metadata/global-corpus
costs remain separate. The reference has no PR; the workflow's push trigger
covers main and release/v1. Exact-tip workflow/check observations and post-
publication lint are recorded in the owning-issue handoff, not represented as
passing integration CI.
