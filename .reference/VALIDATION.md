# Source audit and handoff validation

## Exact source relationship

- Audited integration base: `f0a7c2e263bb96533d71c9a3b4d43544b77d7b85`.
- Base tree: `207b19ea5b7cffeeb08b29550578a8d2affdbf7c`.
- Observed integration ref: `codex/964-session-owner`, draft [#979](https://github.com/nashspence/riverhog/pull/979).
- Owning issue: [#981](https://github.com/nashspence/riverhog/issues/981).
- Additional inspected source: `main@209481c189f7387557a85e1c249184642816e103`.
- Reference changes: four new `.reference/` Markdown files only; all product paths
  remain byte-identical to the audited integration base. No implementation patch,
  fixture, generated contract or new executable qualification is supplied.

The shared producer/canonical production/client/protocol files, CLI, derived
writer/runtime and target-support paths used in the audit are unchanged between
the two inspected commits. FTP `landing.py` differs in status-lock/query behavior;
its publication/observation/cleanup paths are unchanged. The service differs
materially on the integration branch, so the service audit uses the exact base.
The integration branch's `AGENTS.md` also explicitly describes checkpoint pushes
and exact-source/tested-merge qualification for PR integration.

## Executable source anchors

These paths are in the unchanged base tree; line numbers are navigation, not a
separate behavioral authority.

| Path / symbol | Audited finding |
| --- | --- |
| `packages/riverhog-client/src/riverhog_client/producer.py`: `_append_sources` | No binding leads to observation/authoring; existing binding checks only artifact ID. |
| `packages/riverhog-client/src/riverhog_client/canonical_production.py`: `build_member_journal`, `bind_produced_member` | New Native IDs; complete `bytes` result; stage then bind then decision, without durable preparation. |
| `packages/riverhog-client/src/riverhog_client/client.py`: `upload_collection_upload_session_provenance_journal` | Resumes from accepted byte count; streams input including skipped prefix; fixed transport chunks. |
| `packages/riverhog-client/src/riverhog_client/producer.py`: `_accept_registered_rows`, `resume_artifact_custody` | Member/completion checks, no comparison with retained original primary; existing no-payload-reread path. |
| `packages/riverhog-protocol/src/riverhog_protocol/collection_upload_transport.py`: `CollectionUploadArtifactCustodyReceiptDocument`, `validate_collection_upload_artifact_custody_receipt` | Receipt self-commitment and member identity checks; exact field is `primary`. |
| `riverhog/src/riverhog_core/services/collection_uploads.py`: `create_provenance_journal`, `append_provenance_journal`, `seal_provenance_journal`, `bind_artifact_provenance` | Existing immutable primary journal/chunk/binding replay semantics. |
| Same service: `_advance_artifact_custody`, `_validate_staged_canonical_journal_set`, `_upload_payload` | Required closure/primary custody; strict unrelated-journal rejection; status lacks a complete pristine-journal witness. |
| Same service: `set_artifact_materialization_decisions`, `_finalize`, `_advance_catalog_artifacts`, `_publish_catalog_collection`, `get_artifact` | Custody does not imply decision acceptance; every receipt exists before catalog projection but upload deletion removes readback. These motivate the two additional lifecycle rules. |
| `riverhog/src/riverhog_api/routers/collections.py`; `riverhog/src/riverhog_api/schemas/collections.py`; upload service `require_read_access` | Existing exact-artifact response fits a retained receipt; existing read authority already recognizes the published creator and scoped deletion reader. |
| `riverhog/src/riverhog_core/services/canonical_provenance.py`: `get_history_binding_proof`, `_cached_reader`; `provenance_archive_read.py:binding_inclusion` | Per-member proof retrieval rebuilds whole-collection binding work and needs additional provenance permissions. It is deliberately not chosen as automatic cleanup. |
| `packages/riverhog-provenance/src/riverhog_provenance/model.py`: `ObservationResult` | Original graph bytes and selected observation identities. |
| `packages/riverhog-provenance-contracts/src/riverhog_provenance_contracts/schemas.py`: `ContractCatalog.checkpoint_sha256` | Catalog pin, distinct from archive authority. |
| `some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/landing.py` | Durable observation under claim; shared producer invocation; receipt-led claim cleanup. |
| `some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/directory_upload.py`; `main.py:upload_cmd` | Durable opaque allocation; native observer reruns before append today. |
| `packages/riverhog-client/src/riverhog_client/processing/writer.py`; `processing/runtime.py` | Derived shared producer, causal/history/completion ownership and claim/fence source context. |
| `some-implementations/stove0/packages/target-support/src/stove0_target_support/`: `execution.py`, `runtime.py`, `output_checkpoint.py`, `persistent.py` | Durable execution root; restored output omits observation; existing checkpoint/retirement owners. |
| Same package: `completion_checkpoint.py:_immutable_file` | Stream/hash, fsync, no-clobber publication and parent fsync example; implementation stays with its owner. |
| `some-implementations/stove0/packages/extension-support/src/stove0_extension_support/__init__.py`: `ExclusiveStateOwner` | Process-lifetime nonblocking ownership, distinct from expiring timestamp claims. |
| `packages/state-schema/src/state_schema/__init__.py`: `StateSchema` | Explicit database initialization/upgrade; full verification cannot be treated as a constant-cost member lookup. |

## Evidence classification

The issue reports a real-service/Native diagnostic with two sealed journals and
one selected primary after interruption/restart (23.31 s to the intended assertion;
24.52 s command elapsed). Those are **issue-reported observations**, not a run
performed for this reference. Its causal relationship to the earlier FTP CI
failure remains unproven. This handoff preserves that distinction.

For this handoff, source and contract inspection was performed against the pinned
base, with parallel audits of producer ownership, application placement and
crash/growth boundaries. Local artifact checks and their actual results are
recorded below. These are handoff integrity checks, not
production correctness or measured growth qualification.

The review corrected two material lifecycle omissions before publication:
materialization acceptance must survive independently of early custody, and
original receipts must survive finalization for bounded creator-authorized
readback. It also made remote-absence checks, orphan-candidate handling, compact
record precedence and CLI's retained-allocation lifetime explicit.

## Validation performed

- Confirmed exact base commit/tree and current remote integration ref. Re-read
  #981 before publication: scope unchanged and no intervening comments.
- Parsed the 16 referenced Python source files and resolved 42 named source
  symbols. Verified all five acceptance test-file/directory locations.
- Checked all four reference files, six relative Markdown links, fence balance
  and final newlines. The combined source/artifact checker completed in 0.273 s
  using the scratch Python runtime; it imported no product modules.
- `git diff --cached --check` passed. Reviewed the staged file set against the
  exact base: only the four `.reference/` documents, with no product-path edits.
- Independent review covered exact replay/input authority, all three application
  placements, receipt/decision/finalization ordering, missing state, concurrency,
  growth and reclamation. Findings were incorporated before publication.
- `make lint` was attempted and stopped at the toolchain bootstrap: `mise` was
  unavailable (make exit 2, recipe exit 127). No lint tool or product test ran.
  The environment block is not a lint failure in the proposed design or a pass.
- Inspected workflow triggers: ordinary CI/CodeQL push qualification targets
  `main` and `release/v1`; this reference is not a PR. Post-publication tree,
  parent/ref identity and actual check status are recorded in the owning issue.

## Not validated / limitations

No repair was implemented, and no new runtime performance claim is made. The
production interruption matrix, process-kill/fsync/platform proofs, larger growth
workloads, built application lifecycle and exact-source integration qualification
remain implementation acceptance work under [ACCEPTANCE.md](ACCEPTANCE.md).

The available scratch runtime is Python 3.12.14 / uv 0.13.4; the audited repository
pins Python 3.12.3 / uv 0.11.24 and requires `mise`. No `mise` or Docker executable
was available during environment inspection. Unlocked installed dependencies
were not substituted for product test evidence.

Reference publication is not a fully green implementation handoff. The repository
workflow triggers and exact published reference commit are checked separately;
no absent, skipped, blocked, pending or ancestor check is counted as a pass.
