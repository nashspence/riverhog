# #873 qualification claim-to-assertion map

Audited source: **`18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8`**. Source links below are immutable. This document describes existing behavior and potential narrow reuse; it does not change the producer or certify a release. Machine-readable source identities and CI limitations are in [evidence.json](evidence.json).

## 1. Retained aggregate claims: no promotion

| Producer key under `qualification` | Current producer disposition | Successful assertions available | Missing proof / integration disposition |
| --- | --- | --- | --- |
| `positive_local_lifecycles` | `not_established` | Riverhog upload finalization and retrieval-byte checks; fixture-scoped Stove0 transitions; FTP API fixture responses | No attributable complete lifecycle for every required operation. Keep the aggregate unestablished; do not treat a successful response as a lifecycle. |
| `cli_human_json_projection` | `not_established` | Paired successful collection list/show commands with identical fake-client inputs and selected human fields | No complete required-command-to-successful-equivalent-output map. Keep unestablished; neither AST reachability nor callback entry counts as execution. |
| `bounded_state_access` | `not_established` | Riverhog collection-list SELECT-count/materialization comparison | One service fixture does not measure bounds for all operations, applications, sizes, database plans, or scheduler work. Keep unestablished. |
| `event_cursor_restart_resume` | `not_established` | Three separately wired, fresh-process SQLite/ASGI event-feed witnesses | Built images, PostgreSQL, crashes, producer mutation lifecycles, and consumer checkpoint persistence are not established. Retain only the existing narrow subclaim, subject to its witness gate. |

The [producer][producer] explicitly maintains these four dispositions. The [release consumer][workflow] requires all four aggregate statuses to be `passed`, not merely the local restart subclaim. Do not bypass the consumer or set placeholder statuses to make it green. Its existing regression deliberately accepts counterfactual statuses only to test wiring; that is not behavioral evidence.

## 2. Existing retained narrow claim: local event-cursor process restart

**Key:** `event_cursor_restart_resume.local_api_process_restart`.

**Pass rule:** a nonempty discovered feed set must equal the unique set of valid successful witness identities. Each observation must identify its exact application, operation, and expected parametrized test node. Missing, duplicate, stale, or unrelated witnesses cannot qualify the whole scope. A failed pytest session or mismatched/dirty source prevents accepted producer evidence even when a call-phase witness was recorded.

| Application and operation | Public route | Exact successful assertion node | Concrete fixture wiring |
| --- | --- | --- | --- |
| `riverhog:list_lifecycle_events` | `GET /v1/events` | `tests/unit/test_event_cursor_restart.py::test_event_cursor_continues_across_process_restart[riverhog]` | Riverhog `ApiClient` through real ASGI routes; file-backed catalog; owner lifecycle service seeds finalized-event fixtures; fixture credentials persist. |
| `stove0:list_events` | `GET /v1/events` | `tests/unit/test_event_cursor_restart.py::test_event_cursor_continues_across_process_restart[stove0]` | `Stove0ApiClient` through real ASGI routes; persistent SQLite state from `_composition`; work service creates/resumes fixture work to emit events. |
| `a-riverhog-ftp-spool:list_ftp_spool_events` | `GET /v1/sources/{source_id}/events` | `tests/unit/test_event_cursor_restart.py::test_event_cursor_continues_across_process_restart[a-riverhog-ftp-spool]` | `RiverhogFtpSpoolClient` through real ASGI routes; actual `FtpSpool` event owner, with a stub upstream Riverhog API; `_record_claim_event` seeds claim fixtures directly. |

The [assertion function][restart] and [per-application harness][harness] must be read together. Sharing the assertion function does not remove the need for all three parameter cases or establish identical implementations.

Every retained passed row requires: prepare and resume subprocesses exit zero; the same operation identity is read; the initial three events have expected subjects and unique identities; the first one-event page has more data; the two unread events equal their original complete documents in order; each page advances the cursor with correct `has_more`; exhaustion preserves the full-history cursor; an event appended after restart has a new identity and expected subject without replay; the final empty page preserves the appended cursor and reports no more events. The property is recorded after these assertions. The observer exports only passed, non-xfailed call reports.

**Exact-SHA execution support:** CI run `36223966749`, attempt 1, unit job `108354319005`, checks out the audited SHA. The full unit log names `test_release_disposable_selection_satisfies_current_observation_requirements` in its duration output and concludes 2566 passed / 1 unrelated skip. That regression executes the workflow's selected tests and requires the exact three feed identities, row passes, and SHA-pinned assertion links. However, no operation report or individual parametrized result artifact was retained in the retrieved CI listing. This supports the regression/source chain; it is not a new report-level qualification attestation. See E1 in [evidence.json](evidence.json).

## 3. Reusable narrow assertions, not new producer claims

The rows below are integration candidates backed by existing assertions, not new `passed` fields in the qualification payload. Their execution support is the same audited CI unit-suite/selection chain, with the per-node artifact limitation above. Each needs an exact-SHA retained run before a future producer exports it as a qualification claim.

### L1: Riverhog disposable upload, catalog, and retrieval

Node: `tests/unit/test_operation_lifecycle_api.py::test_riverhog_official_client_positive_disposable_lifecycle` ([fixture/composition][riverhog-fixture], [upload/catalog assertions][riverhog-upload], [retrieval assertions][riverhog-retrieval]).

Scope includes `create_or_resume_collection_upload_session`, upload staging/work/finalization, `get_collection_upload_session`, `get_collection`, description/tag reads and mutations, and retrieval download/acknowledgment. The fixture uses real owner services and official-client ASGI transport, **SQLite and in-memory archive stores**, and directly drives background owner work.

Retain only assertions actually present: final upload state is `finalized`; returned collection identity and description match; description identity changes are read back; tags match the staged set; replay after a tag edit returns the same finalized upload; the official retrieval download yields exactly the original source bytes; acknowledgment returns `completed`. The test also checks range bytes and rejects stale/unsatisfiable requests. Its archive-copy portion requests then cancels a job: it is not proof of successful copy completion, real-provider transfer, or independent recovery.

Do not assign the entire observer operation set this lifecycle claim just because `observer.require` sees successful responses. Tie each operation to the state/content postcondition it actually helps establish. Client calls without such a postcondition remain response-level observations.

### L2: Stove0 fixture transition and retry transport

Node: `some-implementations/stove0/application/tests/test_stove0_api_parity.py::test_stove0_official_client_positive_disposable_lifecycle` ([assertions][stove0-assertions], [fixture implementations][stove0-fixture]).

Concrete assertions include `create_work` retried with the same preview returning the same work, `get_work` preserving identity, `step_work` returning `claimed`, `retry_work` returning `eligible`, and `cancel_work` returning `canceled`. These cross the real API and official client but use `_LifecycleState`, `_LifecycleCoordinator`, and other fixture collaborators. They establish those projections and fixture transitions, not actual target execution, durable production coordination, or complete end-to-end work settlement. The target callback fixture also must not be promoted to real target production.

### L3: FTP successful management rendering, not custody completion

Node: `some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_api_parity.py::test_official_client_positive_disposable_lifecycle` ([source][ftp]).

Scope: health, status, `list_ftp_spool_events`, `run_ftp_spool_pass`, `flush_ftp_spool_source`. The official client and API are real; `_Adapter` and the upstream API are stubs. Assertions check health/status formats, an empty event list, pass format, and the selected source. No file custody, durable archive, completed transfer, or restart assertion is established by this node. Separate custody tests are selected by the workflow, but mere selection does not supply an audited claim-to-postcondition map; do not silently credit them to this node.

### C1/C2: successful paired Riverhog CLI fixture projections

Nodes in [test_cli_json_output.py][cli]:

- `test_collection_list_json_emits_the_api_response_without_a_second_model`: `collection list` / `--json`, scope `list_collections`; schema-valid fixture, both exits zero, identical client arguments, JSON exactly equals the fixture, human description/encryption fields are present.
- `test_collection_show_human_and_json_use_one_identical_api_response`: `collection show 42` / `--json`, scope `get_collection`; both exits zero, client calls `[42, 42]`, JSON exactly equals the fixture, human storage/description/encryption fields are present.

Both use fake clients. Their honest claim is preservation of that JSON fixture and the named human projections, not every field's human/JSON equivalence or a live server-to-CLI lifecycle. The description-mutation test uses different human and machine inputs (`--description` versus `--clear`); its name does not turn those into equal-result invocations.

FTP's `test_operator_cli_has_human_and_json_views_for_each_management_operation` checks zero exits, nonempty human output, JSON format, and empty event output against a fake client. That is useful successful rendering coverage, **not semantic output equivalence**. Any future CLI claim needs both invocations to succeed, their common input/result relationship, and explicit expected semantics; failing later in a callback must never earn a pass.

### B1: collection-list query/materialization bound in one fixture

Node: `tests/unit/test_collection_reads.py::test_collection_list_query_count_is_independent_of_page_rows` ([source][reads]).

Scope: `SqlAlchemyCollectionService.list`; related public operation `riverhog:list_collections`, but this test itself calls the service, not the API. Twelve SQLite collections each have one archive copy and nine archive-object records. Compare page sizes 1 and 12: the 12-row page has 12 collections, every copy count is 1, no `CollectionArchiveObjectRecord` is materialized, and SELECT counts are equal.

Do not describe this as a fixed SQL-count budget (no exact count is asserted), asymptotic database-plan proof, bounded rows scanned, or bounds for all application state. Before sharing B1 with another route, demonstrate that route's actual service wiring and equivalent query path; no blanket application-level credit.

## 4. Supporting fields and accounting

`contract_inputs.status = validated` describes checked input identity and extent accounting, not runtime behavior. `provider_backed_lifecycles.status = linked` and provider issue/workflow references are links, not successful provider evidence. Cold CLI startup measures successful, stable `--help` executions, not management-command parity. Server/client wall samples describe response observations, not lifecycle completion or database work.

At this SHA the observed response count comes from `len(local_api.operations)`; locally required scope counts matrix rows without provider evidence. Some provider-linked operations may have disposable observations, so those populations need not be equal. CLI operation counts and the bounded-state application list denote required scope, not successful assertion counts. No current operation totals are invented here; old 140/117 and 154/156 test counts remain historical issue attestations.

## 5. Minimal integration sequence

Keep all aggregate statuses and the #866-owned consumer identities unchanged. First retain a fresh clean exact-SHA run, timings, report, and named result records. Then choose one bounded claim whose successful assertions and concrete operation wiring are complete; reuse the existing tests and attribution pattern rather than adding a general framework. Ensure fail/skip/xfail/xpass, teardown failure, missing/new feed, and dirty/mismatched-source counterexamples remain rejected. Publish observed versus required counts separately. Review any intentional narrowing/removal of a release requirement in #873 rather than silently weakening the predicate. Rerun on the eventual integration SHA: evidence for this audited base does not transfer to changed code.

[producer]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/scripts/operation_qualification.py#L800-L1230
[workflow]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/.github/workflows/release-qualification.yml#L281-L365
[restart]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_event_cursor_restart.py
[harness]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/harness/event_cursor_restart.py
[riverhog-fixture]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_operation_lifecycle_api.py#L81-L230
[riverhog-upload]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_operation_lifecycle_api.py#L340-L535
[riverhog-retrieval]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_operation_lifecycle_api.py#L600-L707
[stove0-fixture]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/some-implementations/stove0/application/tests/test_stove0_api_parity.py#L114-L299
[stove0-assertions]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/some-implementations/stove0/application/tests/test_stove0_api_parity.py#L857-L952
[ftp]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_api_parity.py
[cli]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_cli_json_output.py#L13-L158
[reads]: https://github.com/nashspence/riverhog/blob/18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8/tests/unit/test_collection_reads.py#L93-L135
