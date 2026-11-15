# Ready work and execution occupancy: measurement and growth protocol

**Static, non-authoritative reference at `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e`.** No product operation, project import, fixture, test, diagnostic or benchmark ran for this note. No runtime or repository file was changed. The schemas below specify possible evidence; they are not fabricated measurements or an implemented observer.

## Conclusion and execution decision

**Do not run a new observation solely to infer idle Native workers from two retained slots.** Ordinary Core journal validation calls the selected unit synchronously. It does not submit that unit to a two-thread Native executor. The slot count limits retained original sessions and resources, not simultaneous execution threads. A maintained two-key fixture exists and a few ordinary scheduler calls could reveal its dispatch order, but those calls alone would not establish either continuous fresh eligibility of an unselected key or unused execution capacity authorized for that key. [S1–S3]

The remaining question is narrower than whether two functions ever overlap: **did independently eligible work remain unserved while a relevant, usable execution grant was available, and which owner had authority to dispatch it?** Answering that requires a valid eligibility lifetime, route-specific capacity observations, actual original work identity, and complete admission/start/receiving links. Missing any of these means “not established,” not “no serialization” or “idle capacity.”

The received catalog-error report identifies 465 failures in its own run as `sqlite3.OperationalError`, `SQLITE_BUSY (5)`, at parameter-free `BEGIN IMMEDIATE`. Its 302 `producer.finish` and 163 unparented failures are span buckets, not caller identities or retry dispositions. No original raw diagnostic/profile file or attachment is available in this workspace for those reported hashes; this note preserves the report as a report. It cannot newly attribute either bucket, and it cannot use those 465 errors to classify the earlier 418. [E1]

The current one-step runtime and per-step adoption remain in force. No grouped stepping, skipped prerequisites, delayed adoption, phase fusion, removed commit or changed concurrency is proposed here.

## 1. Define the work and its authority before recording time

### Exact key and observation identity

A journal observation uses the exact selected **collection, journal, capture attempt and captured directory**, with the source version, bytes, SHA-256, capture phase/cursor/failure and replay/catalog binding that the owning call already holds. Once a job is actually selected or claimed, link its workspace identity and worker epoch. A journal label alone cannot join events across replacement attempts. A catalog job version is an adoption/CAS observation; it is not interchangeable with the captured source version or Native checkpoint version. [S1–S2]

Assign bounded experiment-local tokens to exact original objects and invocation occurrences. Do not infer ownership from an OS thread number, thread name, parent span label or equality of two reconstructed records. Tokens for requests, calls, original slots, constructions, futures and workers have different domains. Each token binds to its exact original within the observed lifetime; tokens are never reused in the trace. Identity tracking must not extend a resource's product lifetime or keep exception frames alive merely to preserve an alias. Retain plain scalar metadata and use already retained originals or non-owning references where possible.

### Readiness and admission are several distinct observations

| Event/classification | Fresh evidence it actually provides | What it does not provide |
|---|---|---|
| `candidate_selected` | `_next_upload_journal_validation` returned a key from a fresh `state == validating` selector | Completed preparation, valid dependent rows, resource admission or a queued executor task |
| `source_selected` | `_read_upload_journal_validation` returned exact detached work after its existing source/directory/page checks | Authority that remains valid indefinitely; an atomic snapshot with later preparation checks |
| `preparation_return` | An actual epoch/topology/union/rank/replay call returned its original result after doing its own work | A reusable ready boolean; a reason for every `False` unless the owner exposes that reason |
| `core_ready_to_attempt_owner` | All preceding checks on that actual path completed successfully before the owner call | One atomic live-ready snapshot; successful future worker/source/budget/writer admission |
| `slot_reserved` | The exact existing `_reserve` acquired a slot lock or refused | A new thread, an executor submission, or proof of all subsequent Native admissions |
| `owner_open_or_current` | The ordinary open/currentness path reached its actual source/job/workspace checks | Catalog adoption success, semantic success, or permanence of a prior check |
| `native_invocation_enter` | The actual `Session.advance` method was entered on its original session | Native source/ledger/budget/SQLite admission success: those checks occur inside the method |
| `native_check_return` | A particular existing source, ledger or budget check returned | Other phase admissions; a new permission to omit the next check |
| `writer_admitted` / `writer_refused` | The actual statement attempt returned or raised at that boundary | Semantic completion, original resource receiving, ownership of the competing writer or the cost of a retry |
| `native_invocation_exit` | The original invocation returned or raised | Worker thread join, original future/result receiving, successful catalog adoption or resource closure |
| `catalog_adoption_return` | Actual `publish_progress` result under its source/job/CAS predicates | Final Core journal publication, all cleanup, or authority over a changed attempt |
| `resource_received` | The specific original owner confirmed the resource's receiving boundary | Global closure or receipt of every other original |

The preparation chain is a sequence of fresh observations, not a multi-table atomic ready snapshot. Completed preparation still verifies dependent Frame, Fact, Arc, Ranked and graph-result rows. Native source/certificate callbacks do not subsume all of them. `Session.advance` enters before its fresh source check, ledger load and `_start` admission; a wrapper entry must therefore be called **invocation entry**, not “all Native work admitted.” [S1–S4]

**Eligibility is point-in-time evidence unless its owner grants a demonstrated lifetime.** A successful observation at time t cannot automatically be stretched to the next call, nor can absence of an observed invalidation prove that no unobserved writer changed authority. A future observation must either use a source-owned reservation with an explicit validity/invalidation rule or retain “eligibility unknown” between points. Do not hold a new catalog transaction, rerun mutating preparation, or add extra observer SQL just to manufacture a ready interval.

Two different journal IDs establish two identities. Independence additionally requires that their selected sources/attempts and required semantic dependencies permit separate progress. Shared catalog writer admission and shared storage/execution grants remain real common resources even for logically independent histories.

## 2. Separate the execution routes and their capacity

| Route | Current source behavior | Relevant execution interpretation |
|---|---|---|
| Ordinary Core `process_due_provenance_journal_validations` | Bounded candidate selection; direct synchronous call of each selected stamped unit; stop after first durable progress per outer iteration | No Native submission queue or dedicated Native executor exists in this route. The calling execution context is doing the work. |
| Maintained fixture `_LocalApi.seal…` | Runs one generic Core scheduling opportunity, then invokes the selected-journal HTTP request | Two serial opportunities in one SDK call are not two concurrent Native workers. |
| Selected-journal HTTP seal | Synchronous route invokes `seal_provenance_journal`, which advances that selected journal after its ordinary writer/source controls | A request worker is a transport execution context. It is not the retained-session slot and must be attributed to that exact request. |
| Fixture upload-session polling | `_LocalApi.get_collection_upload_session` advances/receives custody and finalization before the status request; behavior depends on `join_workers` | This supplies archive-work opportunities, not a Native executor capacity measurement. |
| Application lifespan maintenance | Lifespan starts archive/retrieval tasks; archive maintenance serializes its own calls through an async operation lock and uses `asyncio.to_thread` | Entering a TestClient lifespan adds work and scheduling opportunities. It is not a neutral transport substitution for the maintained plain-client fixture. |
| Archive `PhysicalExecutionOwner` | Separate lane/target admission; original Thread and Future retained before start; no queue beyond its configured slots; actual joins before physical collection | This is real physical execution capacity for its owned work. It is not automatically available to synchronous Native journal validation. |

These relationships are source facts, not measurements of thread utilization. [S1, S5–S8]

An idle CPU, a low thread-CPU delta, an unused retained-session slot, a finished Future, or an empty observed call stack does not establish usable execution capacity. Capacity must be identified by its **actual owner, lane, target and grants**, including unresolved originals still charged there. A generic transport/default-executor worker cannot be declared available to Native journal scheduling merely because both exist in the process.

For a reused transport worker, completion of one callable is different from termination of the worker thread. Record the callable's actual return and the owning dispatch/limiter release if observable; do not invent a per-request thread join. For separately owned physical threads, retain the actual original thread join and its future/outcome receiving. Even that join is not semantic/resource receiving. [S8]

## 3. What maintained helpers already provide

| Existing source | Reusable evidence mechanism | Missing for this question / caution |
|---|---|---|
| `qualification/publication_profile.py` | Exact workload/source checks, publication and verification phases, real backend-read counts, full result and fixture cleanup | Publication/verification elapsed observations are not per-key queue/admission/occupancy records. Its progress file replaces the latest phase, so it is not a lossless event history. |
| Service unit/caller/scheduler growth recorders | Actual hierarchical child samples, input envelopes, exclusive Core primitives and independently validated Native/provider work | They qualify work and selected correctness conditions, not continuously fresh ready sets or route-specific executor occupancy. Some witnesses deliberately prepare phase-specific fixtures. |
| `tests/support/growth_batch.py` | Explicit observer lifetime; source metadata captured once; every completed root/descendant retained for existing public validation; flush/release | Root-count admission is not a complete byte or memory bound. Compilation, sealing and validation are observer work and must not be counted as product primitives. |
| `tests/support/async_retirement_growth.py` | Exact original/context/worker tokens, separate worker root samples, original joins and receiving links | The witness deliberately gates workers. Its gate waits alter scheduling and cannot become ordinary queue-delay evidence. Its event records do not contain an occupancy clock. |
| `tests/support/retirement_transcripts.py` | Offline causal checks for missing/duplicate/cross-original workers, joins, receiving and withdrawal | It is retirement-specific and verifies identity/causality, not the eligibility or idle-capacity intervals proposed here. |
| `tests/support/sqlite_observation.py` | Real admitted connections/cursors and original grants; actual execute/fetch behavior | It supplies no caller-ready or dispatch queue. Instrument leaf work once, preserving original argument/generator use. |
| `scripts/ci_timing.py` and `validation_process.py` | Original validation child exit, source-bound lane evidence and actual child receiving after interruption | The cooperative workload cap is not a hard timeout on an entered owner. Cleanup and original join may finish later. |

No existing helper inspected here supplies the complete proposed inference by itself. Reuse actual identity/receiving checks and maintained primitive observers; do not invoke a whole publication or gated witness to obtain unrelated coverage. [S9–S12]

## 4. Conditional raw schema and source-owned hook points

No trace is produced in this static packet. If the owner later selects an observation that can answer a narrower question, the following fields and hooks are the minimum proposal. The companion JSON inventory specifies the field classes and invariants without example measured values.

### Run manifest and identity dictionaries

The manifest must retain exact clean source start/end SHA/tree, relevant source-file hashes, interpreter/dependency/tool pins, observer and reconciler byte hashes, fixture recipe and actual input bytes/hashes, route/lifespan mode, ordinary per-step budgets, configured resource grants, selected logical keys, allowed call/record limits, and the scope of every enabled hook. Actual input hashing/serialization belongs outside primitive observation. A source recipe hash is not an actual generated input hash.

Identity dictionaries bind local tokens to exact selected source/attempt descriptors and original invocation/resource relationships. Retain only the controlled fixture identities and required authority fields. Do not record SQL parameters, exception messages, private deployment paths/tokens, arbitrary object reprs or caller data. Where an identity is sensitive outside a tiny controlled fixture, use a run-local token and retain its exact comparison/authority receipt without publishing the private value.

### Raw event fields

| Field | Meaning / constraint |
|---|---|
| `seq` | Strictly increasing append sequence in this observer process; no duplicates or silent gaps |
| `monotonic_ns` | Process-comparable monotonic timestamp; sampled at the event boundary and stored losslessly as an integer/decimal string |
| `thread_cpu_ns` | Optional per-thread CPU observation; comparable only within that same original thread, never a worker-occupancy measure |
| `event` | Closed event kind, such as call enter/return/raise, selected candidate, actual check result, submission decision, body entry/exit, writer result, original join, result/resource receiving |
| `route_token`, `call_id`, `parent_call_id` | Actual source route and invocation links; cross-thread parenthood requires an explicit original submission link, not inherited thread-local guesses |
| `key_token`, `source_token` | Exact journal/attempt/source identity for this occurrence; nullable where the actual source has not yet been selected |
| `original_token`, `submission_token`, `future_token`, `worker_token`, `resource_token` | Separate identities, populated only when actual originals exist and are observed |
| `owner_site`, `phase` | Pinned source operation/branch and actual phase, with an explicit `unknown` value where the current API does not expose a distinction |
| `outcome` | Original return/disposition category or raised exception family; never substituted for the original result |
| `error_token`, `error_family`, `database_code`, `statement_family` | Optional actual error identity/class/code and a source-whitelisted parameter-free SQL category; no message or bound values |
| `authority_refs`, `sample_refs` | References to the exact check/progress/primitive child records already produced, not synthetic metrics inferred from a final result |
| `coverage` | Complete, right/left censored, unknown, or observer-bound-exhausted as appropriate |

A short observer lock can serialize scalar event appends and the clock sample. It must never call back into product owners while held, acquire their locks in reverse order, perform SQL, hash/canonicalize data, format exceptions or wait for a worker. Such serialization perturbs scheduling and is part of the measurement conditions. Global sequence is observation order, not proof that no unobserved product event occurred between records.

### Concrete hook map

- **Opportunity and source:** actual `_LocalApi` method entry, selected HTTP route entry, lifespan `_process_archive_maintenance` entry, `process_due_provenance_journal_validations`, `_next_upload_journal_validation`, `_read_upload_journal_validation`, and actual preparation returns. Record caller route separately from whichever key the generic scheduler selects. Do not add a selector or preparation call for the observer. [S1, S5–S7]
- **Reservation and Native invocation:** actual `_reserve`, `_open`, `advance`, `_OwnedSource` checks, `Session.advance`, its existing `_start`/ledger or transactional return markers where required, actual `jobs.publish_progress` and final Core publication. `_reserve` returning `None` alone does not distinguish full capacity from a busy key; that requires a source-owned branch observation using values already held at the decision. A later pool snapshot cannot reconstruct that race. Without the hook, record refusal reason as unknown. [S2–S4]
- **Actual execution:** for a route that really submits work, bind the original callable, Future and worker through the owner's existing submission. Capture admission/reservation before start and actual callable entry/exit. A worker may enter or finish **before `try_submit` returns**. Return timestamps alone cannot supply an exact queue interval; at most they support invocation bounds. Archive `PhysicalExecutionOwner` retains the original slot before `Thread.start`, which is the relevant source-owned admission point for that owner. Its no-backlog start delay is not automatically an executor-queue wait. [S8]
- **Writer decision and disposition:** observe the actual parameter-free BEGIN attempt, its DBAPI/SQLAlchemy error callback, and the subsequent owning method's return/propagation/receiving. Link the same error occurrence to the exact source route and handling boundary. A known contention classifier result does not prove that any particular caller returned, retried or was blocked for the whole method interval. [S13]
- **Original receiving:** actual worker/body completion, original join where owned, future result/exception consumption, exact construction/storage/reader/lease receiving, and resource-ticket release are separate events. Use returned values or source-owned changes already performed; do not poll, join, consume a Future or close a resource solely to complete the trace. [S2, S8, S10]

All wrappers must call each original exactly once, preserve signature/descriptor behavior, return the same result and re-raise the same `BaseException` without changing exception order. Context-manager suppression and async cancellation behavior remain unchanged. Never eagerly consume an iterator/generator or materialize SQL parameters for logging. A diagnostic failure must mark evidence invalid without replacing the product's exception or claiming cleanup succeeded. Check descriptor/hook restoration after observation. None of these hooks is implemented by this packet.

## 5. Lossless bounds and observer cost

“Bounded and lossless” applies to an explicitly declared event scope, not all machine activity. Before a future run, derive a record/byte allowance from the admitted scheduler calls, source child caps, actual finite fixture/control widths, permitted originals and required cleanup endpoints. Reserve room for return/raise and closing records before admitting another observed scope. Plain scalar records and references to already returned immutable values are captured online; full JSON, digests, sample canonicalization and reconciliation happen after primitive observation is suspended or finished.

Do not use a ring buffer, sample events, truncate controls while calling the record exact, or silently discard oldest records. At the planned bound, stop admitting further harness turns at a safe boundary. If an entered operation unexpectedly exceeds the preflight event bound, record the first coverage-loss boundary using reserved space, preserve the exact captured prefix, mark the trace incomplete and reject whole-interval occupancy conclusions. Do not inject an asynchronous exception into product bookkeeping, stop the original resource owner, or fabricate the missing tail. A bounded retained prefix is not a successful complete trace.

Cleanup and original receiving keep their own reserved evidence space and original owners. If that space is insufficient or an original remains unknown, state the gap; do not rewrite an unknown receipt as zero live resources. Resource owners must still receive their originals normally. The workload's approximately 120-second cooperative cap stops **new** admission at safe boundaries. It does not bound an entered synchronous database/filesystem call, guarantee cancellation responsiveness or authorize deleting live scratch. The maintained child launcher receives the actual original exit; cleanup may extend beyond the cap. [S12]

Observer locks, clocks, object-token lookups, dictionary/list growth, serialization, source/digest checks, helper compilation, sample validation and final receiving all have costs. An instrumented interval includes the applicable hook overhead. A diagnostic retaining records and identities is not a production memory measurement. Do not subtract guessed observer costs or compare this prospective lane with 36.315/49.217-second publication figures or the older differently instrumented Native timings. No paired speed experiment is required for this question.

## 6. Offline reconciliation and permitted interval claims

A future reconciler must read only the retained trace/manifest/child receipts; it must not import or execute the product, regenerate fixtures, reread mutable catalog state or repair missing events with inferred values.

1. **Validate provenance and coverage.** Check exact source/probe/input hashes, fixed schema, finite limits, monotonic clocks, sequence integrity and explicit censored/overflow state. Report original worker/lane/launcher exits separately from derived reconciliation status.
2. **Validate identities.** Each occurrence belongs to one exact source/attempt and original; replacement starts a new identity. Reject cross-original joins/results, reused tokens, duplicate completion and invented submissions. Preserve actual cross-thread links.
3. **Reconstruct calls and admissions.** Pair every enter with its actual normal return or raise, allowing nesting and workers that enter before submit returns. Keep invocation entry, successful guard, budget admission, writer admission and semantic/catalog outcome distinct. A refused submission has no invented execution body.
4. **Reconstruct receiving.** Future outcome publication, callable exit, original join, physical result collection, semantic result adoption and resource release must occur in their actual permitted order. `Future.done`, cancellation requested or dead-thread observation alone cannot free a charge. An unknown original stays unknown/charged according to its owner.
5. **Reconcile counts.** Every actual declared child occurrence, including completed children before a later failure, must match its independently validated sample/invocation. Keep Native-workspace and Core/provider ownership non-overlapping. Reconcile writer-attempt/error totals to their actual event occurrences and outer dispositions without inferring retry identity from equal statements.
6. **Construct only supported intervals.** Use actual endpoint pairs and owner validity rules. If exact submission/admission time was not observed, label bounds or invocation-to-entry delay rather than queue delay. Open endpoints are censored. Per-thread CPU deltas are not scheduler occupancy or global CPU utilization.
7. **Apply the idle-capacity inference guard.** Only classify an interval as unserved eligible work with usable capacity when the exact key has a source-owned eligibility lifetime covering it, the relevant executor/grant is known and available through it, all competing originals/refusals/dependencies are accounted, and the owner is authorized to dispatch that work there. Otherwise preserve the specific unknown. Even a complete such interval needs the owning policy decision before calling it avoidable serialization.

| Interval | Required actual endpoints/authority | Safe label when missing |
|---|---|---|
| Candidate observation to next attempt | Fresh selection event and actual later invocation | Candidate revisit gap; not ready wait |
| Core preparation pass to owner invocation | Actual sequential return markers and invocation | Guard-chain-to-call elapsed interval; not continuous global eligibility |
| Submission/admission to callable entry | Same original, real pre-start admission marker, entry event | Invocation-to-entry bound, or unknown; not queue residence |
| Callable body | Same actual enter and return/raise | Censored execution interval if exit absent |
| Result availability to original join/collection | Actual Future/result signal and original join/collection | Receiving delay only within that owner's specified scope |
| Resource charge | Original admission and actual matching receiving | Still retained/unknown; never freed by elapsed time |
| Failed SQL attempt | Actual execute start and error callback boundary | Attempt interval only; excludes later handling/retry/rollback |
| Eligible work with idle relevant capacity | Complete validity, ownership and capacity intervals above | Not established |

These are the rules for a possible reconciler, not claims that the supplied reports contain such intervals. The existing retirement reconciler is a useful model for missing/duplicate/cross-original checks; its gated trace cannot supply ordinary queue timing. [S10]

## 7. Growth and resource composition of a conditional tiny observation

The maintained two-journal recipe stages real tiny source bytes and uses the ordinary capture/scheduler route. It does not inject completed preparation. Its existing test runs to terminal with a 1,000-iteration guard and must not be invoked wholesale for this request. A source-derived four-scheduler-call prefix is only a possible bounded observation of dispatch; it is not promised to reach terminality or establish simultaneous readiness. [S3]

Crucially, **four calls to `process_due(limit=1)` do not guarantee four Core or Native calls**. With total outer allowance `L=4` and retained-slot capacity `C=2`, the maintained generic candidate cap is `L(2C+1)=20`. Zero-progress candidates can be tried before the first-progress break. Unless a tighter two-key bound is independently proved, use 20 for observer admission and record the actual count separately. Existing per-unit and per-owner declarations still cap owner and Native calls at one each; there is no grouped stepping. [S1, S14]

For any future observation retain:

- All actual capture/epoch/topology/union/rank/replay work and full source/metadata/error widths, including preparation-only or rejected turns.
- Every actual Native phase, ordinary budget, fresh check, transaction, catalog adoption, finalization opportunity and receiving operation. A row/byte credit is not a complete Core cost or peak-memory bound.
- Exclusive Core primitives and inclusive actual Native/provider children under their existing metric ownership; avoid summing overlapping raw scopes. Complete earlier samples survive a later parent failure.
- Slot/resource peaks separately from sequential turnover. Current defaults of two retained Native slots and shared storage tickets do not grant two execution threads or bound aggregate opens, closes, SQL, CPU or elapsed time. Each operation/cursor ticket and storage/connection/lease follows its own receiving boundary. [S2, S11, S14]
- Observer setup, metadata compilation, records, flushing and restoration outside product primitive counts, with their costs explicitly part of the diagnostic conditions. A sample-root count is not a byte/RSS bound. [S9–S11]

Two minimal initial histories would not exercise correction-wide revalidation, nontrivial DAG adjacency, growing history fanout or all preparation growth. Repeated whole-resource preparation and correction work remain independent obligations. Neither an observed overlap nor a short successful trace clears G10/G12/G13, total-work growth, all-cost non-growth or archive-size acceptance. No extra corpus, publication campaign, full census or broad suite is needed to establish the present measurement gaps.

## 8. Writer-refusal source possibilities, not retrospective attribution

The source supports several distinct contention dispositions:

| Source family | Existing disposition for classified contention |
|---|---|
| Finalization readiness scheduling | `process_due_finalizations` returns progress already made when `_schedule_ready_finalizations` encounters contention |
| Archive constructor/turn registration | Registration returns `busy`; no new work is asserted submitted from that refusal |
| Original finalization pipeline admission | The entered worker first waits outside SQL for its original registration transaction to finish, then resolves its exact claim/gate; a failed transaction leaves admission unresolved rather than caching a no-effect decision |
| Upload maintenance/drain | Outer maintenance returns prior progress; retirement can return `False` |
| Opted-in terminal fence | Returns `busy` only for the explicitly nonwaiting contention path; other errors retain their propagation |
| Failure-feedback recording | Known contention can omit feedback while the original semantic exception remains the actual failure |
| Selected-journal HTTP seal | Uses ordinary writer admission and has no local `busy` deferral catch at that method boundary |

The exact begin mode is selected by `_writer` and `catalog_db`'s transaction event; nonwaiting mode temporarily uses `busy_timeout=0` and restores it in `finally`. `catalog_write_contention` recognizes the relevant SQLite/SQLSTATE families but explicitly grants no operation outcome or closure. The pipeline's registration wait also demonstrates why an entered physical worker is not necessarily executing an admitted semantic child. These are inspected source possibilities, not an exhaustive historical caller inventory or a mapping of 302/163 events. A future observer needs exact route, branch, error-occurrence and outer-disposition links to make that mapping. [S13, S15]

## 9. What this static packet establishes

It establishes the relevant source responsibilities, existing observation pieces and missing predicates for the requested inference. It shows why retained slots, span buckets, thread CPU, candidate rows, prior readiness and Future completion cannot substitute for their separate owners.

It does **not** establish a measured ready-wait interval, a relevant idle-capacity interval, actual worker utilization, queue occupancy, causal writer owner, retry amplification, throughput benefit or avoidable serialization. No actual trace or reconciler output is claimed. The next useful action is an owning decision about which route/admission question to observe and whether it exposes a valid eligibility lifetime and relevant execution grant. A tiny serial Core prefix alone would not resolve those missing authorities, so no new product run is recommended now.

## Pinned sources and supplied evidence

The companion `ready-occupancy-growth-inventory.json` records exact source-file hashes, proposed fields/invariants and the static-versus-reported distinction.

- **S1 — Core selection and dispatch:** [stamped unit and scheduler](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2257); [candidate selector and source reconstruction](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7905-L8025).
- **S2 — Retained Native owner:** [source callbacks](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L439); [reserve, receiving and advance](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1836-L2123); [job currentness and publication](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L463-L544).
- **S3 — Real two-key recipe:** [ordinary scheduler test](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L324-L369), inspected only.
- **S4 — Checks inside invocation:** [Native advance](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1513-L1564); [entry budget start](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py#L2230-L2254); [completed rank dependency checks](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L244-L291).
- **S5 — Fixture SDK opportunities:** [_LocalApi](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_operation_lifecycle_api.py#L234-L289); [real fixture construction](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/qualification/canonical_history_scale.py#L124-L142); [SDK selected-journal loop](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/client.py#L1631-L1643).
- **S6 — Selected HTTP opportunity:** [seal route](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/routers/collections.py#L593-L607); [selected service](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2086-L2120).
- **S7 — Lifespan execution:** [maintenance and archive reaper](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/app.py#L171-L239); [lifespan tasks](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/app.py#L342-L385).
- **S8 — Actual physical execution:** [original Future/Thread, submission, collection and close](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/physical_execution.py#L123-L412).
- **S9 — Maintained publication observer:** [publication profile](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/qualification/publication_profile.py#L47-L243); [full history evidence](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/qualification/publication_evidence.py#L108-L240).
- **S10 — Original causal evidence:** [PhaseEvidence identities/events](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/async_retirement_growth.py#L52-L124); [intentional gates and joins](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/async_retirement_growth.py#L880-L951); [offline retirement verifier](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/retirement_transcripts.py#L15-L130).
- **S11 — Primitive/observer/resource ownership:** [admitted SQLite observer](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/sqlite_observation.py); [SampleBatch](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/growth_batch.py); [storage grants](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/journal_storage_grants.py); [actual operation and cursor receiving](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/sqlite-admission/src/sqlite_admission/__init__.py#L255-L334).
- **S12 — Original child/lane:** [maintained child receiving](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/scripts/validation_process.py#L20-L76); [maintained timing wrapper](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/scripts/ci_timing.py#L310-L365).
- **S13 — Writer mode and dispositions:** [writer admission](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_write_admission.py#L66-L79); [classification/BEGIN mode](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/catalog_db.py#L75-L131); [finalization readiness](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3532-L3583); [registration](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_work.py#L127-L171); [maintenance/drain](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_maintenance.py#L90-L150); [retirement](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_maintenance.py#L219-L253); [terminal finalizer fence](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_registry.py#L370-L413); [terminal invocation fence](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_invocations.py#L602-L628); [failure feedback](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3968-L4019).
- **S14 — Current growth composition:** [service unit and stamped caller](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L9411-L10043); [scheduler caps](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L10046-L10333); [Native owner children](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1422-L1833); [actual Native child/residual accounting](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_advance_growth.py#L602-L805).
- **S15 — Entered original awaiting admission:** [original pipeline admission](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalization_admission.py#L26-L79); [registration Event receiving](https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3735-L3800).
- **E1 — Supplied reports/request:** `/workspace/scratch/21c38f9cc56d/ready-occupancy-context.json` retains comments [6889181049](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049), [6889902338](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) and [6890015223](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890015223). The last report gives raw diagnostic hash `5da872016fae26039c92c53d36ec3281374e71b2b8475281cca72d55b8187390` and profile hash `bdee7c0313e257933888f0fc6058e8387def8c30c1e5f44c3256897d8bd5ee6a`; their original files were not available here and their hashes were not independently verified. The preceding qualification and Core-stepping receiving decisions are likewise attributed reports, not fresh executions.
