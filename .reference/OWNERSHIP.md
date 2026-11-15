# Ready work and execution ownership: static responsibility map

## Scope and result

This analysis is pinned to `d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e` in `/workspace/scratch/21c38f9cc56d/riverhog`, for issue #991 comment 6890015223. It reads source and the supplied receiving reports. It does not import the product, execute a fixture, run tests, or generate an occupancy measurement. The accompanying `ready-occupancy-ownership.json` pins the inspected files, line ranges, supplied request/context, and this note.

**The available evidence does not establish an interval in which independently ready journal work waited while an authorized journal executor was idle. It also does not disprove avoidable serialization.** The ordinary Core scheduler executes a selected journal unit synchronously on its caller; `JournalValidationOwner` retains custody slots and per-key locks, not a journal thread pool. The fixture's direct scheduler call and selected-journal HTTP request are separate, sequential opportunities. An application with lifespan enabled has additional maintenance callers. Archive finalizers and custody work use an explicit physical execution owner with different authority and capacity. These cannot be combined into one worker-occupancy denominator. [S01] [S02] [S03] [S04] [S05]

A maintained two-journal setup exists. Reading its source establishes that two distinct keys can be submitted to the ordinary scheduler without manufacturing accepted preparation state. A few calls could produce a legal partial Core trace. That trace would not, by itself, establish simultaneous fresh readiness plus an idle authorized executor: the direct Core route has no such executor, and its readiness checks occur inside each selected synchronous turn. No run of that fixture was performed for this analysis. [S06]

## 1. What can be observed without inventing authority

“Ready” must name the particular operation and the observation boundary. The current source exposes the following distinguishable events; it does not expose a durable, all-purpose ready flag.

| Observation | What it establishes | What remains unknown or must still happen |
| --- | --- | --- |
| `_next_upload_journal_validation` returns a key | A bounded catalog selection found a row in `validating` state. | Capture and preparation may be incomplete; source, current ownership, local capacity and execution admission have not been established. |
| `active_keys()` includes a key | This owner retains a slot for a collection/journal/attempt. | The slot can be executing, warm, closing, or retained for original receiving; it is not a ready queue or an occupied worker count. |
| The preparation chain reaches the call to `owner.advance` | The selected Work passed this turn's source-read and epoch/topology/union/rank/replay checks. This is a **Core-ready-to-attempt observation** for that exact invocation. | These were separate live checks, not one atomic snapshot. Reservation, lease, worker claim, later currentness and Native resource checks remain. No continued ready interval follows. |
| `_reserve` returns the exact slot | Local retained capacity and the nonblocking per-key lock were acquired for this attempt. | A cold open can still fail source/job/lease/worker checks or fail to receive original construction; storage and operation admission remain. |
| Exact workspace lease and worker claim succeed | Those particular ownership fences succeeded for the selected source/job/epoch. | This grants neither all future Native capacity nor arbitrary other work. |
| `JournalValidationSession.advance` is entered | An actual Native invocation began on the caller's execution thread. | Its open/failure/budget checks, current-source check, ledger load, replay admission and phase operations still follow. Entry is **not** proof of successful Native admission or commit. |
| A particular Native phase returns and durable progress is available | The corresponding actual operation and its returned progress can be recorded. | A method can return unchanged progress. Catalog adoption, terminal-result checks and original receiving are separate. |
| `jobs.publish_progress` returns an adopted job | The exact source fence and current job identity/version/progress CAS accepted that detached progress. | Final Core journal state and storage/reader receiving can still remain. |
| An owner releases its original slot/resources | Only the receiving conditions actually checked by that owner have completed. | This is not global quiescence, other-owner completion, or provider disposal authority. |

The selector checks only catalog state. [S24] Core performs capture and all preparation checks before calling the owner. The owner may return `ValidationAdvance(False)` for a full pool, a busy key, stale work, capacity refusal, or a non-progressing/closing path. A cold `_open` can already adopt recovered progress before a later refusal. Consequently `False` does not mean “no catalog effects,” and one boolean cannot classify queueing. The ordinary service compares before/after stamps containing journal state, capture version, scan version and job version, and only on a normal return. [S01] [S02] [S07]

The exact Native entry distinction matters: `advance` starts by checking its open/failure state and budget, then verifies source, loads its ledger, calls replay admission and executes the selected phase. Capacity can be refused after method entry. Its `_OwnedSource` combines worker identity, current source and graph-certificate checks; progress adoption additionally compares the latest job version/progress. These are separate obligations. [S08] [S09] [S23]

An observer must follow the actual selected Work, source/attempt binding and original job/worker identity through these events. It must not re-query readiness just to create a timestamp, reuse a saved boolean as authority, or replace the normal chain with one cheaper predicate. The observable event is the original branch or return, with its actual disposition. The record remains evidence of that point, not a permission token.

## 2. Responsibility and opportunity by route

| Route | Who supplies the execution opportunity? | Ordering and authority | Meaning of capacity or an idle interval |
| --- | --- | --- | --- |
| Ordinary Core `process_due_provenance_journal_validations` | Its caller's current thread. No executor submission occurs in this method or `JournalValidationOwner.advance`. | Bounded candidate selection, fresh preparation, at most one Native `advance` per owner call, per-step catalog adoption, then the existing finalization opportunity on progress. | Configured journal concurrency limits retained slots. A free slot is not an idle worker authorized to run another journal. |
| Maintained `_LocalApi` seal override | First the synchronous test coordinator caller; then the HTTP transport for the selected seal request. | The direct scheduler opportunity precedes the HTTP call. The first can select another key or do no work; the second addresses the specified key. | Two opportunities do not imply simultaneous journal execution. Their complete invocation and return boundaries must remain separate. |
| Selected-journal HTTP seal | FastAPI's synchronous endpoint dispatch through the transport's AnyIO worker path. | Access check, selected service seal transaction, one selected Core advance, status return. It does not fan out across candidate keys. | Available transport worker capacity would matter only with an independently submitted request and its own fresh authority/admission. It is not inferred from a retained Native slot or a request count. |
| Application archive lifespan maintenance | One coroutine awaits one `asyncio.to_thread` maintenance call at a time under its local operation lock. | Full sweeps call registration, journal validation, custody, finalization and other owners in source order. Short receive sweeps call finalization maintenance. Other HTTP callers are not protected by this local lock. | The asynchronous application can have other requests/tasks in flight, but the archive loop itself is serial. Default-executor capacity is a distinct owner boundary from AnyIO endpoint capacity. |
| Archive finalizer/custody physical execution | `PhysicalExecutionOwner` retains and starts an actual original `Thread` and `Future` after capacity checks. | Global lane and per-target slots, exact key/target/owner identity, retained construction and semantic continuation requirements. | These are actual physical-worker slots. They do not grant authority to execute Core journal turns. A joined but not collected result still occupies a physical slot. |

### Ordinary Core scheduling

For one `limit=1` iteration, the scheduler sweeps original stale custody, may take one cleanup turn, rotates warm keys, selects at most `journal_validation_concurrency + 1` fresh candidates, and orders warm/fresh work according to retained occupancy. It invokes candidate units synchronously until one changes its catalog stamp, then schedules finalization if ready and stops that iteration. With `C` retained slots, at most `C` warm plus `C+1` fresh candidates are presented in that iteration. `limit` counts progressed scheduler/cleanup iterations, not all attempted candidate units or Native calls. Nothing here establishes a backlog of ready Native work. [S01]

The default `journal_validation_concurrency=2` bounds retained Native custody. The owner uses a short pool mutex, a nonblocking slot lock, exact source/job matching and a kernel workspace lease before claiming a worker. Slot contents include the actual construction, Native session, resource stack and lease. Original independent readers can keep those resources charged after semantic completion. The two-key scheduler test checks eventual bounded progress and worker reuse; it is not a two-thread execution test. [S02] [S06] [S10]

Changing the caller from one synchronous coordinator to multiple concurrent callers would be a scheduling/workload decision. Existing locks and capacity defenses do not themselves establish that such an execution route was used, that independent work was ready at overlapping times, or that a current delay is avoidable.

### Maintained fixture SDK and coordinator

The publication fixture deliberately appends member inputs in a loop, uploads required/shared journals through the synchronous SDK, stages member-history structures, and only then calls `producer.finish`. The SDK journal helper creates/appends the selected bytes and repeatedly seals that journal while its status remains `validating`. `producer._stage_journals` likewise uploads sorted entries one at a time. This source creates serial opportunities; it does not maintain an independently ready journal queue. Some ordering serves actual downstream inputs: member-history construction consumes selected journal summaries/anchors, and finalization follows completion of the staged inputs. The inspection does **not** prove that every independently valid journal must always be serialized by product policy. [S11] [S12]

`_LocalApi.seal_collection_upload_session_provenance_journal` first directly calls the Core scheduler, then performs the selected HTTP seal. `_LocalApi.get_collection_upload_session`, when `join_workers=False`, directly polls custody and finalization before doing the HTTP status read. In this mode these calls receive already completed physical work rather than synchronously joining every worker. The separate test helper mode joins the original retained physical workers and then lets maintained owners collect their results; that mode is not interchangeable with the publication fixture's `join_workers=False`. [S03] [S13]

`producer.finish` waits for its local retained recovery/closing work, requests completion, sleeps between collection status polls, and later drains custody. The producer owns a separate original heartbeat thread, with a spawned API client; `stop` signals and joins that exact thread before releasing source resources. The heartbeat creates a possible concurrent request route, but this static fact does not attribute any reported SQL failures to it. The producer operation lock serializes its protected lifecycle operations; it is not a global service lock. [S14] [S30]

### Selected HTTP and TestClient lifecycle

The selected seal endpoint is synchronous. The installed, pinned FastAPI source calls Starlette `run_in_threadpool` for such endpoints, and Starlette uses AnyIO `to_thread.run_sync`. The inspected AnyIO implementation retains an invocation future, obtains its capacity limiter and dispatches to a new or idle worker associated with the event-loop context. None of these limits is the journal owner's retained-slot setting. A count of created threads or completed requests does not establish simultaneous busy or idle worker capacity. [S04] [L01] [L02] [L04]

The maintained publication fixture constructs TestClient without entering it. Its `_api` helper also constructs an unentered authenticated TestClient. Starlette's unentered path uses a blocking portal for each request when no persistent portal exists. `TestClient.__enter__` instead creates the persistent portal, starts the ASGI lifespan task and waits for startup. Entering the client therefore changes both transport lifetime and the application's active work, rather than only reducing portal setup cost. [S03] [S11] [L03]

### Application lifespan and original receiving

The lifespan starts archive maintenance, retrieval restore and retrieval cache tasks. The archive coroutine serially awaits each `to_thread` call; its operation lock belongs to that coroutine's maintenance route, not to all requests. Full maintenance sweeps call several owners sequentially. The other reapers also use background thread dispatch for their work. A trace of this route must distinguish each original task/submission, thread entry/exit and synchronous result from the parent coroutine span. [S05]

On lifespan exit the code cancels and awaits the asynchronous reaper tasks, then closes an application-owned container. An asynchronous cancellation request or cancellation receipt is not by itself evidence that an already entered synchronous function has returned. The source inspection does not classify this as an observed cleanup defect. It identifies an evidence obligation for any future measurement: receive actual underlying work and the owning container/resources, not merely the cancellation of the awaiting task. The maintained unentered fixture avoids adding these lifespan tasks; enabling them for a comparison requires an explicit ownership and workload decision. [S05]

## 3. Original workers, locks, leases and results

The resource boundaries must be recorded separately:

* **Journal custody:** the original per-attempt slot lock is held around owner advancement. The kernel workspace lease precedes worker claim. `_drop` does not remove the slot until Native construction/storage receiving completes; independent result readers can retain storage. Source replacement, a complete job, or a cancellation request cannot be used as release evidence. Exact source verification and the later catalog CAS still protect stale worker publication. [S02] [S08] [S09]
* **Physical worker:** capacity is reserved and the exact `Future`/non-daemon `Thread` stored before `Thread.start`. Worker code can begin before `try_submit` returns, so a submission-return timestamp cannot serve as the admission time. `_run` preserves the actual return or `BaseException` in the original future. [S15]
* **Physical result collection:** collection rotates only slots belonging to the exact receiving owner. It requires the worker to be nonalive, calls `join(timeout=0)`, and also requires the future to be done. It then returns the original future and removes that physical slot. `Future.done()` alone is insufficient. `close` stops submissions and joins original threads outside the registry lock; it does not erase pending outcomes. [S15]
* **Semantic continuation:** physical collection is not disposal. The retained finalizer registry resumes an existing pending original before considering another construction. `ArchiveFinalizerProgress` retains a completed child result until its original workflow receiver returns; retrying that receiver does not advance the child again. Maintenance catches/logs the actual received worker failure while the original actor/constructor remains retained. [S16] [S17] [S26]
* **Producer/transport/lifespan:** producer heartbeat stop/join, transport invocation/future receiving, and lifespan underlying-thread receiving have their own owners. They cannot be substituted for Native workspace or physical finalizer receiving. [S14] [L01] [L02] [S05]

The physical execution defaults reserve separate forward/terminal lanes and per-target allowances, plus distinct construction retention limits. A free physical lane, an available journal slot, an available SQLite operation ticket, and a free transport worker are different facts. A valid occupancy analysis must use only capacity authorized for the specific ready operation. [S10] [S15] [S25]

## 4. Catalog writer-refusal paths: source candidates, not attribution

The supplied request reports 51,811 catalog execute attempts and 465 failures, all `sqlite3.OperationalError`, SQLite `SQLITE_BUSY (5)`, at parameter-free `BEGIN IMMEDIATE`. It assigns 302 to the `producer.finish` instrumented parent bucket and 163 to no instrumented parent. Those observations are receiving-owner reports here. Only their published raw/profile hashes were supplied; their raw caller records were not received or revalidated in this analysis. The two buckets are **not** two established caller families.

The catalog hook can issue the identical `BEGIN IMMEDIATE` in two modes. With `sqlite_immediate_writer` and `sqlite_defer_if_busy=True`, it temporarily sets `PRAGMA busy_timeout=0`, attempts the statement, and restores the prior setting. The default immediate path uses the same statement without that temporary zero timeout. `_writer` owns those options. Thus statement text and error code alone do not identify either the caller or its waiting/refusal policy. `catalog_write_contention` classifies busy/locked SQLite errors and selected PostgreSQL SQLSTATEs; its own contract supplies no operation outcome or closure. [S18] [S27]

The following is a deliberately narrow source map of reachable path candidates, **not a full writer census and not an attribution of any of the 465 failures**:

| Source-owned path | Actual writer mode and local disposition | Source |
| --- | --- | --- |
| Scheduling ready finalizations | `_schedule_ready_finalizations` uses deferred writer admission. `process_due_finalizations` returns already obtained progress on classified contention; other errors propagate. This can follow collection of earlier original results. | [S19] |
| Registering finalizer/custody work | `ArchiveFinalizerWork._submit` acquires deferred parent/control admission before registration. Classified contention returns `"busy"`; it does not create successful child work. The custody scheduler uses this same registration path. | [S20] |
| Maintenance terminal draining / idle finalizer retirement | `_drain_due` uses deferred writer admission; the outer maintenance turn can return prior progress on contention. `_retire_finalizer` uses deferred writer admission and returns `False` on contention, keeping the original retained. | [S17] |
| Exact terminal fences for retained finalizer or upload invocation | `inspect_terminal_write_records` reaches `_writer(defer_if_busy=nowait)`. The fence methods return `"busy"` for classified contention only when their explicit `defer_if_busy` option is true; otherwise the error propagates. | [S21] [S28] [S29] |
| Retained finalization failure feedback | The best-effort diagnostic write uses deferred admission and omits feedback on classified contention. The original semantic exception is still re-raised; diagnostic failure cannot release its claim or discard its receiver. | [S22] |
| Original finalization admission after registration | The original physical pipeline waits for the registering transaction to exit, then takes deferred writer admission to resolve its exact claim and gate. A failed read/transaction leaves admission unresolved; the actual error propagates through the original retained workflow/future, rather than caching a no-effect decision. This is a later admission than local thread registration. | [S31] [S32] |
| Selected-journal HTTP seal transaction | `seal_provenance_journal` calls `_writer(session)` with the default mode, before catalog reads. There is no local classified-busy-to-progress conversion in this method; an error propagates to its caller's handling. | [S07] |

A later polling opportunity can retry a refused admission, but source review alone gives neither its actual count nor its elapsed interval in the reported publication. The maintenance caller, physical worker, request worker and heartbeat are distinguishable candidate execution contexts; none can be assigned to the 302/163 buckets without their original caller/operation records.

The reported failed-attempt totals of 18.410 ms wall and 12.768 ms thread CPU end at entry to SQLAlchemy's error callback. They exclude retry, connection, rollback, coordination and receiving work and do not define a concurrent critical path. They cannot bound the total scheduling cost or establish idle capacity. The new 465 observations also do not supply the missing exception metadata for the historical 418.

## 5. Ownership decisions needed before a useful occupancy measurement

| Route selected for observation | Decision and minimum actual evidence needed | What a small current-source trace could not establish by itself |
| --- | --- | --- |
| One ordinary Core coordinator | Identify the original caller and bounded candidate/turn events. Record actual prep returns, refusal branches, Native invocation/return, adoption and cleanup on each exact key. | An idle journal executor or a continuous independent-ready interval, because no executor/ready queue is owned by this route. |
| Maintained SDK/coordinator | Preserve the direct-coordinator-then-selected-HTTP order, exact unentered transport mode and producer heartbeat ownership. Separate coordinator, API, SDK wait and original physical-receive intervals. | That serial SDK staging withheld concurrently admissible work, unless the second exact source and all its fresh prerequisites actually existed during the claimed interval. |
| Concurrent selected HTTP requests | The application/transport owner must identify which existing requests are submitted independently and who receives each original worker/future on failure or cancellation. Keep the current endpoint and Core guards intact. | That merely enabling another request or entering TestClient measures the old fixture; either changes the available execution opportunities/workload. |
| Application lifespan | Identify the original maintenance/restore/cache submissions and their joins/results, plus concurrent request ownership. Preserve all normal maintenance/finalization opportunities. | That a delayed journal candidate was ready rather than waiting on prerequisites, busy custody, writer admission, or an earlier owner in the same serial sweep. |
| Existing physical finalizers/custody | Use exact pre-start reservation events, global/per-target refusal, worker entry/exit, original future collection and separate semantic receiving. | That spare physical capacity can execute Native journal turns, or that a physically joined result has completed semantic disposal. |

Any future bounded record needs an operation/route identifier and exact source/attempt correlation in addition to thread-parent spans. Queue/admission/start/return/adopt/receive intervals must be derived only where the original owner emitted both endpoints. Missing events remain unknown; a cap-triggered incomplete operation remains partial until its original receiving is observed. The observer must avoid SQL parameters, exception messages and private deployment identity, and must not add authority queries or replace normal checks. This is a measurement boundary specification, not a proposal for a new ready cache or scheduling API.

For the present request, the proportionate conclusion is static: preserve one-step execution, per-step adoption and all existing finalization/custody opportunities. The maintained two-key fixture gives a legal bounded setup, but a four-turn Core-only prefix would not resolve the central idle-authorized-capacity question. Before more execution, identify the specific route and an original owner boundary that can supply the missing simultaneous-ready/admission/occupancy evidence, or obtain the supplied SQL diagnostic's lossless caller records for a bounded offline attribution. No runtime optimization, concurrency increase, throughput claim, whole-census clearance, or G10/G12/G13 closure follows from this analysis.

## Source references

All `S` references below are at the pinned d4d7 source. The JSON receipt records exact file SHA-256 and Git blob identities. `L` references are read-only installed dependency sources, pinned separately by file hashes and installed metadata; they were not imported. The `L` citations target the matching `id` records in `installed_dependency_sources` in [the ownership audit](evidence/ownership-audit.json), which pins each installed file, line range, version and hash.

[S01]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2251
[S02]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1836-L2122
[S03]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_operation_lifecycle_api.py#L234-L291
[S04]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/routers/collections.py#L593-L607
[S05]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/app.py#L105-L384
[S06]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L303-L369
[S07]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2086-L2127
[S08]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L465
[S09]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py#L433-L544
[S10]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/runtime_config.py#L124-L152
[S11]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/qualification/canonical_history_scale.py#L128-L363
[S12]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/client.py#L1589-L1642
[S13]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/finalization.py#L13-L36
[S14]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/producer.py#L518-L1047
[S15]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/physical_execution.py#L120-L408
[S16]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_progress.py#L16-L65
[S17]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_maintenance.py#L90-L253
[S18]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/catalog_db.py#L75-L131
[S19]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3520-L3583
[S20]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_work.py#L102-L171
[S21]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_registry.py#L370-L425
[S22]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3968-L4048
[L01]: evidence/ownership-audit.json
[L02]: evidence/ownership-audit.json
[L03]: evidence/ownership-audit.json

[S23]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L1513-L1584
[S24]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7874-L7928
[S25]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/archive_execution_tuning.py#L9-L43
[S26]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalizer_registry.py#L95-L220
[S27]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_write_admission.py#L66-L79
[S28]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_invocations.py#L602-L631
[S29]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_upload_drain.py#L128-L190
[S30]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/producer.py#L327-L336
[S31]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/archive_finalization_admission.py#L21-L79
[S32]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3735-L3844

[L04]: evidence/ownership-audit.json
