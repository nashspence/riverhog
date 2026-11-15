# Core invocation, coordination, and available parallelism

Audited source: `3b47cbba770c31eb1487b50f7e3b16533c10e827`, branch `reference/external/991-native-phase-cost`.

This is a read-only source analysis. No tests, fixtures, Native advances, publications, or concurrency experiments were run. The cited maintained tests are executable specifications inspected as source, not new passing evidence. The ten principal service/client/publication files listed below have no diff from published observation source `931f01e65d8849835774a97ffbd2954ae6802ce5`.

The historical counts are supplied by published comment `6879735829`, reproduced in `native-phase-issue-evidence.json`. That comment reports 400 stamped Core calls, 399 owner advances, four retained opens, 427 TestClient requests, 428 hook-observed AnyIO worker lifetimes, and 418 raised catalog execution attempts without error-family attribution. A raw interval/queue receipt for that run has not been located in this review. Those counts therefore do not establish simultaneous work, available backlog, waiting time, or the causes of the raised attempts.

## Conclusions

1. Journal concurrency 2 is a limit on retained session slots. It does not create a two-thread executor. One journal's original session is serialized; independent journals may proceed through separate slots when independent callers/scheduling provide them.
2. The publication fixture intentionally appends members and stages supplemental journals sequentially. Each journal upload waits until sealing completes. It cannot demonstrate saturation of the configured journal capacity simply by counting four opens.
3. One local SDK seal invocation can perform two separate Core opportunities in sequence: the fixture's direct scheduler call, then the HTTP seal endpoint's selected-journal call. This helps explain why Core calls and HTTP requests are different inventories. The historical totals alone do not identify exactly which calls made progress or map all 399 advances to those two sites.
4. The journal upload loop immediately repeats the seal request while validation remains active. The 50 ms delay belongs to finalization/custody polling, not that journal loop.
5. The fixture creates unentered TestClient instances. In the pinned transport, each request therefore creates a fresh blocking portal; AnyIO worker pools belong to their event loop and workers are stopped with the root task. This is a source-supported mechanism for repeated worker lifetimes, not evidence of 428 concurrent workers or a measured attribution of their cost.
6. Entering TestClient as a context manager also runs application startup/lifespan and starts background maintenance. It would change the fixture's scheduling conditions. Persistent portal reuse needs a separately specified ownership/lifecycle comparison; a mechanical `with TestClient(...)` change is not semantics-neutral.

## The Core turn surrounds Native work

`SqlAlchemyCollectionUploadService._advance_provenance_journal_validation` reads a metadata stamp, executes one unit, then reads the stamp again. Its returned boolean detects catalog-visible progress; it does not directly count Native actions (`collection_uploads.py:2122–2127`). The stamp reads journal, epoch-scan, and job versions through one joined snapshot (`:7874–7902`).

The unit executes this sequence (`collection_uploads.py:2129–2177`):

| Stage | Work and dependency |
| --- | --- |
| Capture | Read detached capture work, advance framing under explicit byte/frame budgets, then commit the exact capture checkpoint. Source readers close before detached framing. |
| Read validation source | Select the validating journal's current attempt/directory/version and check its captured segment. Incomplete capture does not produce ready validation work. |
| Epoch scan | Advance or verify the completed private epoch boundary preparation. |
| Topology capture | Advance or verify private topology preparation after epoch readiness. |
| Epoch union | Advance or verify the union after its prerequisite preparations. |
| Epoch rank | Advance or verify finite rank preparation after the union. |
| Replay binding | Check or publish the exact source/catalog-bound replay binding. |
| Native owner | Reserve the source's slot, open/adopt it if needed, then advance the retained Native session. |
| Terminal publication | If the owner returns an outcome, publish the journal's sealed/failed state under a fresh exact source fence. |

The completed warm preparation paths still execute their `_begin`/metadata checks before returning readiness. They do not automatically rerun the entire preparation: epoch scan returns at `upload_journal_epochs.py:428–429`; topology at `upload_journal_topology.py:284–285`; union at `upload_journal_union.py:764–765`; rank at `upload_journal_rank.py:673–674`. `prepare_upload_journal_replay` still obtains `rank._begin` and calls its idempotent binding commit for a complete source (`upload_journal_replay.py:73–87`). Each `_begin` and its auxiliary row checks remain part of the authority contract; a previously true ready result cannot simply be cached.

The budgets have different owners. Native defaults are rows 64, source octets 65,536, and an independent work-byte allowance of `4 * PROVENANCE_JOURNAL_ENTRY_BYTES_MAX + 65536` (`journal_session.py:66–75`). Union permits up to four empty phase transitions in one bounded turn (`upload_journal_union.py:756–773`). Rank permits up to 64 vertex/edge items and sixteen phase transitions (`upload_journal_rank.py:664–682`). These are not the fixture's F/C/K/Q geometry, nor a promise to complete every phase in one call.

`RuntimeConfig.for_testing` supplies test secrets/workspace placement and leaves concurrency at its ordinary class default unless overridden (`runtime_config.py:131–132,165–183`). The actual publication `_container` overrides database location, scrypt factor and workspace, not journal concurrency or Native budgets (`test_operation_lifecycle_api.py:129–151`).

## Fresh fences and commit order

The source fence matches collection/journal, validating state, declared and accepted bytes/digest, capture attempt and directory, algorithm, phase/version, framing cursor, and failure (`collection_uploads.py:7958–7979`). A result from an old selected source is not authority for a replacement.

The job identity adds workspace, source binding, certificate binding and worker epoch (`upload_journal_validation_jobs.py:371–380`). Opening performs source verification, certificate construction and exact job selection, then obtains the original workspace lease **before** claiming a new worker epoch (`upload_journal_validation_owner.py:1955–2004`). A missing accepted workspace is not recreated as fresh validation.

`claim_worker` is conditional on source currentness, job identity, prior version/progress and active/opening state (`upload_journal_validation_jobs.py:433–460`). The workspace lease uses nonblocking kernel locks; failure yields no workspace rather than authorizing a second owner (`journal_validation_workspace.py:400–479`; its selected workspace lease is held by the original receiving frame).

Worker currentness and publication are different predicates: `worker_current` checks the original job identity/epoch and permitted state, intentionally without comparing its old version/progress (`upload_journal_validation_jobs.py:463–471`). The owner separately keeps the newly published `slot.job` for each later version/progress compare-and-set. Combining these checks as if they were duplicate reads could reject ordinary progress or weaken stale-worker rejection.

`_OwnedSource.verify_current` first checks the worker and source, then the frozen certificate manifest (`upload_journal_validation_owner.py:417–438`). `UploadJournalValidationSource.verify_current` performs a metadata probe and checks the retained replay binding; it does not read the full journal (`upload_journal_source.py:378–405`). The source port contract explicitly forbids currentness verification from scanning source contents (`journal_session.py:45–53`). Source ranges can reuse one captured byte page, while fresh authority checks still precede access.

Certificate currentness already reuses its source/manifest-bound SQL statement structure. Each call executes that statement in a fresh read snapshot (`upload_journal_certificates.py:735–751`). Construction reuse is not successful-currentness reuse.

After Native returns from `advance`, Core publishes detached progress under source and worker/job version/progress fences (`upload_journal_validation_owner.py:2054–2089`; `upload_journal_validation_jobs.py:474–544`). `publish_progress` rejects backward progress or a changed hash/octet count for an unchanged accepted frame count, and successful completion requires complete Native progress. Native commit therefore precedes catalog adoption; an interrupted catalog publication can be reconciled without discarding already committed Native work.

Final journal publication is another boundary: it checks terminal anchor shape, current replay binding, captured segment, and source fence before an atomic state transition (`collection_uploads.py:8165–8226`). It is outside `JournalValidationSession.advance`; its elapsed time belongs in the enclosing Core turn, not a second addition to Native inclusive time.

## Independent work and serial dependencies

| Boundary | What can proceed independently | What remains serialized or charged |
| --- | --- | --- |
| Journal slots | Different exact journal/attempt keys can hold different slots. The pool mutex protects reservation only. | A source's slot lock permits one advancing owner. A busy key or full pool returns no progress immediately. |
| Scheduler | Other candidate journals and retirement work can be selected when a candidate cannot advance. | `process_due_provenance_journal_validations(limit=1)` tries candidates sequentially and stops after one progresses. It is not a fan-out executor. |
| Catalog and Native | Native semantic computation runs after detached application readers close; unrelated catalog work can use its own transaction. | Before/after authority checks and conditional progress publication are still required. SQLite transaction conflicts can still occur. |
| Original cleanup | Another admitted journal can proceed while one session has retained result readers or pending cleanup. | The old slot/lease remains charged until original storage/dependents are actually received. Capacity cannot be reclaimed from a completion hint. |
| Archive physical execution | Separate original execution keys can run actual threads within global and per-target grants. | One exact retained key cannot acquire a duplicate invocation. Running workers and unreceived results occupy their slots. |
| This publication fixture | Physical finalization workers can run between its bounded polls. | Member append, journal upload, shared-history assembly, and the three named reader passes are explicitly sequenced by the helper. |

Source locations: owner reservation/release `upload_journal_validation_owner.py:1836–1953,2036–2099`; scheduler `collection_uploads.py:2209–2251`; physical owner `physical_execution.py:140–147,218–278,321–408`.

Archive physical capacity is a separate policy: defaults are four forward workers/two per target and two terminal workers/one per target, plus distinct retained-construction limits (`archive_execution_tuning.py:9–27`). Raising journal concurrency would neither increase those grants nor make one dependent journal phase parallel. Neither configured capacity nor the number of worker constructions gives active-worker occupancy.

Useful maintained controls, inspected only:

- `test_upload_journal_validation_owner.py:73–89`: a capacity pause preserves the same worker epoch and unfinished atom.
- `:121–150`: a second owner cannot acquire the same kernel lease or claim a worker until the first releases it.
- `:237–281`: changed worker/source/version and certificate manifest reject stale progress, including a nongraph unit.
- `:303–368`: repeated bounded scheduler turns preserve one worker per journal, and two exact journals can make progress without reopening their atoms. This tests progress, not simultaneous execution.
- `test_journal_validation_handoff.py:202–271`: an explicitly paused Native advance has zero checked-out catalog readers while an unrelated catalog write proceeds; replacing the source prevents old publication.
- `test_upload_journal_validation_custody.py:292–334`: retained independent readers keep one original slot/lease charged while another existing journal finishes.

## The fixture's HTTP and poll lifetimes

`canonical_history_scale.py:133–138` constructs a TestClient and two `_LocalApi` clients without entering their contexts. `_api` creates a further TestClient for the authenticated requests (`test_operation_lifecycle_api.py:268–290`). `TimeoutNeutralTestClient` forwards requests and closes the client; it does not provide application startup.

The fixture intentionally appends one member at a time (`canonical_history_scale.py:192–199`). It then validates primary history and uploads supplemental journals in sequence (`:204–290`). The SDK upload method immediately loops over `seal_collection_upload_session_provenance_journal` until the returned state leaves `validating` (`client.py:1589–1642`). No sleep appears in that loop.

For this fixture, `_LocalApi.seal_collection_upload_session_provenance_journal` calls `process_due_provenance_journal_validations()` on its current caller before issuing the HTTP request (`test_operation_lifecycle_api.py:262–265`). The synchronous HTTP seal handler calls service seal (`routers/collections.py:598–607`), which itself executes the selected stamped Core unit after its transaction closes (`collection_uploads.py:2086–2120`). These are sequential opportunities; the direct scheduler can select another eligible journal and can make zero progress. Exact historical 399/199 attribution would require operation ancestry and outcome data, not subtraction from aggregate totals.

The 50 ms poll is passed only to `producer.finish` (`canonical_history_scale.py:352–355`). That method sleeps between finalization status requests (`producer.py:744–753`), and custody draining sleeps only when waiting (`:766–788`). With `join_workers=False`, the local status request drives ordinary custody/finalization coordinators without the separate test-owned blocking worker joins (`test_operation_lifecycle_api.py:251–260`). Thus the helper's poll cadence can add receiving latency or generate repeated readiness checks, but it does not stop an already submitted physical worker from running.

Fresh/warm/evicted verification is also a sequential diagnostic protocol (`publication_profile.py:97–112`), with a new reader for fresh/evicted and reuse for warm. Counting these independent verification obligations as runnable publication backlog would confuse workload design with scheduler underutilization.

### Pinned transport mechanism and lifecycle caveat

Installed package metadata agrees with `uv.lock`: Starlette 1.3.1 (`uv.lock:3551–3553`), AnyIO 4.14.2 (`:1272–1274`), FastAPI 0.141.1 (`:1632–1634`), HTTPX 0.28.1 (`:1880–1882`). Local sources inspected:

- `starlette/testclient.py:351–353,421–427`: each HTTP call enters `_portal_factory`; if `self.portal` is absent, it starts a new blocking portal for that call.
- `fastapi/routing.py:344–354` and `starlette/concurrency.py:32–35`: synchronous endpoints run through AnyIO's worker-thread path.
- `anyio/_backends/_asyncio.py:2592–2641`: worker/idle collections are per event-loop context; a worker starts when none is idle and registers `stop` with the root task. This supports transport turnover as a candidate fixed cost. It does not assign an exact lifetime to every historical request or quantify startup CPU.
- `starlette/testclient.py:678–718`: context entry both establishes a persistent portal and starts/waits for ASGI lifespan.
- `riverhog_api/app.py:343–383`: lifespan starts archive, restore, and cache reapers. The archive reaper immediately runs bounded maintenance through `asyncio.to_thread` and an operation lock (`:189–239`). It would introduce autonomous scheduler work absent from the current unentered-client helper.

Therefore a context-managed TestClient change needs an explicit decision about startup, background work, cancellation and closure. It cannot be treated as a free transport-only performance comparison. Reusing a private portal field without an owning lifecycle is not recommended either.

The older available 62211ac recorder method labels a thread-local lifetime when a selected hook first runs; it does not record operating-system thread birth/death, ready queues or overlap intervals. It is method context only, not the missing extended 931f01e observer/raw receipt. The published 428 hook-observed lifetimes must not be promoted to an independently verified thread-creation count.

## Cost attribution and the next narrow questions

Partition future evidence by nested ownership rather than adding inclusive spans:

1. SDK/HTTP setup, dispatch and deliberate poll sleep.
2. Core stamps, completed-prerequisite reads, detached preparation and Core checkpoint publication outside the Native call.
3. Worker/source/certificate callbacks invoked inside Native; count them as nested children of Native inclusive time, not a second independent total.
4. Native checkpoint and semantic work, with its child costs attributed once.
5. Physical database I/O and wait, separately from calling-thread CPU. Aggregate process CPU is not a per-worker utilization measurement.
6. Final catalog progress/outcome adoption and resource receiving.

The public observation's 51,377 catalog execution attempts and 418 raised attempts do not distinguish SQL compile work, snapshot BEGIN, useful reads/writes, contention deferral or unexpected failure. The catalog has an explicit classifier for SQLite BUSY/LOCKED and PostgreSQL 40001/40P01/55P03 (`catalog_db.py:75–81`). Some maintained scheduling deliberately requests a zero-timeout SQLite writer and handles classified contention as no progress (`catalog_db.py:109–131`; `collection_uploads.py:3532–3547,3559–3564`). These demonstrate possible control paths, not the causes of the historical 418 attempts. Capture exception class, original driver code/SQLSTATE, owner/call site, and propagated versus handled disposition before assigning that total.

Narrow next questions justified by the source:

- For a few ordinary tiny Native turns, which phase and accepted checkpoint changed, how much of each explicit budget was consumed, and which fixed source/control checks occurred? This separates one-child scheduling granularity from actual semantic rows and from any repeated input scans.
- On a completed warm Core unit, which prerequisite checks revisit only fixed metadata and which genuinely fetch an auxiliary directory/page? Query-structure reuse may be investigated separately from freshness; successful-read or ready-result caching is not authorized by this analysis.
- Does a transport-lifetime-only control, with explicitly equivalent startup/background policy, reduce per-request fixed work? This requires preserving actual endpoint transitions and cleanup rather than starting background workers incidentally.
- At an alleged missed parallelism interval, were at least two independent exact keys ready, did they have sufficient resource grants, and why was each unstarted: same-key busy, capacity, target capacity, live lease, retained reader/cleanup, delayed poll, or dependency? Record worker start/end/join/receive intervals and ready-key counts before recommending a concurrency change.

A bounded diagnostic can answer those questions without new publication or a caller census. Current source analysis does not justify changing concurrency policy, skipping a fence, removing a checkpoint, or treating the small fixture as a utilization test.

## Principal unchanged files checked against 931f01e

`riverhog/src/riverhog_core/services/collection_uploads.py`; `upload_journal_validation_owner.py`; `upload_journal_validation_jobs.py`; `upload_journal_source.py`; `upload_journal_certificates.py`; `tests/support/qualification/canonical_history_scale.py`; `tests/support/qualification/publication_profile.py`; `tests/unit/test_operation_lifecycle_api.py`; `packages/riverhog-client/src/riverhog_client/client.py`; `packages/riverhog-client/src/riverhog_client/producer.py`.
