# Two-key readiness and occupancy: fixture feasibility

## Decision and scope

**A legal maintained tiny two-key setup exists. It can plausibly reach both real Native owners in a few ordinary Core scheduler invocations, without injecting completed preparation or bypassing guards. That does not make a short synchronous Core trace evidence of avoidable serialization.** The missing conjunction is a contemporaneously fresh-ready and admissible unselected key, an authorized executor with usable idle capacity, and a causal dispatch delay between them. Core's two retained slots are not a two-worker executor.

This review therefore recommends **no new product run for the present question**. Four synchronous scheduler invocations could confirm a small prefix already explained by the source, but would not establish the needed waiting/idle overlap. This is a limited negative result: avoidable serialization is **not demonstrated**, not disproved. The fixture is not incapable of creating independent work.

The review is static on exact **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, responding to [#991 comment 6890015223](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890015223). No product/test module was imported; no test collection, fixture, product turn, publication, probe or benchmark ran. No repository file was edited. The companion `ready-occupancy-source-audit.json` pins the inspected sources, guides, tool metadata and exact supplied comment bodies. Any conditional protocol below is an outline, not an executed result or instruction to launch a campaign.

The receiving-owner report of one 36.315-second publication, 49.217-second helper, 51,811 catalog execute attempts and 465 `SQLITE_BUSY` refusals is retained as a **report**. Its raw/profile hashes are identified in the supplied context; the original raw artifact was not re-executed or independently reconciled by this fixture review. The report itself limits those failed-attempt intervals to error-callback entry and leaves subsequent retry/coordination/receiving outside the interval. It does not establish per-key readiness or idle-worker overlap. The earlier 418 errors remain a different historical observation.

## 1. What the maintained two-journal test actually creates

The exact source owner is [`test_scheduler_progresses_two_exact_journals_without_reopening_their_atoms`][two-key]. It constructs a minimal canonical journal for `JOURNAL`, another for a distinct literal journal ID, stages each journal's own exact bytes and digest, and begins ordinary upload capture. It then calls `process_due_provenance_journal_validations(limit=1)` repeatedly. The existing test drives both to terminal with a 1,000-iteration guard and checks two completed jobs at worker epoch one. **Running that entire test is outside the requested small-prefix scope.** Its source is useful as the setup recipe.

The components of that recipe have distinct authority:

| Maintained component | What it supplies | What it does not supply |
| --- | --- | --- |
| `source(journal_id=...)` | A fresh `BytesSource(b"native summary")` observation and one fully authored canonical `journal_init` frame | No Core capture/preparation/job state; no earlier Core acceptance |
| `retained(root, raw)` and `staged_transport` | Initialized disposable catalog, upload/write-control fixture, exact staged transport and beginning capture for the first journal | No completed epoch/topology/union/rank/replay state |
| The second-journal block in the owning test | A distinct Journal/Chunk key with its own bytes, digest and capture attempt | No clone of the first journal's workspace or readiness marker |
| `scheduler_for(factory)` | A real service with ordinary test configuration and an open empty planner checkpoint suitable for calling its scheduler | No executor, accepted Native result, or skipped preparation |
| Ordinary scheduler calls | Fresh source selection, preparation, owner admission, one Native call when eligible, catalog adoption and finalization opportunity | No permission to call a private Native loop or reuse a previous readiness boolean |

The setup deliberately seeds staged transport using the maintained unit fixture. It is not a full SDK upload/publication and must not be reported as one. After that fixture boundary, capture and every preparation transition run through the actual Core route. Calling `_begin_upload_journal_capture` at setup does not write completed preparation or Native semantic acceptance. [Staged transport][transport], [capture fixture][capture-fixture], [minimal source][source], [scheduler fixture][scheduler-fixture]

The two journals are independent in the relevant data sense: different journal keys and newly authored self-contained initialization documents, with no `forked_from` or cross-journal endpoint supplied. They still share the catalog, collection, writer admission and service resources. Independence of journal input does not imply independent database writers or execution threads. Their distinct actual capture attempts and workspace/job identities would have to be recorded if the outline were ever executed.

### The default source is genuinely small, but setup is still work

The default observation graph has one agent, artifact, occurrence, state, activity and description. `BytesSource` supplies no source context, locators or additional supporting assertions; the helper adds an extension only when its optional nonzero `text_octets` argument is used. With the default argument there are no `relations` or `retracts`. `create_journal` emits one initialization frame and invokes maintained complete Native validation before returning bytes. That builder validation and its resource lifetime belong to setup; they cannot disappear from a workload cap or record merely because they are not Core owner turns. [Observation graph][graph], [BytesSource][bytes-source], [journal authoring][author]

The exact generated byte length is not measured here. Any later small-prefix outline must check the actual retained byte width and frame count after generation. The source-derived first-turn argument below is conditional on one valid frame fitting the ordinary 65,536-byte capture window, not an invented measured byte count.

## 2. Conditional ordinary turn count

The service unit proceeds in one invocation through capture, selected-source read, epoch scan, topology, union, rank, replay binding and finally one retained owner call; it returns early if any stage is not ready. Nothing in the outline would patch a return value or call a preparation helper to skip this sequence. [Core service unit][unit]

For the default one-frame, zero-topology-edge input, with no source conflict, retirement backlog, capacity refusal or provider failure:

| Actual stage | Source-derived reason it can finish in the first Core unit |
| --- | --- |
| Capture | One frame within the ordinary byte/frame windows can complete its exact scan and commit. The ordinary frame budget is 64. |
| Epoch scan | One initial frame fits one actual boundary batch; the completed offset must equal source bytes. Fresh retirement checking finds no retired attempt in a new fixture. |
| Topology | Planning sees absent `relations` and `retracts`; both cursors have zero items. Advancing them completes this frame without fabricated facts. |
| Union | The initial frame phase creates the two graph kinds; the empty add phase reaches the sole frame's end and completes them. Two empty transitions fit the existing maximum of four. |
| Rank | Each of the two empty graph kinds requires graph, empty seed and empty pop/finish transitions. Six transitions fit the existing maximum of sixteen, using zero vertex/edge rows. |
| Replay | The ordinary rank/source-bound replay binding is prepared freshly after completed rank. |
| Retained owner | The existing owner opens/adopts as required, then attempts at most one unchanged Native advance under its ordinary budget. |

These are **conditional source deductions**, not observed call counts or a time bound. The corresponding implementations retain their fresh predicates, CAS and transaction boundaries. [Epoch/cleanup][epoch] [cleanup], [topology planning][topology-facts] [topology advance][topology-advance], [Core topology][topology], [union phases][union-phases] [union-loop], [rank phases][rank-phases] [rank-loop], [replay][replay]

Under that successful-first-candidate path, the maintained scheduler's rotation can select A, B, A, B across four `limit=1` invocations. The first two calls can therefore enter different real Native owners; the next two would repeat fresh eligibility and execute another ordinary phase for each. It is neither necessary nor appropriate to finish their full Native journals to observe those entries. Actual outcomes must confirm the path; any mismatch remains an observed refusal, setup limitation or partial trace, not permission to expand the workload.

### Scheduler invocations are not actual Core or Native call counts

The outline admits at most **four scheduler invocations**. It must not report exactly four Core/Native calls by construction. Each scheduler iteration can attempt multiple candidates that return unchanged or refuse admission before finding progress. Those attempts, stamps, guards, failures and child work are real work.

With retained-slot capacity `C=2` and four scheduler invocations of `limit=1`, the existing general candidate envelope is `4(2C+1) = 20` attempted stamped Core candidates, each with at most one owner/Native call. A tighter bound of eight is available only after proving this fixture's complete candidate universe contains exactly its two keys, no replacement attempts, and no other producer or lifespan worker: warm keys and fresh selectors exclude each other, so at most two distinct candidates are attempted in one iteration. The conditional A/B/A/B success path has four candidate and Native calls; **all these bounds and conditional counts are distinct from an actual observation**. [Scheduler][scheduler], [fresh-key exclusions][selector], [reservation and active keys][reserve]

The ordinary Native child budget remains 64 row credits, 33,619,968 work octets and 65,536 source-window octets. No aggregated budget, concurrency setting or turn loop is introduced. Each actual owner call preserves per-step catalog adoption. Scheduler cleanup/retirement, cursor rotation and `_schedule_finalization_if_ready` remain active. The fixture's open empty planner naturally makes finalization readiness false; the observer must still record the actual call and outcome rather than patch or omit it. [Budget][budget], [finalization opportunity][finalization], [readiness predicate][finalization-ready]

## 3. Why this is not the requested waiting/idle-worker demonstration

A passive candidate row or `active_keys()` result is insufficient to call a key ready. Completed preparation predicates check exact retained links and captured bytes, in addition to source/worker/certificate metadata. Existing corruption tests show that unchanged manifests do not substitute for these dependent-row checks. Passing those checks on an earlier turn is not a perpetual authority token. [Core unit][unit], [rank corruption owner][rank-corruption], [owned source callbacks][owned-source]

The inspected ordinary Core route exposes no passive operation that atomically says “this other key remains ready, all resources are admissible, and an authorized executor is idle” for the interval in which the selected key runs. The existing preparation operations can publish progress and perform cleanup; reusing them as an extra readiness scan would be added product work, not a passive observer. Probing `_reserve` and releasing it would likewise change lock/resource behavior. Actual admission is established by the actual path, at its actual time.

| Observation available from the four-call Core prefix | Legitimate conclusion | Missing fact |
| --- | --- | --- |
| Two distinct keys each pass fresh preparation and enter their owner | Both inputs can be independently advanced through ordinary Core authority | It does not certify the other key's fresh readiness throughout an earlier interval |
| An actual reservation succeeds | The selected key obtained its slot then | It does not prove an unselected key could obtain all resources during a prior interval |
| No checked-out catalog connection at Native entry | This selected boundary released catalog readers | It does not certify writer admission or execution-worker availability for another key |
| A second retained slot exists or is unused | Retained-lifetime capacity has that state | The owner has no corresponding worker queue or executor thread pool |
| A/B/A/B serial entry/exit order | This direct synchronous caller invoked work serially and followed scheduler rotation | It is not evidence that an authorized worker was left idle while freshly eligible work waited |
| A writer refusal during an actual attempt | The named actual writer admission failed, with its real caller/disposition if captured | An idle CPU/thread or spare retained slot does not make that writer admissible |

The scheduler runs candidate calls synchronously on its caller and stops after the first durable progress in one `limit=1` iteration. The retained owner uses a nonblocking slot lock and no journal execution queue or executor. Thus a Core-only prefix has no unclaimed worker interval to correlate merely from its configured capacity of two. Existing resource tests target particular ownership properties, not natural occupancy: the unrelated-catalog test deliberately pauses a Native call using an injected event, while the retained-reader tests deliberately hold readers and often run full fixtures to terminal. Neither should be repurposed as an unmodified passive tiny occupancy measurement. [Owner reservation][reserve], [unrelated catalog test][unrelated], [reader custody tests][readers]

## 4. Do not merge four different opportunity surfaces

| Surface | Maintained behavior | Why the tiny Core fixture does not stand in for it |
| --- | --- | --- |
| Direct ordinary Core scheduler | Synchronous bounded candidate selection, one progressed candidate per admitted outer iteration | It owns retained work and scheduling decisions, but no journal thread executor |
| Publication fixture SDK/coordinator | `_LocalApi.seal` first calls the ordinary Core scheduler, then makes the selected-journal HTTP request; the SDK upload method loops sealing one journal until terminal | An apparent journal-call count includes two different opportunities, and the scheduler opportunity may select another key. Fully staging the publication also carries primary/history dependencies and is not a four-turn setup |
| Selected-journal HTTP endpoint | A synchronous access-checked seal calls the selected Core advance, then returns current journal status | AnyIO request-worker lifetime is not an independent journal-ready queue, and dispatching parallel requests would change the observed caller pattern |
| Entered application lifespan | Starts archive, restore and retrieval-cache maintenance tasks with their own receiving | Entering TestClient would add those workers; it is not a neutral portal-reuse or transport-only change |

The maintained publication helper constructs its `TestClient` objects without entering their lifespan and sequentially appends inputs for deterministic SQLite history work. It uploads supplemental/shared histories through the serial SDK operation and eventually runs full finalization and archive verification. There is no maintained stop-at-two-freshly-admissible-independent-keys hook in that publication fixture. Using it to obtain such a boundary would require a new, explicitly owned partial-fixture/observation design; executing a full publication is expressly outside this request. [Publication setup][publication], [fixture client][local-api], [SDK upload loop][sdk-upload], [selected HTTP][http-seal] [selected-core], [lifespan][lifespan]

This is a specific evidence/observer-boundary gap, not a claim that the system cannot expose independent work or that all observed serialization is necessary. The Core-only fixture establishes a legal small input route. It does not establish occupancy of the SDK coordinator, HTTP worker capacity, application reapers or archive executors.

## 5. Smallest legal partial observation outline, if a later question requires it

No execution is recommended now. If an owner later needs to falsify the narrower claim “the minimal maintained setup reaches two distinct ordinary owners,” the bounded outline is:

1. Begin one approximately 120-second cooperative whole-workload admission cap before constructing either source or initializing the catalog. Retain exact fixture bytes/digests and report source-builder validation as setup work. Use the maintained two-key recipe only, zero unrelated rows and default source size; do not set completed preparation, job progress or Native snapshots.
2. Before dispatch, verify the actual closed candidate universe, one-frame byte extents, ordinary configuration and original resource owners. Reject a mismatching setup as partial without increasing geometry or retry count. This is a diagnostic precondition, not an archive-size policy.
3. Invoke only `process_due_provenance_journal_validations(limit=1)`, at most four times, checking the cooperative deadline before each invocation. Preserve its normal per-candidate calls, readiness, budgets, transactions, adoption and finalization scheduling. Do not call private `_advance` methods as an observer shortcut. Record every actual candidate/child, including zero-progress and failed attempts; the admitted scheduler count is not the actual work count.
4. Preserve exact before/after source/attempt and job/checkpoint identities, observed readiness-stage results, actual reserve refusal/success, worker claim, Native entry/exit, adoption outcome and original execution return/exception. These events establish observations at their boundaries only; they must not fabricate ready-wait or executor-idle intervals.
5. Stop at the admitted prefix even if both journals remain validating. Receive the original service/slots/constructions/leases, every actual reader and SQLite/catalog resource, then close the original fixture generator/engine. Inspect `checkedout()` and actual admission status; do not replace failed receiving with a cleanup assertion. Remove scratch only after original receiving succeeds. Preserve unknown originals and their scratch if it does not.

Use bounded monotonic records with exact parent/child correlation and explicit overflow failure, no sampling/drop-on-overflow. If wrapper inspection is used, record original method outcomes without changing returned objects, exception identity/chaining, generator behavior, scheduling or budgets. Do not record SQL parameters, error messages or deployment identity. Resource and callback metrics retain their owning domains; do not sum overlapping inclusive spans. The measurement protocol must distinguish setup, product observations, observer overhead and receiving.

A cooperative deadline prevents new admissions; it does not interrupt or receive an entered call, and cleanup may finish later. There is no static guarantee that all setup and original receiving complete inside 120 seconds. An overrun or incomplete prefix must remain an honest partial receipt. No current source or small success would turn that into a production timing gate.

This outline would produce only a **partial two-key Core trace**. A demonstration of avoidable serialization still requires an owner-approved opportunity surface exposing contemporaneous fresh readiness/admissibility, actual dispatch queue and authorized executor state, exact worker/future ownership and original receiving. Adding threads, parallel HTTP calls, lifespan tasks or a readiness-mutating probe would change conditions and is not silently included.

## 6. Retained tests and limits

| Existing owner/test | What it contributes to this static assessment | Why it is not a requested new run |
| --- | --- | --- |
| Two-journal scheduler owner test | Exact distinct keys, ordinary scheduler progression, final same-epoch jobs and zero catalog checkouts | Drives both journals to terminal with a 1,000 guard |
| `ready(factory)` / rank / union preparation helpers | Real phase implementations and source-bound certificates | Often hardcode the first key and loop to readiness; a returned earlier result is not fresh authority for an occupancy interval |
| Service caller/unit growth witnesses | Actual observation hierarchy and current one-step composition | Include `advance_fixture` fast-forwarding, unrelated 1,025-row or wide-control cases; cannot be used as the tiny ordinary route |
| Unrelated-catalog-work test | Catalog readers close before Native work; separate writes can progress in its controlled interleaving | Injected pause and terminal-driving loops do not measure natural readiness or worker occupancy |
| Independent-reader custody tests | Original retained slots/leases stay charged, and another existing key can progress under specified conditions | Deliberately altered resource state and terminal-driving fixture scope differ from this passive prefix |

The request leaves grouped stepping, skipped guards, deferred adoption, phase fusion, removed commits, production concurrency changes, publication campaigns, full census and large trials out of scope. This assessment leaves them out. It makes no new G10/G12/G13, whole-cost growth, throughput or timing-acceptance claim.

## Pinned sources

[two-key]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L324-L369
[transport]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_staged_journal_range.py#L42-L120
[capture-fixture]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_capture.py#L49-L65
[source]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_summary_consumers.py#L22-L37
[scheduler-fixture]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_owner.py#L289-L300
[graph]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/observer.py#L73-L245
[bytes-source]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/sources.py#L24-L64
[author]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal.py#L845-L888
[unit]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2122-L2177
[epoch]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_epochs.py#L332-L433
[cleanup]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_cleanup.py#L288-L298
[topology-facts]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/topology_facts.py#L82-L115
[topology]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_topology.py#L166-L289
[union-phases]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_union.py#L302-L358
[union-loop]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_union.py#L619-L773
[rank-phases]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L480-L534
[rank-loop]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_rank.py#L664-L683
[replay]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_replay.py#L73-L87
[scheduler]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2209-L2251
[reserve]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L1836-L1897
[budget]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py#L66-L75
[finalization]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L3618-L3669
[finalization-ready]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7369-L7384
[rank-corruption]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_epoch_rank.py#L306-L351
[owned-source]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py#L402-L438
[unrelated]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_journal_validation_handoff.py#L201-L271
[readers]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_upload_journal_validation_custody.py#L236-L334
[publication]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/support/qualification/canonical_history_scale.py#L96-L358
[local-api]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/tests/unit/test_operation_lifecycle_api.py#L234-L290
[sdk-upload]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-client/src/riverhog_client/client.py#L1589-L1642
[http-seal]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/routers/collections.py#L593-L607
[selected-core]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L2086-L2120
[lifespan]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_api/app.py#L343-L384

[topology-advance]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/packages/riverhog-provenance/src/riverhog_provenance/topology_facts.py#L262-L293
[selector]: https://github.com/nashspence/riverhog/blob/d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e/riverhog/src/riverhog_core/services/collection_uploads.py#L7905-L7928
