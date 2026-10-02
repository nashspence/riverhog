# Integration-agent reading map

This is proposed concrete input for #948 under #903, not an instruction to merge.
Read the current issue, reconcile against current main, and use the normal
integration rail. Do not copy `.reference` material into production as documentation
or treat the miniature model as a shared production kernel.

## Audited source anchors

All links below pin `239985271c9ec24942073f4005dd18da621d0251`. Read ranges are
stated to avoid claiming a full repository audit. Blob hashes cover the corresponding
whole file; matching a hash does not imply every line was reviewed.

| Source | Read anchors | Git blob |
| --- | --- | --- |
| [AGENTS.md](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/AGENTS.md) | Complete; boundaries, baseline and integration policy | `6c2ad90111bc6f58c5fc82e25700beffb028e9d9` |
| [scheduler.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/scheduler.py) | Complete; sequential step loop and admission/departure lanes | `473e33195859e6063d9709fba8cb6b173f1c1a4a` |
| [coordinator.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/coordinator.py) | 384-635; observation, nested planning, preflight and target dispatch/poll functions also read in prior inspection; whole-file blob unchanged | `fbb76a78d6b0a0bb5300b1f94c9cc093a6d93fa0` |
| [work_state.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/work_state.py) | 1524-1619; bind request before dispatch, target status mapping | `00415744bc03c2114bc37e751a55f3232ac99fea` |
| [persistent.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/packages/target-support/src/stove0_target_support/persistent.py) | 1-340, 450-752; accept, submit, cancel, recover, durable writes | `4e0a8b25e0769b61b10757224787a57b438a72a7` |
| [observer http_binding.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/packages/observer-support/src/stove0_observer_support/http_binding.py) | Complete; synchronous handler and blocking semaphore | `c7f11b1bca58a2a2e19b1dd97ce449b324eec018` |
| [protocol models.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/packages/protocol/src/stove0_protocol/models.py) | 550-735; semantic request versus fence-bound invocation, terminal evidence | `aede32aa95034a7abfaa614266c6ee8d7c658cca` |
| [preview.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/preview.py) | Complete; future waits, nested traversal, claim finally | `14e2338f0a71b75070c7fbc5b52f61bc5ad3a374` |
| [admission.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/admission.py) | 570-760; durable candidate and synchronous preview | `a3427c12f728fd55220f49c7c140b4022671ecf8` |
| [departure.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/application/server/src/stove0_core/departure.py) | 535-632; durable pending effect, synchronous receipt and retry | `3eaddeaef02e0245c1760051f0a772e1926c4995` |
| [NVENC common.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/common.py) | Complete; process wait, cancellation, timeout, version probe | `a06795b0f5fea67168c85cdab2745c6fa1bfd9bf` |
| [test_departure_target.py](https://github.com/nashspence/riverhog/blob/239985271c9ec24942073f4005dd18da621d0251/some-implementations/stove0/packages/target-support/tests/test_departure_target.py) | Complete; exact intent and lost-response receipt replay | `90e27865fa9c29ef340dc6c7ba5f23d3ac29d80b` |

#903's body and its process-correction comment were read. #948's body was read;
there were no comments when this audit began. The branch does not infer acceptance
of earlier conversation-level API sketches from that absence.

## Suggested change sequence

1. Establish the control-call and lifecycle invariants in focused tests first.
   Preserve typed terminal evidence and clarify metadata/preflight's bounded role.
2. In target support, implement dormant durable queueing, bounded dispatch capacity,
   admission injection, and started-versus-unstarted recovery/cancellation. Include
   partial acceptance writes and repeated queued PUTs, not just the happy path.
3. Add observer runtime envelopes and support wrapping the existing observer
   implementation boundary. Update client, binding, schema/conformance surfaces,
   exact invocation identity, freshness refresh, and cancellation propagation together.
4. Convert main and nested observation advancement plus preview/admission consumers
   to durable resumable steps. Cover operator and evaluation callers sharing preview;
   do not leave a hidden synchronous fallback or abandon a live preview claim.
5. Add pollable departure-effect delivery without losing artifact-free identity,
   durable receipt idempotency, uncertain-effect reconciliation, or policy intent.
6. Apply shared due scheduling, authority maintenance, bounded backlog admission,
   observability and component deployment configuration. Wire a fake external lease
   into the actual NVENC target for an end-to-end witness before claiming completion.

These are integration suggestions, not new child issues, priorities, or accepted
scope. A support abstraction is justified by shared lifecycle mechanics; it does
not authorize a cross-application database or imports between implementation packages.

## Regression evidence to retain or extend

Reconcile the existing target-support tests (including publication/checkpoint and
shutdown behavior), observer-support tests, coordinator/work-state tests,
preview/evaluation tests, departure tests, and relevant PostgreSQL concurrency tests.
The supplied reference does not run those suites. The inspected departure test's
lost-response scenario must continue to produce one exact receipt, not a duplicate
effect, after transport becomes pollable.

Update the appropriate package dependencies/exports, protocol schemas, HTTP error
bindings, API clients, CLI/operator views, distribution manifests, state baselines,
fixtures and generated contract closure using repository-owned generation/validation.
Follow the current pre-v1 baseline policy rather than adding legacy aliases by
assumption. Do not regenerate the frozen contract candidate on this reference branch.

## Evidence matrix (proposed checks, not authoritative acceptance)

| Property | Reference evidence | Production integration still needed |
| --- | --- | --- |
| Acceptance precedes execution and survives reopening | SQLite acceptance/reopen tests | Real outbox/target crash windows and actual storage ownership |
| Indeterminate NVENC lease wait does not consume workers | 500 denied probes; later grant; unrelated work progress | Actual NVENC + external lease service + worker/process inspection |
| Local capacity before external lease; bounded probes | Reservation, deadline, stale-grant tests | Transport deadlines and prevention of orphaned probe threads |
| Idempotent submit/poll and immutable terminal evidence | Repeated accepts, one start, lost-result replay | HTTP retry/status validation, multi-scheduler and process races |
| Cancel while queued/probing/running | Explicit interleaving tests | Actual cancellation route, process-tree teardown and work cleanup |
| Recovery distinguishes queued from may-have-started | All four kinds; uncertain effects do not replay | Atomic storage, process containment, existing checkpoint resumption |
| Fresh authority and claim-generation identity | Expiry and identity separation tests | Real capabilities/callback refresh, stale fences, authorization checks |
| Backlog bounded before acceptance | Refusal/deduplication test | Queue byte/row limits, retention/deduplication, load and fairness |
| Preview/nested/automatic admission does not wait | Design and audited call graph only | Durable continuation, restart, read-only scope, result-order tests |
| Poll backoff never starves claim maintenance | Design only | Due indexes, independent renewal, long waits and restart tests |
| Lease loss cannot release a still-running consumer | Model requires explicit stop proof | Real broker fencing/renewal, orphaned children, kill/crash qualification |

The model's `workers_stopped=True` and authority expiry are **test inputs**, not
an implementation of proof, capability validation, or device fencing. `complete`
in the model carries an opaque fixture string, not a Riverhog result schema. The
single-owner SQLite model does not demonstrate distributed CAS or actual concurrency.

## Questions to settle in the owning issue

The publication comment summarizes these so they are not stranded only in a branch:
confirm bounded metadata/preflight is permitted; confirm resumable preview and
artifact-free departure delivery belong in the consistent boundary; decide whether
any independent sampler API enters scope. Choose concrete poll/control budgets,
backlog/retention policy and permit-supervision responsibilities in normal integration.
The reference recommends those invariants but does not establish those decisions.
