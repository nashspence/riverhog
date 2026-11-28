# One hint after committed queue eligibility

## Exact boundary

The API payload-unit route awaits the Core `upload_unit` call. That method obtains a sealed receipt, commits `_record_sealed_volume`, reads the returned unit state, and invokes `_schedule_finalization_if_ready` before returning. `complete` also invokes the same point-scheduling method after closing the plan.

These are distinct facts:

| Boundary | What it establishes |
| --- | --- |
| Last sealed-receipt transaction | The remaining payload/custody facts can now satisfy readiness, if the plan and all other required facts are complete. |
| Successful point-readiness conditional UPDATE, commit and session close | The service has accepted due-now `finalization_queued` work under the selected facts and current write gate. This is the hint boundary. |
| Later bounded due selection and exact original registration | The current worker/claim owner independently decides whether work may be admitted now. |
| Physical worker exit, join and semantic receiving | The original outcome is received. A hint or a published Future does not establish this. |

`_ready_for_finalization` requires a closed nonempty planner, exact sealed artifact count and bytes, and the exact sealed-volume count. The point UPDATE additionally matches the selected planner checkpoint/sequence, artifact and sealed totals, eligible upload state, and the current open gate epoch. The proposal inspects that existing UPDATE's row count. It does not add a query.

A zero-row UPDATE emits no hint. Rollback, failed result close, or unconfirmed transaction/session exit emits no hint. A commit with a lost acknowledgement can leave durable queued work without a hint; existing timer/startup selection handles it. A notification exception after successful exit is logged without changing the already committed operation into a reported failure.

## Smallest owner

The collection-upload service exposes a scoped internal `archive_maintenance_wakeup(callback)` binding. It permits one live receiver and detaches its own callback on scope exit. Its default is no receiver, preserving direct service use. Core has no dependency on asyncio or API implementation.

The API reaper owns `_ArchiveMaintenanceWake`: one pending bit, a thread lock, one asyncio event, and the current event loop. At most one callback is scheduled per pending-hint interval; notifications before consumption coalesce. A consumed hint's callback can still be queued when a later interval begins, so the design does not claim a single outstanding callback across all intervals. It retains no per-collection map, per-notification work queue or durable state. The lock covers only these constant-size local state changes and callback submission; it never covers catalog, provider, worker or callback execution.

The reaper binds the actual service returned by its provider, covering eager and supplied-provider application modes. When that owner changes, it detaches the old binding, clears the remembered owner before attempting the new binding, and forces an ordinary sweep. If a new binding is rejected, returning to the original owner binds it again. Another live receiver is not overwritten.

The notification hook is deliberately narrow. Batch readiness inside an active sweep already reports progress and drives the next turn. Retained finalizer continuation has its own retry cadence. Other external mutations, other processes and uninstrumented owners retain timer discovery. This proposal does not promise a notification for every repository operation.

## Wake, wait and lifecycle

| Case | Behavior |
| --- | --- |
| Notify before wait registration | The pending bit/event preserves the hint. |
| Notify while waiting | Thread-safe loop delivery wakes the reaper. |
| Notify during a sweep | Consumption happened before work; the new hint remains pending for the next turn. |
| Notify between consumption and another wait | The new pending bit is retained; it is not cleared after work. |
| Duplicate burst | One pending hint; notifications before consumption schedule only one delivery callback. |
| Delivery callback arrives after its hint was consumed | It checks current pending/closed state and cannot resurrect the consumed hint. |
| Lost, stale or unavailable hint | Ordinary durable selectors and timer remain authoritative. A stale hint can cause one empty full sweep. |
| Startup | Existing unconditional startup-recovery sweep remains. Work accepted before binding is discoverable there. |
| Shutdown | Reaper cancellation closes the wake before detaching its binding; queued or late callbacks become harmless. Closing the wake does not release workers or acknowledge durable work. |

Startup recovery retains its existing once-per-reaper-task behavior. Changing a supplied container causes a fresh ordinary sweep, not a new per-container restart protocol. The helper's close operation is used after the owning wait has been cancelled; it is not a general cross-task cancellation service.

## What a wake does, and what it costs

A consumed hint forces `_process_archive_maintenance(..., execution_only=False)`. Merely interrupting sleep while leaving the future full-sweep deadline authoritative would choose the receiving-only branch and omit other due owners.

The existing full sweep makes 15 steady-state owner calls: registration; journal validation; custody receipts; catalog history reap; finalization; expired custody transfers; workflow claims; disposition sets; outcome sets; deletion; cache accounting; archive copies; descriptions; tags; and lifecycle contexts. Each retains its existing batch/control limits. Startup retains its additional existing recovery calls.

Those calls are real cost. The new hint adds no catalog query itself, but can bring a full sweep forward. A stale hint can therefore cause one additional empty sweep. The measured 1,024-hint burst produces one additional sweep, not 1,024. Hints arriving after consumption may legitimately request subsequent passes. Emitting a hint only for an accepted fresh readiness transition avoids waking on all payload units, hundreds of journal seal/status requests, every stage return or every physical result publication.

Without a readiness hint, live originals still receive the existing short execution-only polling cadence. Progress still causes immediate bounded follow-up. The timer interval and retry deadlines are unchanged. Every full wake also gives unrelated owners their existing ordinary opportunity; it does not put the triggering collection at the front of a new queue.

## Authority, admission and actual receiving

`process_due_finalizations` first receives existing originals. Its due selector keeps indexed bounded selection and due timestamps. `_submit_finalization_if_due` and `ArchiveFinalizerWork` still check the selected parent, current control and due claim under their existing locks, register the exact original, and resolve its transaction. `OriginalFinalizationAdmission` waits for that resolution and checks the exact durable claim/open gate before child work. Semantic children retain their own checks.

No physical execution code changes. Forward and reserved terminal capacities, per-target limits, registry contention, original identities, `resume_after`, and retained acknowledgement handling are unchanged. The measured parent wait does not establish spare CPU or justify more parallelism.

The actual activation test holds the real first finalizer thread alive after its Future has a result. Another wake and ordinary sweep cannot remove the original slot. After actual exit, the existing receiver calls the original thread's `join(timeout=0)`, receives that same Future and removes the slot. Existing claim-registration rollback/lost-reply, retry-cadence, terminal-headroom and unrelated-construction proofs also pass.

## Alternatives rejected for this slice

- **Globally shorter polling:** spends additional catalog/control work whenever idle and changes unrelated cadence. The direct owned hint addresses the observed transition.
- **Wake on an HTTP completion response:** that response can precede payload readiness; internal readiness also has non-HTTP paths.
- **Wake before commit or after a merely positive predicate:** a sweep can miss uncommitted work, and the conditional UPDATE can lose a currentness/gate race.
- **Execute directly from notification:** bypasses independent selection, admission and original receiving.
- **Notify on every stage/Future result:** expands the scope and can multiply full sweeps; `Future.done` does not prove physical receiving.
- **Durable scheduler or generic subscription registry:** duplicates authority and machinery that this finite gap does not require.
