# Proposed implementation for #995

This is concrete, non-authoritative input under [#903](https://github.com/nashspence/riverhog/issues/903). [#995](https://github.com/nashspence/riverhog/issues/995) and its parent [#994](https://github.com/nashspence/riverhog/issues/994) control acceptance. The proposal uses the current #979 source, **0f8030b0c0cd53eb33456924f070ae5979ed307e**, whose product files remain unchanged in this reference. Material under `.reference/` is for explicit integration/reproduction, not wholesale copying into main.

## 1. Selected implementation

Introduce the neutral, standard-library-only package `packages/replay-policy/src/replay_policy/`. Use [the executable proposal](implementation/replay_policy.py) for its values and decision function. It has no storage, clocks, authentication, scheduling, history traversal, Core imports or resource-lifetime authority. Configuration wrappers and serialization remain with their existing owners. The separate [accounting model](implementation/accounting_model.py) makes the composition protocol testable; it is an in-memory specification, not a database implementation.

The first product slice is **one real Native → Core/SDK collection publication and independent verification path**, coordinated with #996/#997. The policy alone does not satisfy #995. Reuse the same semantic executor for restart/adaptive, disposable/retained and short/long input. A second legacy executor or a permanently unknown estimate for supported long work is unfinished integration.

| Decision | Selected behavior |
| --- | --- |
| Logical owner | Existing collection construction, rooted in `ArchiveWriteControlRecord`, from initial admission through terminal receiving. |
| Root identity | Collection ID plus exact original construction identity; worker/claim/phase/epoch changes never create a budget. |
| Policy lifetime | Snapshot once at construction admission. Resume/idempotent retry uses that snapshot. Operator changes affect new constructions. |
| First-slice elapsed credit | Core-observed fresh useful elapsed only, conservatively excluding SDK time; overlapping children earn time once. |
| Nested work | Disjoint grants from the root; no independent Native/Core/SDK percentages. |
| Checkpoint publication | Complete durable Native candidate becomes usable only through the fenced root selection transaction. Count once there. |
| Unknown accounting | Hold known grants/count slots and suppress additional optional work until authoritative finite reconciliation; semantics and mandatory work continue. |
| Crash with unknown executed extent | No additional useful credit for the remainder of the construction unless a mandatory frontier proves the new work is beyond all possibly executed prior work. |
| Progress | Volatile processed progress and selected durably resumable progress are separate fields. |
| Retention | Two usable generations by default; pinned predecessors/dependencies remain until positive receiving, within storage admission. |
| State cutover | Current pre-v1 baseline hard cut; preserve upgrade machinery and refuse incompatible existing state without erasing it. |
| Publication scope | Reference branch and issue record only; no merge, release, provider activation or #995 closure. |

### Why this root

`ArchiveWriteControlRecord` is already a collection-owned record that survives upload retirement. `initialize_archive_write_control()` runs in the transaction creating the collection construction. In contrast, `_publish_catalog_collection()` deletes `CollectionUploadRecord`. Putting budget history only on the upload would erase known reservations and cleanup costs at finalization. Keep policy/accounting in **separate operational fields on the surviving control**, outside `construction_json`, protected archive identities, original encryption state and creation/idempotency hashes. The control's current `epoch` fences access; rotation never resets accounting. A closed gate prevents new optional grants and new optional generation selections, but permits reconciliation of already-selected generations and exact original settlement/receiving. [Control model][control] · [initialization/admission][admission] · [construction/publication][uploads]

## 2. Exact policy contract

The single definition owns these defaults:

```json
{
  "recovery_mode": "adaptive",
  "replay_target_seconds": 120,
  "max_optional_overhead_fraction": 0.125,
  "max_optional_checkpoints": null,
  "retained_checkpoint_generations": 2
}
```

Accept only explicit integers/floats for time/fraction, excluding booleans and nonfinite floats; positive target, `0 <= beta < 1`; count unset or a nonnegative integer; retention a positive integer. Reject unknown fields, numeric strings, legacy aliases and null where a required value is expected. Zero count disables every optional checkpoint, including a zero-cost one. Zero fraction admits only a genuinely zero incremental-cost candidate satisfying the remaining predicates. `restart` disables all optional checkpoints, not mandatory evidence, intents, receipts, fencing or receiving.

The helper accepts nonnegative **integer nanoseconds**, with `None` for unknown estimates and separate nonempty provenance labels. Convert measured elapsed with monotonic nanosecond clocks. Round estimated cost/restore upward and saved-work credit downward; target conversion rounds upward. Policy float values use their exact numeric ratio; for example a binary float `0.1` conservatively rounds to 100,000,001 ns. JSON numeric values preserve the selected numeric meaning; no fuzzy epsilon admits an otherwise unaffordable candidate. Integral seconds are exact. This is accounting precision, not a physical timing guarantee.

At every already-admitted safe execution boundary:

1. Perform ready completion or a required anchor first, even at the threshold. There is no redundant final optional checkpoint.
2. Require a dependency-consistent candidate, adaptive mode, available successful-count slot, admitted storage, no conflicting candidate in the domain and a finite cost envelope.
3. If known remaining useful work `R <= C`, finish toward that anchor instead. Unknown remaining work is not zero and does not prevent later promotion.
4. Calculate `S = max(0, gross_replay_saved - restore_load_validation_cost)`. Require all three conditions: `S >= target`, `C <= beta/(1-beta)*S`, and `O+C <= beta/(1-beta)*U`.
5. Obtain an actual owner grant before candidate work. A `ReservationProposal` has no authority, does not allocate storage and does not certify a checkpoint.

The implementation uses equivalent integer products, avoiding division overflow and threshold roundoff. `O` is actual charged optional cost plus held reservations. Successful creations, held potential-success slots, attempts, retained generations and storage bytes remain distinct. The fixed-size policy/clock instrumentation is measured separately; attributable incremental setup and bookkeeping are charged conservatively rather than hidden in mandatory work or another application. Existing overrun debt is a diagnostic derived from charged/held liability; never charge that debt a second time. Lowering a future estimate does not reduce past cost or outstanding grants.

Suppression reasons are operational observations, not archive-validity errors. Completion, mandatory work, live safety checks and cleanup do not stop because optional budget is unavailable. At the default, a serial domain with negligible restore and `C=30s` first satisfies the local cost predicate at `S=210s`; a `C=0.2s` candidate remains gated by 120s. These are arithmetic examples, not measured Riverhog performance.

## 3. Root accounting and publication protocol

Add `recovery_policy_json`, `recovery_accounting_json` and a CAS revision to the existing control. The operational summary contains a format/algorithm identity, stable root binding, credited useful ns, actual charged ns, successful count, next grant sequence, replay-credit certainty and sealed/unknown state. A bounded set of grants contains exact grant/domain/owner/fence IDs, the full remaining cost reservation, count-slot state, selected generation identity and receiving status. Bound grant slots by existing execution/storage admission, not collection size. Use the repository's nonnegative decimal-text convention where a SQL fixed-width lifetime counter would create an artificial total limit.

The resumable frontier belongs to the existing domain job, not the ephemeral accounting grant. Hard-cut `JournalValidationJobRecord.progress_json` to a typed `resume_frontier_json` containing the selected generation/mandatory frontier, exact input/algorithm binding and covered counters; null means no optional resume promise. Update it in the **same catalog transaction** as the control revision/count. Lock control before domain job consistently. Fully receiving/pruning a grant must not remove the job selector, retained generation or dependency pins. Fallback to an older already-published valid generation does not increment creation count or rewind root accounting. The accounting simulation intentionally omits the domain-owned selector/backend.

The helper sees scalar totals; it does not scan the grant set. The root maintains totals transactionally and validates them during bounded recovery. Bound serialized metadata/control capacity explicitly. No per-member replay registry, per-turn telemetry ledger or new accounting service is introduced.

### Protocol

| Boundary | Required action and crash interpretation |
| --- | --- |
| Initial construction | Snapshot policy and zero accounting in the existing construction/control transaction. Repeated idempotent admission reads the same root. Missing/corrupt existing accounting is never replaced with zero. |
| Candidate assessment | Read bounded live counters. Reassess changed estimates at the current revision. No optional directory/database is opened merely to ask the policy. |
| Grant admission | Short root CAS persists the exact grant, cost envelope and one possible-success count slot before construction. Include its encoding/synchronization cost in `C`. A competing CAS loser re-reads and recomputes; it cannot reuse the old decision. |
| Private construction | Under the grant, construct admitted incremental state outside a shared catalog writer. Keep the previous usable generation and its dependencies intact. |
| Dependency persistence | Native returns an opaque prepared generation only after its dependency closure is durable under the qualified backend. This alone does not publish composed success. |
| Root selection | Fresh source/worker/authority fences; one short transaction selects the exact prepared descriptor, increments success once, and records actual cost plus remaining receiving/cleanup reservation. This is the composed publication point. |
| Lost selection reply | Reconcile the same grant/generation ID. Do not create a new ID, increment again or infer nonpublication from timeout. An uncertain slot remains held. |
| Failed/unselected attempt | Preserve measured cost; release the potential-success slot only after authoritative nonpublication and actual receiving. Retain cleanup reservation until the original is received. |
| Terminal publication/cancellation | The surviving control keeps summaries/grants after upload deletion. Close new optional admission while receiving exact originals. Durably accepted user output and fully reclaimed workflow are separate milestones. |

No transaction is claimed across Native files and the catalog: persist files first, select their exact descriptor second. Unselected files are orphans, not checkpoints; selected files must already be usable. A standalone Native invocation uses its own owner-qualified selector with the same contract.

### Settlement and failures

Reservations are replaced, not accumulated twice. If a 10-second reservation settles with 8 seconds of actual work and 2 seconds of cleanup reserved, liability remains 10. If cleanup then takes 3 seconds, final charge is 11. Publication can increment success while cleanup is still held. Cleanup failure cannot retract usable publication or erase cost.

Allocate contiguous grant sequence IDs independently of the control CAS revision, atomically with the active grant. A grant can leave the bounded active set only in the transaction accepting its complete original lifecycle cost and receiving. The issued-sequence high-water mark plus that active set therefore proves that an allocated-but-absent original is already received. A duplicate final receipt returns `already_received` without another charge/count/revision, even with a stale caller revision. Wrong-root, never-issued or noncanonical IDs and partial reporting after complete receiving are rejected. The domain owner first verifies the same immutable original receipt; contradictory new costs cannot be disguised as duplicates. New publication through an absent grant is refused: use the Job-owned frontier/original receipt for reconciliation. No unbounded tombstone map is needed.

Charge every failed attempt, including failed **admission CAS work before a grant exists**, optional-store setup, serialization, hashing, synchronization, contention and attributable teardown. Use the existing bounded retry/admission controls. A failed candidate does not retry at every semantic yield: it needs the owner's retry admission plus changed useful frontier, estimate or resource availability. Never wait merely to earn elapsed credit. Mandatory work is not withheld to repair an optional ratio.

A crash can lose an unrecorded CAS attempt or an actual duration that exceeded its reservation. Absence of a grant does not prove no cost occurred. On an unclean owner restart, seal further optional admission unless exact authoritative receiving/reconciliation establishes a conservative finite summary for every possible attempt. If that cannot be established, **finish this construction without further optional checkpoints**. This is the chosen conservative behavior, not a request for maintainer clarification; already valid checkpoints may still be used.

Record an open cost epoch with the existing mandatory construction admission and close it only after terminal receiving; an interrupted open epoch is evidence that unrecorded optional work may exist. This avoids a separate pre-attempt write merely to remember possible failed CAS work. Restore the newest durable **root** summary and known held grants. Never restore spending/count from the older optional data generation selected for fallback. Unknown execution extent earns no new credit; replay through a known prefix does not earn it again. Known count slots survive an ambiguous publication even if the candidate is missing. A finite successful-count cap cannot be bypassed by retries, phase changes, worker replacement, generation corruption or retention cleanup.

### Elapsed composition

Only the root advances `U`. Use the existing monotonic execution clock and bounded active-worker state to count the union of intervals with actual new useful work. Overlapping children do not sum their durations. Exclude replay, idle polling, backoff and unrelated queues. Conservatively exclude an interval whenever optional work overlaps useful work; this under-credits rather than inventing allowance. Worker scheduling or an outstanding Future alone is not proof of useful execution.

A parent grant may be split into bounded disjoint child subgrants, never reissued in full to each nested layer. The root counts the parent envelope once; child actuals settle into that envelope. Distinct successful generations need distinct root count slots; subdividing execution of one candidate does not create extra publications. The existing scheduler selects ready candidates fairly within its admitted capacity; this proposal adds no scheduler and no global stop-the-world checkpoint.

## 4. Native and Core seam

Current `EntryReplay._commit()` and `JournalValidationSession._move()` fence, encode/publish and commit small transitions. Core `_open()` immediately adopts `current.progress()`, and `UploadJournalValidationOwner.advance()` calls `jobs.publish_progress()` after every Native advance. Eliminating only one side leaves redundant work. [Entry replay][entry] · [session][session] · [Core owner][owner] · [jobs][jobs]

Use the following owner-facing result contract (names are selected for the integration, not claims about existing APIs):

- `advance(budget)` returns a bounded `ExecutionAdvance` with processed progress, yield/completion state, and an optional candidate descriptor. `JournalValidationBudget` retains its existing `rows`, `octets` and `source_octets` limits. Existing owner epoch controls enforce time/cancellation responsiveness; resource admission and private spill enforce memory bounds independently of recovery policy.
- `prepare_generation(grant)` returns `PreparedGeneration` binding exact source/history/catalog/algorithm/semantic state, parent usable frontier, generation ID and all durable dependencies. It cannot select itself for the Core construction.
- `select_generation(prepared, root_revision)` uses the root publication protocol above; `resume_generation(descriptor)` revalidates exact binding and current authority. Neither accepts arbitrary caller-supplied database files as history.
- `receive_generation(original)` reports actual original receiving and attributable cost; it never uses timeout as proof of quiescence.

`#996` owns incremental immutable pages/segments and bounded manifests, restore validation, compaction and retirement. For this shared seam, candidates must not require copying/hash-scanning the entire growing history per checkpoint, and restore chains must have an explicit bounded-growth lifecycle. The helper does not dictate SQLite internals or interpret provenance. This confines backend design to its existing owner while fixing the policy/publication interface completely.

Keep live semantic state in the existing bounded slot; use private scratch/spill when needed. **Zero optional store** does not mean unbounded RAM or no scratch. Do not open `snapshot.sqlite3` merely to support a small workflow's old API. Bounded private transactions can provide rollback without claiming durable recovery; shared catalog writers must not span Native CPU work, source reads or provider waits.

Keep `_OwnedSource.verify_current()` and exact worker/source/certificate checks. Immutable representation reuse is not cached live authorization. Required semantic completion still verifies exact source byte/digest/frame totals, graph/history closure and final result before acceptance. Unknown effects, original prepared history and custody are never regenerated as optional cache data.

## 5. Generations, progress and retention

A selected generation covers the complete consistent semantic state and dependency closure, not just an accepted-frame cursor. Retain original source bytes, selected historical inputs, catalog/algorithm binding and mandatory effect outcomes independently. Missing/corrupt **optional derived** generations are quarantined and may fall back to a valid predecessor or safe replay; corruption of source/mandatory authority remains a real failure. Preserve the original error and report which fallback was used.

Retention is a target number of independently usable generations. Do not prune a predecessor until its replacement is complete and selected, all dependency references are accounted for, and readers/workers are positively received. Retention=1 still keeps the old generation throughout new publication. Pinned extras consume storage admission; no space suppresses optional work rather than deleting needed data. Counting and budget history never shrink when generations are pruned. No local checkpoint promises survival of destruction of its only device.

Hard-cut progress into:

- `processed`: advisory counters and current execution epoch; nullable/unavailable after restart, allowed to move backward with an epoch change.
- `durably_resumable`: selected complete generation/mandatory frontier and covered counters; never inferred from volatile processed work. Rejection of corrupt optional state may select an older valid frontier; report the fallback/epoch explicitly.
- `recovery`: effective immutable policy, actual charged and held reserved cost, successful count/held slots, last bounded suppression reason, estimate provenance, overrun/uncertainty and retained/pinned generation counts.

For exact public reporting, encode cumulative ns and lifetime counts as canonical nonnegative decimal strings in the typed operational report (zero is `"0"`, no leading zeroes), with `null` for unknown values; this avoids JSON/JavaScript precision loss or a hidden lifetime cap. The five public policy settings remain the numeric/enum schema above. Parse these report values through the shared protocol wrapper; CLI may render rounded seconds while preserving exact JSON. Admit control-byte widths explicitly rather than silently truncating.

Keep the ordinary status response bounded and coalesced. Do not list every grant or generation, scan files, read payloads or force checkpoints on status reads. Finalized status can obtain the same recovery summary from the surviving control. Archive receipt contents and validity are unchanged.

## 6. Cost estimates: finite, owned and honest

The audited source has a maintained publication profile and some upload checkpoint timers, but no calibrated cost model for the proposed optional Native generation backend. Do not reuse total old publication latency or fabricate a rate. `tests/support/qualification/publication_profile.py` explicitly measures an in-process HTTP/SQLite/in-memory ciphertext fixture, plus three verification passes; it is not installed recovery evidence. [Profile source][profile]

Implement a fixed-size owner/backend `GenerationCostProfile` alongside the new generation owner. Its keys bind implementation/algorithm version, backend/runtime/filesystem class, cold/warm condition and maintained measurement provenance. Its coefficients cover fixed admission/selection work; incremental encoded/hash bytes; changed persisted segments/pages and bounded dependency metadata; and lifecycle reclamation. Restore-load/validation is a separate estimate.

Use the existing `make profile` path with disjoint timing spans and one independently metered driver per phase: fixed admission/selection/setup; encode/hash bytes; persisted bounded-size segments; dependency metadata rows; and reclaimed segments. Put each operation in exactly one span. For each variable phase, set its coefficient to the ceiling of the maximum observed phase duration divided by its positive driver count across matching samples; set the fixed coefficient to the maximum observed fixed-phase duration. Then `C = 2 * (fixed + encode_rate*encoded_bytes + persist_rate*segments + metadata_rate*rows + reclaim_rate*retired_segments)`, rounded upward in ns. A phase with nonzero cost but zero driver is invalid evidence unless its cost is assigned to the fixed span. A required unmeasured phase is unknown, never zero. Apply the same upper-envelope recipe separately to restore/load/validation. Use half the minimum matching observed recomputation time per bounded semantic work unit for the replay lower estimate, capped by covered fresh useful elapsed. The factor of two and reciprocal replay margin are explicit **proposed estimator engineering margins**, not parent-selected policy defaults, measured confidence bounds or physical guarantees. All rates, segment/semantic-unit geometry and the supported backend/resource domain come from actual owner measurement and validation; no optimization library or subjective fitting choice is required. For replay saving, use the smaller of covered fresh useful elapsed and a matched conservative replay-cost estimate, then subtract the restore upper estimate. Do not credit work outside the candidate's dependency closure or sum parallel CPU durations as elapsed time.

The profile evaluation reads existing bounded counters, not the payload/history. Keep only constant-size recent high-water cost observations per active backend/owner, update estimates upward after overruns, and never rewrite history to reduce charged cost. A changed backend/algorithm or absent admitted dimension produces explicit unknown provenance. Use a compatible installed maintained profile as the fallback; with no compatible profile, suppress optional work and continue safely without a trial snapshot. Unknown size is acceptable when the admitted incremental state has a known finite cost; unknown total workflow length merely leaves `R` unknown.

The actual first product integration must ship and exercise finite profiles for its supported long path. Calibration and coefficients are implementation work for the integration agent, not an open numerical preference for the user. Permanent unknown fallback, clock-only eligibility, or a synthetic profile labelled as measured cannot satisfy #995. Estimate overruns require reporting, upward refinement and bounded retry/backoff; they do not invalidate a correct archive or create a flaky wall-clock CI gate.

## 7. Configuration, authority and coordinated files

Use the existing configuration owners and a single `recovery` object. Server YAML remains selected by `RIVERHOG_CONFIG`; do not introduce five independent environment knobs. Raw parser validation calls `Policy.from_mapping()` before coercion; Pydantic/protocol wrappers derive defaults and constraints from the same definition and preserve finite-value/type checks.

| Source owner | Concrete integration |
| --- | --- |
| `packages/replay-policy/` | Transfer/refine the neutral module, strict tests and public typed values. Add `py.typed`, Apache license/package metadata, bounded dependency declarations and source-owned growth/contract coverage. |
| Root `pyproject.toml`, `uv.lock`, consumer pyprojects | Register the workspace source and only actual consumers. Reconcile discovered wheel/image/release inventories through existing generation/validation; no production registry. |
| `riverhog_core/runtime_config.py` | Frozen `recovery: Policy` in `RuntimeConfig`; default from shared implementation. |
| `riverhog_core/runtime_document.py` and `config.schema.json` | `RecoveryDocument` / parser mapping; regenerate schema; strict mode/numbers/counts/retention. |
| `riverhog_api/deps.py` and service construction | Pass effective runtime policy to the collection owner; no implicit environment read in reusable code. |
| `catalog_archive_write_models.py`, `archive_write_admission.py`, `collection_uploads.py` | Snapshot policy/create accounting in original control transaction; bounded grant/selection/settlement CAS and authorized inspection. Preserve construction identity and crypto preparation. |
| `upload_journal_validation_owner.py`, `upload_journal_validation_jobs.py`, `catalog_journal_validation_models.py` | One root consumer; bounded volatile progress; replace `progress_json` with typed `resume_frontier_json`, selected atomically with the control CAS/count. No old progress alias. |
| Native session/checkpoint/input/store owners | Implement the #996 interfaces above; same semantic path, optional-generation fallback, current fences and original receiving. |
| `packages/riverhog-protocol/src/riverhog_protocol/` | Focused shared operational policy/progress/grant wire values; no archive/provenance format changes. |
| API `schemas/collections.py`, `_upload_payload`, `_finalized_payload` | Exact typed effective-policy/recovery inspection and separate progress; bounded existing authorized read snapshots. |
| SDK `producer.py`, `_prepared_primary_session.py`, client create/get session methods | Carry server root binding and granted context; preserve mandatory prepared-primary state. Never instantiate a fresh percentage budget inside a nested helper. |
| CLI `main.py`, `output.py` | `upload show` renders processed/durable/uncertain separately. Add an application-owned JSON `--recovery-policy` input following existing structured policy inputs; help explicitly limits it to local application optional work. |
| Current state baselines, parser/API/CLI/generated contracts/examples | Update together, using existing discovery and generation. Preserve protected archive identities and supported state-upgrade tests. |

For the first publication slice Core is the budget root. SDK optional allowance is zero unless Core has issued a disjoint authenticated grant; mandatory SDK preparation remains enabled. Ignore caller-claimed useful elapsed. An application's stricter local mode/fraction/count/storage policy can further suppress its own work but cannot enlarge a server grant or alter server settings. Server work obeys the server's snapshotted policy independently. A standalone application owns a root only for an independent top-level workflow, never implicitly within an attached construction. Remote unused grant refunds require positive original receiving; a timeout or unverified zero-cost claim is insufficient.

Stove0, FTP and other #998/#999 application owners consume the same contract through their existing settings, but their complete implementation is not falsely included in this first slice. The owning issue records those scope relationships; no new repository work queue is added.

For existing operator state, initialization stays explicit. Replace the current pre-v1 schema through the existing baseline mechanism, keep supported upgrade test machinery, and refuse incompatible unresolved constructions with their data intact. Default operator disposition is **drain/settle existing work with its original executable before the cutover**; preserve the previous environment and state until receiving completes. If it cannot drain, preserve it for exact reconciliation and report that concrete blocker. Do not silently initialize it, assume safe cancellation, regenerate original observations, or ship an obsolete compatibility executor. This follows the existing owner/operator boundary rather than requesting speculative permission now.

## 8. Safety obligations and evidence

The proposed classification and minimum retained barriers are recorded in the owning #995 publication comment. The decisive examples are prepared-primary stage/bind loss (#981), original encryption/dispatch intent and late provider writes (#984), exact publication commit/reply loss, source/auth changes and real original receiving. Phase labels and per-step cursor persistence are not substitute safety reasons.

[Qualification instructions](QUALIFICATION.md) give the concrete development sequence, tests, measurement boundaries and stopping rule. The reference tests establish decision/composition behavior only. The integrated slice must additionally demonstrate real cheap-work savings, long-work promotion and restart, finite estimates, installed configuration behavior, growth and exact-source qualification. Preserve #979 WIP, #964 obligations and #994's pause on independent #991 work. No user decision is needed to choose these implementation defaults; later conflicting authoritative issue decisions take precedence.

### External research used narrowly

[SQLite synchronous documentation](https://www.sqlite.org/pragma.html#pragma_synchronous) distinguishes application failure, OS/power failure and backend mode guarantees. Therefore the proposal never weakens the sole retained database and calls a later flush sufficient. [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html) describes assumptions that still need backend/platform qualification. [PostgreSQL asynchronous commit](https://www.postgresql.org/docs/current/wal-async-commit.html) explains why an acknowledged transaction may be lost when external actions rely on it; optional economics does not weaken mandatory acknowledgement. [AWS idempotent API guidance](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/) supports retaining exact intent/parameters and reconciling lost replies. These sources support mechanisms and limits, not Riverhog timing estimates or authority beyond #994.

[control]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/riverhog/src/riverhog_core/catalog_archive_write_models.py
[admission]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/riverhog/src/riverhog_core/services/archive_write_admission.py
[uploads]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/riverhog/src/riverhog_core/services/collection_uploads.py
[entry]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py
[session]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py
[owner]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py
[jobs]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/riverhog/src/riverhog_core/services/upload_journal_validation_jobs.py
[profile]: https://github.com/nashspence/riverhog/blob/0f8030b0c0cd53eb33456924f070ae5979ed307e/tests/support/qualification/publication_profile.py
