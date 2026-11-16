# Native execution and optional generations

**Non-authoritative implementation proposal for [#996](https://github.com/nashspence/riverhog/issues/996), under [#994](https://github.com/nashspence/riverhog/issues/994) and the [#903 reference convention](https://github.com/nashspence/riverhog/issues/903).** Product files on this branch are unchanged. The executable experiments isolate real mechanisms; they are not an integrated implementation or issue-completion evidence.

Audited integration source: `codex/964-session-owner` / [#979](https://github.com/nashspence/riverhog/pull/979), **c28eceaa2dfec1b3712135469ba12230e74f5ffa**. The policy input is the separate [#995 handoff at b545a77996fbc9fe98019796860473bda1bb1aa0](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md), recorded in [its owning issue](https://github.com/nashspence/riverhog/issues/995#issuecomment-6926007041). Reconcile later source and maintainer decisions before reproducing this proposal.

Initial research used `0f8030b0c0cd53eb33456924f070ae5979ed307e`. Before publication, #979 advanced by one commit to the base above. The complete delta changes Python LF checkout attributes and two qualification regression files; Native/Core implementation, package tests, repository policies and locked tools/dependencies are identical. The reference was refreshed to that exact current base and its final focused checks/paired observations rerun there. This does not import or claim #979's pending full CI. The #995 reference remains separately pinned and unchanged.

## 1. Selected implementation

Use **one bounded semantic executor per Native journal/corpus**, with live control state and owner-private disposable SQLite indexes. A yield keeps that state live. Durable recovery is a separate, lazily opened mechanism: an admitted checkpoint freezes a complete semantic generation, stores changed SQLite pages in an immutable authenticated radix map, and offers a descriptor to the owning Core transaction. Core selects that descriptor and the matching accounting outcome together. Every selected generation restores into a new private working database.

The first production slice is the actual small Native → Core/SDK F1 publication and independent-reader path with the shared policy, complete semantics and **zero optional recovery-store initialization**. Then add promotion and recovery to that same executor. Do not defer the first slice until every parent-issue workflow is converted; do not retain a separate durable semantic implementation after conversion.

The following choices are settled in this proposal:

| Question | Selected behavior |
| --- | --- |
| Where does semantic control live? | One private typed conductor, plus bounded scratch tables for variable-size state. No parent/child recovery codec round trip on every phase. |
| What does a yield commit? | Whatever bounded private scratch transaction is needed for semantic consistency; no optional generation and no Core progress adoption are implied. |
| Which policy controls checkpoints? | The common #995 policy and surviving construction accounting root. Native supplies costs/frontiers, never a second threshold or budget. |
| When is optional storage opened? | Only after the owner durably grants an eligible attempt, including setup and the first complete capture. Ordinary scratch is allowed before that. |
| How are generations stored? | One separate owner-private SQLite object store in fixed DELETE/EXTRA mode, immutable page objects, eight-level copy-on-write radix nodes, and hashed generation envelopes. See [STORAGE.md](STORAGE.md). |
| How are changed pages found? | Sole-owner, quiescent scratch WAL; scan only the current bounded WAL interval after grant. Coalesce into disposable bounded spill; no continuous uncharged tracking. |
| How are old generations protected? | The scratch DB and retained store are separate; immutable objects and exact roots remain pinned until known owner outcome and original reader/worker receiving. |
| Can restore require a chain of old deltas? | No. Walk the selected root directly and validate all referenced nodes/pages into fresh scratch. |
| What happens after corrupt optional state? | Preserve the diagnostic, try the bounded retained descriptors newest first, then replay the exact authoritative input. Never convert source, intent, outcome or custody errors into a cache miss. |
| Can a cursor outpace its semantic state? | No. Native control, accepted prefix, source framing, semantic tables, child state and any Core corpus cursor share one generation envelope. |
| What happens to archival journal checkpoints? | Nothing changes: `append_checkpoint()` records are immutable documentary evidence, distinct from optional runtime recovery. |
| What if an ordinary iterator cannot reopen? | Same executor, no optional generation. Do not implicitly spool an unseekable source or manufacture old evidence. A caller may supply the existing exact replayable source contract. |
| Does corruption/fallback permit a new branch in the page store? | No. A resumed executor that falls back below the accepted tip or loses WAL continuity continues semantics with further optional promotion disabled for that live attempt. Existing usable roots remain valid. |

These are engineering choices for review and reproduction, not additional accepted policy. [README](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/README.md), [AGENTS](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md), #994 and #996 remain controlling.

## 2. What the source actually does

The costs are structural, not just an overly frequent filesystem flush:

- `JournalValidationSession.advance()` loads `ValidationLedger`, advances one parent phase/input/child unit, and returns a digest-bearing checkpoint progress value. `_move()` begins, fences, publishes and commits a parent transition. `_finish()` performs real exact-source and terminal checks; these checks remain.
- `EntryReplay` coordinates semantic staging, graph work and finalization through persisted child checkpoints and parent `_commit()` transitions. `_start()` includes `before.checkpoint.to_json()` in admission. Merely putting a larger transaction around this machine retains representation work and violates existing detached-prepare versus owner-transaction contracts.
- `JournalStore` already distinguishes disposable `synchronous=OFF` work from retained DELETE/FULL work. The retained `snapshot.sqlite3` is the mutable state being resumed. Making that one file non-durable between saves cannot preserve the previous recovery generation.
- `UploadJournalValidationOwner._open()` publishes progress immediately after opening Native; `advance()` then adopts progress after every Native unit. `DiscoveryJournalOwner` has the corresponding retained responsibility. These must adopt selected generations or final outcomes rather than every live yield.
- `RetainedCorpusValidator._operation()` restores its checkpoint, begins a transaction, checks currentness, serializes/CAS-writes the new checkpoint and commits for every operation. Even `historical_assertion` and `selected_delivery_support` pass through this wrapper. Moving only journal progress leaves corpus/read paths paying the old cost.

Pinned source: [session](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py), [entry replay](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/_entry_replay.py), [parent ledger](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/_session_checkpoint.py), [store](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py), [corpus workspace](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance/corpus_workspace.py), [upload owner](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py).

`JournalStore.accept()` also exposes a direct semantic executor. The audit found no maintained production call to it in Native source. Remove this obsolete private path and update any tests to the common executor; do not preserve it as an accidental second acceptance algorithm. Confirm that finding against the integration head before removal.

## 3. Private execution cut

### Conductor and transactions

Introduce a private `NativeExecutionState` owned by the existing session/construction. It contains the current parent/child phase, accepted `PrefixState`/SHA continuation, source position, frame ordinal and quarantine reference, graph-run IDs, child progress and completion state. Cache immutable source/catalog/profile/certificate **identity and parsed content** only after the existing admission checks. Do not cache positive current authorization.

Variable-size assertions, maps, graph edges, source-frame bodies and child staging remain in spillable semantic tables. Large frames/rows are referenced from that state rather than copied into every conductor snapshot. The conductor must fit its admitted metadata envelope; exceeding that optional encoding envelope suppresses a checkpoint or spills metadata into the semantic DB, without changing the archive acceptance limit.

Refactor child preparation/application deliberately:

1. Read and admit a bounded source/semantic batch outside a private writer transaction. Complete any source/network/certificate interaction here.
2. Open a bounded owner-private transaction and pass a shared `EpochContext` into child apply functions. Child apply functions no longer begin, commit, encode or reload their own recovery checkpoint. They retain all semantic predicates and indexed mutations.
3. Accumulate the corresponding next conductor in temporary live state. Swap it into the session only after the private DB commit succeeds. Complete post-commit graph/frame/cache updates before marking the epoch quiescent.
4. On rollback or a pre-commit exception, discard the pending conductor together with the rolled-back DB work. On ambiguous commit or failure after commit but before the live swap, discard that disposable executor and replay from a validated retained generation/authority; never guess whether memory matches the database.
5. A call may execute several private transactions, but they share one aggregate admission budget. Before another source read or external wait, finish the current transaction. No Core catalog transaction spans this execution.

Private transaction boundaries and optional checkpoint boundaries are independent. A checkpoint is allowed only between transactions, after all live post-commit updates, with no open statement cursor, child batch, borrowed mutable frame, or pending control swap. Normal progress can be returned at any completed bounded epoch. Completion finishes semantic checks and required outcome handling; it does not create a redundant optional final checkpoint.

The experiment in [native_execution_experiment.py](implementation/native_execution_experiment.py) demonstrates the parent part on actual Native semantics. It is deliberately narrower: child checkpoints and transactions remain. Its transaction success callback guards the live parent swap. Reproduce the principle in owned production code, not by installing its instrumentation/monkeypatches.

### Budget and responsiveness

Today `JournalValidationBudget` contains `rows`, `octets` and `source_octets`; it does not already provide a time or memory budget. Extend the owned contract coherently to include a positive maximum control-transition count and an execution deadline/cancellation observation supplied by the owner. Preserve the existing source/frame limits and resource admission.

Debit one shared quantum for every actually inspected semantic row/edge, newly read source byte, processed bounded representation byte, and control-only transition. An empty source chunk and a phase with no row still debit a transition. Passing a freshly reset budget to every child would turn batching into an unbounded loop. Reusing an already admitted immutable frame does not repeatedly charge its entire source length as fresh work; repeated decoding/scanning that actually occurs is still measured.

Observe cancellation and the monotonic deadline between admitted units. For SQLite work that can consume a long VM interval, use an **owned progress-handler dispatcher in the sqlite-admission connection scope**, with a fixed bounded instruction sampling quantum (initially 1,024 VM instructions). Python exposes a setter but no getter for an arbitrary installed handler; do not overwrite a handler and pretend it can be recovered. Register cancellation/deadline and existing VM-growth observation through that dispatcher from connection creation, preserve each registered observer's actual calls, and unregister the exact cancellation token in `finally`. Update affected observation fixtures together. An interruption rolls back the active private transaction and discards its pending memory state. Installed tests must prove the dispatch covers the actual queries and keeps original connection custody. Timing is cooperative responsiveness control, not a hard wall-clock bound on arbitrary storage calls.

Scratch cache, parser buffers, batch memory, dirty-page spill and snapshot staging all stay within original admission scopes. When an operation cannot admit the next bounded unit, return the established capacity/deferred result without pretending semantic completion. No archive-wide member/history/page ceiling is added.

### Currentness

Keep fresh worker/source/certificate authority checks at quantum admission, every source-port read required by that port, checkpoint offer and selection, result admission/return and any irreversible effect. Check them again after an external read when the current port requires both sides. A live progress getter may perform fixed-metadata currentness checking; it must never replay payload or force a snapshot.

The new epoch counter is a volatile observation sequence, not an authorization fence. Only the owner's current worker epoch/lease and existing source authority can authorize work. A new worker cannot concurrently mutate the old worker's scratch file. Old resources remain charged until the actual worker is joined and original handles are received.

## 4. Complete generation closure

Use one hashed envelope per active semantic database. Keep its variable-sized contents in the page tree, not an expanding list of historical children in JSON.

| Envelope part | Exact coverage |
| --- | --- |
| Identity | Format/algorithm version; operation/construction/workspace identity; journal or corpus role; generation identity and store identity; captured worker epoch; original #995 grant identity. |
| Immutable inputs | Exact journal ID/hash/length and selected history/source manifest roots/counts; catalog/profile configuration digest; graph-certificate binding; mode/require-profiles flags; the exact `expected_anchor` and `require_exact_tail` requirements. An archive root alone does not identify a partially prepared source, and identical bytes under another exact-tail requirement cannot reuse `matched_anchor` control. |
| Native control | Accepted frame/byte/hash prefix and continuation; observed/processed source position; unfinished framing/quarantine reference; phase and child cursors; graph-run IDs; semantic completion state. |
| Semantic database | Immutable root for every live semantic/staging/prefix/frame/graph table, page size/count and database format binding. Source cursor and semantic state cannot be selected independently. |
| Owner continuation | For corpus/discovery, the matching Core phase, source/selection cursors (`after_a`, `after_b`), receipt state and at most one current child source/result descriptor. |
| Dependencies | Exact referenced terminal child/result or mandatory receipt identities; authenticated root/count references for stable manifest inputs. No cached authorization result. |

### Journals

The audited journal has one `JournalStore.db`: its prefix, quarantined frame, preparation/assertion/generic/edge/online-DAG/finish tables are colocated. Additional state is the conductor, parsed current data, `_dag_runs` and input framing. Put the required variable state in that same DB or the bounded control envelope at the quiescent point. Persisting only the old `ValidationCheckpoint` JSON is insufficient.

### Corpus

The retained corpus also has one DB; `_RetainedOrder` places both graph-order workspaces there. Ordinary `CanonicalCorpusValidator` currently allocates separate `_CorpusOrder` DBs. Use the colocated table adapter for ordinary and retained execution, retaining the same order algorithms. This avoids making normal validation a different semantic path or introducing a growing cross-DB snapshot protocol.

The corpus generation covers identities, imported entries/assertions/payloads, source extents/references/forks/selections/fixities/edges, order state, `corpus_source_inputs`/`corpus_selected_inputs`, flags and counts. A completed child frame is imported under the existing savepoint; after its full import, the corpus DB owns the needed prefix facts. For the next unread child frame, reopen the exact bound child summary under fresh authority. If its optional terminal workspace is unavailable, derive/revalidate that exact source; do not skip to a later source or accept a newer Core cursor. Freeze the matching Core corpus continuation into the offered generation envelope before selection.

Restoring a native page image does not validate an arbitrary caller's database. Restore only an owner-selected exact descriptor, verify its complete content-addressed closure and bindings, run the appropriate Native state/SQLite integrity admission, then recheck current source authority. The expected descriptor must come from the owner, not from a manifest found beside a candidate file.

The captured worker epoch is historical selection provenance. A legitimately claimed newer worker may restore that selected generation: verify the captured epoch against the original selected receipt, then authorize the new executor with the fresh current fence. Do not require the old captured epoch to equal the new live worker, rewrite the immutable envelope to update it, or treat that old epoch as continuing permission. Rebind execution-only child ownership in the new disposable state after restore admission; preserve the stable construction/source/algorithm identity and surviving accounting root.

## 5. Policy and Core handshake

Use #995's defaults and accounting unchanged: adaptive mode, 120-second saved-replay target, optional fraction 1/8, unlimited successful-creation count, two usable retained generations. The proposed neutral policy package owns admission; Native does not import Core and does not redefine those values. Configuration and public status remain owned by their existing packages/applications.

The original `ArchiveWriteControlRecord` holds the immutable policy and composed accounting outside `construction_json`. Do not attach the budget solely to `CollectionUploadRecord`, which is retired at publication. Do not reset it on worker changes, retries, validation phases or SDK/Core handoffs. Native observations feed the owner; overlapping children cannot mint independent useful-time credits.

The integration sequence is explicit:

1. The Core owner measures fresh useful execution with the #995 union-clock rules. It keeps advisory processed progress live/coalesced and performs mandatory heartbeat/currentness separately.
2. At a quiescent candidate frontier, Native gives fixed-metadata extents for capture, restore and receiving estimates. Do not serialize the whole conductor or initialize the generation store just to ask the policy.
3. The owner reserves the finite cost/count grant at the surviving accounting root **before optional setup, encoding, scanning, snapshot writes or cleanup work**. The estimate covers the full candidate and its retained/retirement tail. If no trustworthy finite estimate or resource reservation is available, optional work is refused; semantic execution continues.
4. Native holds the quiescent executor and builds/persists the complete generation outside the Core writer transaction. It retains its original resources and candidate pin throughout. The captured descriptor identifies exact source, algorithm, generation, control and page root.
5. Core opens one short fenced transaction and verifies current construction/source/worker/grant and candidate coverage. It atomically adopts the typed `resume_frontier_json`, settles successful-creation count and records the exact selection outcome. Accounting updates must not erase a prior selected frontier.
6. Native receives the known selection outcome. A lost catalog response is reconciled by the same grant/generation identity; the candidate remains pinned until resolved. A stale worker never publishes. A known rejected candidate is reclaimed through its original receiving frame. An unknown outcome/cost follows #995's conservative accounting seal and prevents further optional work; it is not a reason to discard confirmed archive outcomes. Once the complete candidate is independent of scratch, an executor whose current source/worker authority is still positively established may abandon incremental tracking and continue disposable semantic work while that candidate remains pinned. It must not wait indefinitely solely for optional receiving; genuinely unknown current authority still prevents unauthorized work.
7. Retire excess unpinned generations in bounded quanta only after replacement selection and original reader/worker receiving. Required cleanup costs are settled even if semantic completion means no new optional selection is useful.

Do not implement an atomic transaction across the generation store and Core. The persist-before-select ordering, immutable predecessors and pinned ambiguous candidates make the actual two-resource protocol safe. PostgreSQL versus SQLite selection is Core's existing backend responsibility; Native's SQLite snapshot backend does not replace it.

### Progress contract hard cut

Replace the current mixed `JournalValidationProgress` with a structure whose fields distinguish meaning:

- `processed`: volatile phase, accepted/processed frame and byte counts, observed source offset, local observation sequence and complete flag. It can move backward after interruption and is not a recovery promise.
- `resume_frontier`: optional exact selected generation descriptor and its covered prefix, or absence. It changes only after owner selection and receiving; an offered candidate is not selected progress.
- `identity`: operation/construction ID and current worker epoch. The epoch is the owner's fence, independent of observation sequence.

Remove the old per-microstep `version`/`checkpoint_sha256` semantics; do not keep an alias that still implies every yield is durable. `progress()` stays bounded and metadata-only. Return/check the accepted-prefix digest from the live continuation with bounded padding work, rather than re-encoding parent recovery state.

In `UploadJournalValidationOwner`, retain the existing passive `JournalConstruction`, early-handle callback, `_Slot`, lease and `_drop` ownership protocol. Change `_open()` to admit/open disposable live execution and an optional selected descriptor. Change `advance()` to execute a quantum, coalesce `processed`, and call durable jobs logic only for a selected generation, final semantic outcome, or actual mandatory owner transition. Discovery and corpus owners receive the same cut. Missing optional state safely replays; missing source/intent/custody retains the original error classification.

Lazy generation storage requires its **own original passive construction child**, created after the grant and retained by the existing slot before its first file/connection effect. `JournalConstruction.begin()` is single-attempt, retains one storage facade and closes transient creator/directory records at `ready()`; do not reopen that completed frame or use one receipt for two databases. Add a Native-owned generation construction carrying its own admitted `SqliteConnectionScope` and file/sidecar records, with bounded subordinate capture/restore scopes. The existing slot retains those originals and its original workspace lease. Its receiving callback releases that lease only after scratch/result dependents, generation/capture/restore children and the actual worker have all received. A partial open or callback failure keeps the exact child reachable and charged. This extends the existing owner protocol; it does not create a second reclamation authority or initialize optional resources before grant.

For terminal result callers, introduce a narrow `DerivedNativeCacheUnavailable` at the derived-cache layer. Current `open_upload_journal_result` catches broad runtime/value/database exceptions as lost workspace; those broad classes must not become a restart discriminator. Preserve `_confirmed` checks of mandatory capture coordinates, source/certificate identity and exact outcome, replacing only its dependency on complete old `job.progress_json` with the semantic outcome and a separate optional cache descriptor. A receipt cannot certify a surviving non-durable scratch image after process death. Reopen an exact verified completed generation when one exists, otherwise coalesce bounded revalidation; preserve discovery's final-anchor and live-custody checks.

Use one owner-pool rebuild key `(construction_id, exact_terminal_outcome_digest, complete_source_history_config_binding_digest, semantic_algorithm_version)`. The binding includes journal/corpus identity, exact source hash/extent, catalog/profile flags and source/selection manifests as applicable. Concurrent requests for that key share one admitted rebuild and then independently borrow its frozen view. A capacity/deferred result uses the existing owner scheduling path; it never runs payload replay inside a status or catalog transaction. A cache hit still performs the required fresh source/authorization checks.

Update affected jobs records, state-schema baseline/upgrade tests, public contracts/client/CLI status, format validation and growth declarations in the same integration change. Exact serialized field names proposed by #995 (`resume_frontier_json`) remain the common seam. Neither reference branch installs this contract.

## 6. Consumers and dispositions

This is the focused source-backed inventory for #996. It is not a new production registry and does not claim a whole-#994 census.

| Actual family | Required implementation/disposition |
| --- | --- |
| `journal.py::validate_journal_chunks` / `validate_journal` | Construct and drive the common executor. Preserve complete exact-source summary, materialization and independently owned result lifetime. |
| `create_journal`, `_append`, assertion-batch helpers, observation/correction/delivery helpers | Reuse common validation. Preserve the same authored bytes/IDs/timestamps and custody commitments; never regenerate captured nondeterministic evidence as old evidence. |
| `append_checkpoint` and archived checkpoint semantics | Evidenced no change to the archive format and exact framed-prefix commitment. They remain mandatory documentary records. |
| Retained journal/session/result and Core upload/discovery owners | Disposable active state plus selected immutable recovery generations; current source/worker fences and original resource receiving remain. |
| Ordinary/retained corpus and Core discovery continuation | One colocated semantic executor; complete DB/control/owner-cursor closure; no per-read recovery publication. |
| `MemberHistoryClosure.summary_at`, segmented archive validation, canonical provenance readers | Same complete semantic executor. Reuse verified immutable bytes safely; fresh/warm/evicted conditions never bypass current authority. |
| SDK `canonical_production`, processing provenance/writer, CLI local | Carry the changed public progress/policy contract where consumed. Preserve exact prepared-primary bytes and mandatory effects under #997; Native gains no application imports. |
| Independent recovery | Continue validation from archive plus required keys; no server, Core DB, optional checkpoint or online lookup dependency. Keep unknown optional profiles as exact evidence under existing `require_profiles=False` behavior. |
| Construction / sqlite-admission / retained-resource / workspace-reclamation | Extend exact admitted filenames/state objects and bounded cleanup for the new owned store. Preserve early handles, reservations, result-reader independence, positive close and actual worker join. |
| Direct `JournalStore.accept` and obsolete parent/child recovery machinery | Remove/reroute redundant private acceptance path and superseded codecs/restore traces together; retain semantic algorithms and needed scratch tables. |

Source navigation: [Native package](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance), [Core services](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/services), [SDK](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-client), [independent recovery](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/some-implementations/riverhog/recovery).

## 7. Cutover and implementation order

1. **Reproduce the small semantic slice.** Replace parent and child recovery representation with the live conductor/epoch interfaces. Bring ordinary journal and retained upload validation through it, preserve all golden/adversarial semantics and receiving, and demonstrate zero optional-store setup for cheap work in both policy modes. Integrate #995 accounting and #997 generation/result-only adoption for F1 immediately.
2. **Add exact generation offer/restore.** Implement the backend and complete envelope in [STORAGE.md](STORAGE.md) through existing admission/receiving owners. Use real long and unknown-length work on that same path to show default-policy eligibility, durable selection, process interruption, restoration and fallback. Synthetic elapsed values remain only decision tests.
3. **Converge corpus and remaining Native consumers.** Apply the same executor, colocated order state and coupled Core cursor; remove the old durable executor/unused direct path. Exercise readers, SDK authoring and independent recovery with the same captured evidence.
4. **Complete the scoped qualification.** Run the owned tests and full-lifecycle growth/installed/backend/platform gates in [QUALIFICATION.md](QUALIFICATION.md), record matched old/restart/adaptive observations and publish exact-source/merge-checkout evidence through the normal integration rail.

For existing in-flight work, preserve original construction IDs, captured prepared sources, authored evidence, intents, sealed outcomes and custody records. Quiesce/join old workers through the existing owner. Reclaim an obsolete Native optional workspace only after original receiving; replay its exact source under the new executor. Do not migrate arbitrary private microstep codecs or silently reset whole application databases. If exact source or mandatory state is unavailable, retain the existing explicit owner/operator error/settlement process. State structure changes use the existing explicit state-schema initialization/upgrade path; application startup refuses incompatible state rather than repairing or erasing it.

The reference does not reopen #991's former no-batching restriction, resume its held campaigns, change protected archive formats, authorize providers or establish release authority. The parent remains open until its wider cutover and aggregate evidence are complete.

## 8. Evidence and limits

[VALIDATION.md](VALIDATION.md) and [VALIDATION.json](VALIDATION.json) record exact commands, versions, source hashes and observed scope. The Native experiment runs maintained semantics on fixed captured examples. The storage prototype uses actual SQLite files and fault tests, but bypasses production admission/receiving and is not an installed package. The two are separate experiments; their successful tests do not establish integrated Native optional recovery, default-policy economics, F1 performance, cross-platform durability or #996 completion.

Use the production source and issue authorities to reproduce accepted mechanisms. Do not mechanically cherry-pick `.reference` into main, import its instrumentation, or treat a green reference test as a required production gate that already exists.
