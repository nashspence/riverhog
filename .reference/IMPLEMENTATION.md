# Processing and application replay implementation proposal

This is the focused implementation input for [#998](https://github.com/nashspence/riverhog/issues/998), governed by [#994](https://github.com/nashspence/riverhog/issues/994) and published under [#903](https://github.com/nashspence/riverhog/issues/903). Product source remains exactly `c28eceaa2dfec1b3712135469ba12230e74f5ffa`. The executable material here consists of reference experiments and two small proposed helpers; the full production implementation is specified for the integration owner, not claimed complete.

## 1. Selected implementation

Retain the original accepted obligation and make repeatable computation economical. Claims, accepted observer/tool evidence, exact output/completion bytes, external intent/results, ingress custody and actual resource receiving remain required. Derived planning/hash/index progress moves to admitted private live state. Status and callbacks expose bounded advisory samples without making each sample a durable recovery boundary. Optional consistent generations use the same shared policy and semantic executor as the publication path.

The first integration slice is one real Stove0 processing claim through its actual target and SDK completion/publication. Bind its logical workflow root at admission, use the volatile target progress slot, and preserve exact terminal results and original worker receiving. The directly measured target callback cut removes concrete overhead without waiting for all application owners. Extend the same path with late optional grants once the shared contracts are usable, then add the remaining owner-specific corrections in this package. Independent safety fixes such as FTP receiving can proceed in parallel; no whole-repository census or six serial issue closures is introduced.

Three pinned references supply adjacent design input, each with its own owner and evidence:

| Input | Exact reference | Used here |
| --- | --- | --- |
| Shared policy, eligibility and liability | [#995 / b545a77996fb](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md) | One policy, replay saving/cost admission, failed attempts, count slots and unknown accounting. |
| Native execution and generations | [#996 / e4a607081d17](https://github.com/nashspence/riverhog/blob/e4a607081d17ae1a067bb766241bd5febd9ddeb9/.reference/HANDOFF.md) | Private execution, bounded spill, complete consistent generation and dependency publication. |
| SDK/Core publication and custody | [#997 / a7ea19bf795c](https://github.com/nashspence/riverhog/blob/a7ea19bf795c249476c9d767eb821d74bcf98b2a/.reference/HANDOFF.md) | Actual shared collection production, original receiving, source lifetime and operational control. |

These are non-authoritative inputs, not prerequisite branch merges. Reconcile with then-current implementation and owning decisions. #999 retains physical storage/read/retrieval/materialization ownership; #955/#969 consume shared seams only if separately commissioned. Parent #994 retains aggregate acceptance and the pause on independent #991 work.

## 2. Exact owner dispositions

| Owner | Proposed change | Required mechanism retained | Detail |
| --- | --- | --- | --- |
| Core collection workflows and SDK processing | Private set conductor; async seal status; exact prepared-set proof; revision/current-key/source pins; conservative sealed-effect refusal. | Accepted rows/ordinals, canonical identities, plans, exact effect/no-output/mixed outcomes, leases, source-retirement authority and original close. | [CORE_WORKFLOWS.md](CORE_WORKFLOWS.md) |
| Stove0 controller and target/observer support | Volatile callback overlay; no redundant status adoption; private planning epoch; readonly index access; required original retention/capacity. | Original requests, observations, selected child declarations, nondeterministic outputs, completion preimages and publication receipts. | [STOVE0.md](STOVE0.md) |
| Opus/NVENC and other supplied observers/tools | Qualified original process receiving; complete file durability before group/result selection; exact retained completed outputs. | Tool inputs/intent and original evidence. No invented midpoint codec/observer resume. | [STOVE0.md](STOVE0.md) |
| rclone and Core external effects | Retain streamed exact intent before entry; reconcile exact remote completion outside status; refuse ambiguous whole-effect replay/abandonment. | Copy/verify/marker-last ordering and exact successful effect settlement. | [STOVE0.md](STOVE0.md), [CORE_WORKFLOWS.md](CORE_WORKFLOWS.md) |
| FTP ingress | Original pass slot/actual thread receiving; preserve candidate after ambiguous registration; close original store before reclamation; volatile retry rotation. | Original delivery log/custody, manifest/IDs, receipt coverage and completion-offset guard. Keep root accounting after payload receipt when necessary. | [INGRESS_LIFETIME.md](INGRESS_LIFETIME.md) |
| Gogurt/native listeners | No automatic arbitrary-script replay after possible entry; advisory heartbeat durability cut; bounded summary; retain state on uninstall. | Original event/dispatch identity, entered/uncertain outcome, actual native/process lifetime. | [INGRESS_LIFETIME.md](INGRESS_LIFETIME.md) |
| CLI | Retain exact retrieval-planning idempotency key and parameters, including explicitly resolved lease, before request; forward shared policy. | Original upload identities/history and factory receiving. Local output authority stays with #999. | [INGRESS_LIFETIME.md](INGRESS_LIFETIME.md) |
| Event relay | Positive original worker joins on shutdown. No new recovery store or cursor batching. | Whole-page delivery acknowledgement, stable event IDs and intentional duplicate semantics. | [APPLICATION_EFFECTS.md](APPLICATION_EFFECTS.md) |
| Minisign/OpenTimestamps witnesses | Fresh read-only no-change branch eliminates empty-poll writes. | Atomic changed observations/intent/cursor, original statement/nonce/signature/proof, CAS and current endpoint/key controls. | [APPLICATION_EFFECTS.md](APPLICATION_EFFECTS.md) |
| Review0 and remote samplers | Specific original invocation accept/readback/cancel/receive contract; protected control owner; output barrier; separate execution slot from retained result; bounded original receiving proof. | Existing sampler semantic request/result digests, descriptor/plan/output identity, current public authorization and shared publication owner. | [APPLICATION_EFFECTS.md](APPLICATION_EFFECTS.md) |

Already conforming paths have deliberate no-change dispositions. In particular, an original observer envelope, a completion journal or a witness proof is not made optional by calling it a checkpoint. Conversely, a rolling hash over already retained accepted rows is not made mandatory merely because it currently shares a claim transaction.

## 3. Decisions that cross component boundaries

### One root, late grants and measurable replay saving

[BUDGET.md](BUDGET.md) fixes roots on existing accepted owners: root Stove0 WorkRecord, original FTP claim, or standalone Core construction. Child records, Review0 samplers, SDK and Native work inherit the original assignment and disjoint reservations. Every candidate must independently justify the net replay work it saves; a long parent does not make every cheap child worth saving.

The immutable `RecoveryAttachment` assigns a receiver/invocation with zero initial allowance. Separately numbered `RecoveryGrantOffer`s can later reach that same running original. Root reservations precede offers; exact offers survive lost acknowledgements. Receiver assignment uniqueness prevents two Core constructions spending the same attached allowance. The parent pulls bounded phase requests/costs from existing authenticated readback, records its freeze gate, then sends a start permit to the configured receiver; Core needs no reverse callback to an application and the CLI needs no server. New offers/phase starts require current authorization; narrowly scoped original receiving does not grant new work.

Bounded cumulative fresh-work prefixes may arrive before terminal completion. The conservative cohort algorithm counts the maximum verified child interval rather than adding concurrent child times, freezes new credit during optional phases, subtracts overlap, and discards unanchored credit after restart. Old-child cleanup is still debited when it overlaps a new cohort. Uncertain cost holds liability and suppresses further optional work while mandatory execution/receiving continues. The reference arithmetic has 26 final cases; it does not itself implement authentication, durable grants or process receiving.

### Core asynchronous sealing must be usable by actual clients

The selected Core conductor keeps one execution path for warm, cold and optional recovery. Accepted input/artifact rows remain exact. When a seal is not immediately prepared, the operation returns accepted `sealing`; bounded GET readback reports completion without advancing work. The specification names the missing capability-artifact status endpoint and SDK method, and the current composite SDK/Stove0 callers that must wait under the original deadline/lease.

A `PreparedProcessingSet` proves complete canonical content and exact ordinal installation. Final selection compares fixed source/semantic revisions, current key authority and fences in a short transaction. Required ordinal installation is idempotent and does not invalidate its own semantic revision. Pending-seal source blockers survive lease expiry until original receiving. Spill and installations reserve bytes as well as workers; capacity refusal does not create a hidden total collection limit.

### A failed effect does not imply that no effect happened

Current Core restart already guards a sealed external-effect plan, while ordinary abandonment only checks transform output. The proposal closes that mismatch: once an external-effect plan is sealed, failure, cancellation, timeout or absent remote marker cannot substitute abandonment/no-output or release the source claim. Exact successful effect receipt and Core settlement remain the closing authority. A component's local claim that dispatch never happened has no current Core negative-effect receipt contract; the selected behavior is conservative retention with visible admission pressure, not a new generic closure API.

Gogurt's arbitrary command retry has the same distinction. Only positively pre-entry failures may automatically retry. After possible entry, retain the original uncertainty. CLI retrieval similarly retains its original planning request so a lost response cannot create a second plan/job under a newly generated key.

### Quiescence, output adoption and parent settlement are different facts

FTP cannot cancel an asyncio wrapper and then close the API while its `to_thread` worker still runs. Its retained slot owns the actual worker, result and original storage until each is positively received. A lost registration reply cannot authorize deleting the candidate whose registration may already be committed. A received payload can be reclaimed separately while its compact original accounting root survives outstanding cleanup liability.

Review0's existing terminal synchronous sampler API cannot reconcile timeout safely. The selected application-specific protocol retains original invocation/control/workspace identities and a terminal result only after actual process/stream receiving and full output durability/pinning. Release the execution slot at that boundary so the next disjoint input group can use the workspace before parent settlement. Transfer result/output ownership to the configured Review0 owner through exact `:receive`; Review0 then retains remaining publication/parent obligations. The sampler never asserts Core settlement authority.

The explicit protected state owner, per-registration original-receiving proof, capacity reservation, key rotation/revocation handling and workspace retirement commitment are fully specified in the owner document. Current public authority remains required for new execution and full-result disclosure. Compact retired-workspace records remain charged for the control incarnation's lifetime; quota pressure refuses new admission while reserved receiving remains operable. This is proposed protocol work, not behavior proven by the existing sampler baseline.

## 4. Required coordinated cut and qualification

Implement public operational contracts through their owning packages. Do not import Stove0/FTP/Gogurt code into the SDK or a reusable protocol library. Keep recovery attachments and accounting outside semantic identity, archived/provenance bytes, target plans, sampler semantic digests and original completion encodings. Update schemas, clients, CLI progress, installed configuration and current state baselines together. Remove superseded progress interfaces and durable microstep execution routes.

Preserve explicit state initialization/upgrade machinery. A new executable refuses populated incompatible pre-v1 state; it does not erase accepted evidence or silently initialize over uncertainty. An old compatible owner may finish and receive its originals. Explicit operator disposition remains required for irreplaceable data the new owner cannot safely consume. No invented migration/export facility is assumed.

[QUALIFICATION.md](QUALIFICATION.md) gives causal fault cases, exact owner test families, deliberate private-trace rebaselines, growth/resource declarations, installed/backend/platform gates and matched old/restart/adaptive measurement conditions. Mandatory completion and final original cleanup are measured separately. Count actual application/SQL boundaries; do not label each transaction a physical fsync. Timing targets inform investigation; safety, policy-decision conformance, growth and evidence validity remain enforced.

The actual evidence is in [VALIDATION.md](VALIDATION.md): 65 passing reference cases, separate maintained source baselines, a target progress counter comparison and witness idle-poll SQL comparison. The full production cut, grant transport, new sampler protocol and installed lifecycle remain qualification work. A green local reference suite does not establish #998 or parent #994 completion.

## 5. Research basis and failure limits

The primary authority is the pinned implementation, public contracts, policy tests and current issue decisions linked above and in each owner document. External primary sources confirm narrow failure-model distinctions: [Python's Future contract](https://docs.python.org/3.12/library/concurrent.futures.html) does not equate cancellation with stopping an already running callable; [RFC9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2) distinguishes safe retry of idempotent intent from arbitrary replay; [SQLite atomic commit](https://www.sqlite.org/atomiccommit.html) explains why SQL commit observations and storage crash guarantees must be reported separately. These sources support the reasoning, not a claim that Riverhog's proposed paths have been qualified on every backend.
