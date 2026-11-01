# Focused implementation and qualification vectors

This is the required proof plan for consuming the proposed design, not executed
product acceptance. Keep work with the current upload, protocol, producer and
application owners. Do not merge `.reference/` into maintained product paths.

## First useful slice

Extend `tests/unit/test_collection_registration_work.py` using the actual upload
service/database. Pin the current construction and policy; declare one member
spanning 17 raw volumes; set a three-plan emission quantum. Admit, run one quantum,
close every service/session object and reopen. Confirm three staged plans and no
ordinary accepted member, volume, coverage, work or custody. Finish bounded stage
and verification steps, then publish once. Compare original/new planner checkpoint,
all canonical plan bytes and IDs, all raw offsets and all coverage. Inject a lost
publication reply and resolve the same acknowledgement without emitting again.

Use a separate observer connection, actual ORM/service queries and direct endpoint
attempts, not just model table counts. Cover the worker and HTTP/client path in the
next slice; a pure planner comparison is not a service visibility proof.

## Semantic and fault matrix

| Vector | Required exact outcome |
| --- | --- |
| 1/16/17/129/1,000 raw volumes, one member fixed | Complete original sequence/offset coverage, bounded rows/bytes per transaction and response. No inference about 1,000-member acceptance. |
| Small members, zero bytes, pending pack plus raw, exact raw boundary and tail | Identical pack membership/bytes, part units, raw plan bytes and final checkpoint to old planner. Earlier pending pack flushes exactly once. |
| Pending pack filled by several separate registrations | Earlier members remain published; new pack/coverage hidden until its owning operation publishes; no premature source release. |
| Existing equal members mixed with new members | Preserve original orders/identities and no re-emission for existing members; publish only new delta atomically. |
| Duplicate IDs in one request; changed length/SHA/raw declaration/absence | Same current validation/conflict, no staging mutation or substituted authority. |
| Changed construction/policy/generation/incarnation/claim | Fail closed; never carry cursor or plans to a replacement session. |
| Failure inserting member, plan, coverage, stage cursor or verification cursor | Current quantum rolls back. Previously committed hidden prefix remains resumable; ordinary accepted state is unchanged. |
| Lost stage-commit acknowledgement | Read persisted exact frontier, reuse selected plans; no replacement/upsert over changed bytes. |
| Lost publish acknowledgement; cold process restart | Immutable original registration outcome and pinned range; no reauthoring even if later registration/closure occurred. |
| Corrupted/missing/extra plan, edge, raw offset or frontier | Verification cannot reach ready/publication; retain evidence, no cursor skipping. |
| Two identical requests/workers | One operation and one selected quantum; stale revision/epoch loses without duplicate rows. |
| Different concurrent append, complete versus admit | Bounded busy/conflict or deterministic serialized outcome; no unbounded queue, leapfrog suffix or lost member. |
| Arbitrary GET/list/relationship query during staging | No staged member/plan/coverage; parent counters/checkpoint remain an all-old snapshot. |
| Direct guessed staged volume upload, provenance bind or raw-digest POST | Rejected despite an existing staged FK row; hidden IDs are not work permission. |
| Slow pending operation in one construction | Independent constructions and existing custody/finalization/heartbeat progress; status does no emission. |
| Client wait canceled/disconnected; old call still alive | No inferred server cancellation, no source/primary release, exact batch survives cold restart. |
| Current key/grant revoked, claim replaced, custody lease expires | Pause/reject further mutation/publication under owner policy. No automatic heartbeat by server staging; orphan state retains intent. |
| Authorized cancel/discard racing ready publication | One fenced outcome, existing custody restrictions preserved; stale worker cannot publish after terminal transition. |
| Abort/cleanup interrupted after any bounded delete/cursor commit | No accepted rows removed; no suffix reuse before cleanup; exact resource reservation released once. |
| Complete while registration pending/aborting | Explicit pending conflict; no tail flush, closed checkpoint or false planning_complete. |
| No currently ready work while planning open or raw digests incomplete | No false payload/collection complete. Published prior volumes remain available when independently ready. |
| Raw digest batches crossing volume boundaries | Ready set exactly matches accepted prefix and original raw-part authority; update work bounded by append/volume-part invariant. |
| First/last raw receipt; earlier pack receipt during pending append | No hidden member seal, no truncated-coverage custody, no duplicate counters. Existing content/Native/history checks remain. |
| First/middle/last/empty summary page, later append and finalization | Pinned original emission interval, explicit exhaustion, immutable summaries, current authorized readback. |
| Forged cursor, wrong request/scope, overlarge page, repeat page | Reject substitution/invalid grants, exact page replay; no OFFSET scan or unbounded response. |
| Disk/transaction capacity/timeout during stage, verify or publish | Preserve original frontier; explicit pending/refusal; no partial-as-complete or global volume cap. |
| Cleanup capacity unavailable or permanent storage error | Report retained-state failure and operator action; do not promise success on unwritable storage. |

## Actual-content acceptance

After metadata parity, use actual HTTP/ApiClient, maintained shared producer and
Native/crypto/archive store with a small allowed per-volume policy. Exercise at
least one raw member spanning multiple stage quanta and a prior pending pack.
Include actual stage/publish lost responses, source mutation rejection, cold client
batch recovery, raw digest submission, repeated unit receipt, complete finalization,
original custody readback and independent recovery. Compare the original exact
archive roots, descriptions/tags, history, IDs and verified bytes. Existing conditional
provider writes must not be called for unpublished staging and must remain unchanged
for published work. A mocked sealed flag is not evidence for any of this.

The tested actual-byte workload can be small without redefining the separately
approved larger collection envelope/cadence. Do not change existing deadlines,
crypto, source-release rules or required workload just to pass.

## Growth, resources and compositional evidence

Vary member count N and volumes per member V independently, and vary input/plan
metadata bytes, per-volume coverage K, batch/quantum size, retained published
backlog, raw-part count and retry count. Include first/middle/last/cold/empty and
near-capacity cases. Pin configuration in each evidence record.

Measure actual ORM creation/loads, SQL rows and backend plan/VM/index work,
transaction count and largest committed/attempted row/byte set, serialized request,
response and cursor bytes, planner calls, total plan bytes generated/verified,
peak live objects/buffers, persisted cursor/control size, and cleanup work.
Do not infer storage-engine page/cache limits from row counts or time alone.

Success criteria: each quantum stays within its explicit grants; final publication
has no V/E-sized scan/update; total generation and verification traverse each plan
and coverage a bounded number of times; response paging does not rematerialize its
tail; failures/retries and eventual cleanup remain visible. Large prior accepted
backlog and hidden suffix do not turn point control/status into scans. Retain
necessary single-volume/pack author and backend-index costs as explicit terms.

Use existing source-owned growth declarations and #963 evidence machinery. Add
no parallel policy registry or timing gate. Report actual times separately; no
new numerical speed guarantee is proposed. This metadata reference supplies no
Native/aggregate process heap or filesystem page-bound qualification.

## Integration placement and delivery

Adapt `incremental_plan.py` and raw emission under their owning implementation;
store operational cursors/frontiers in server models and the existing StateSchema.
Adapt `CollectionUploadService.register_artifacts`, `_persist_plan_batch`, every
consumer predicate, bounded maintenance execution and closure/disposition.

Update protocol/API/client registration outcomes and page cursors together. The
shared producer owns durable pending-batch reconciliation under existing application
allocation state, with FTP/CLI/derived forwarding and recovery tests. Preserve
private cross-component boundaries and protected contract vectors.

Run locked lint/focused owner and installed-client tests, SQLite and PostgreSQL
concurrency/rollback witnesses, HTTP/Native/ingress lifecycle and existing required
qualification. Require the exact final source and tested merge's complete required
checks, not an ancestor or the reference probe. Missing/blocked/skipped evidence
is not green. Keep the earlier efb9a615 evidence separate.

Record in #964 the exact source, finite adoption/reproduction/disposition of this
packet, interface decisions actually accepted, actual evidence, remaining cleanup
and one next slice. This handoff does not close P15, #964, #981 or release work.
