# Focused implementation and decisive acceptance

This is a proposed proof plan, not a claim that these maintained tests ran.
Use the current authoritative source and existing qualification rail. Keep the
original #984 four-run witness as prior issue evidence, not this packet's run.

## First useful slice

Extend `tests/unit/test_collection_uploads.py` / the existing
`_service_with_archive_objects` fixture and actual `upload_unit` path. Keep its
raw member, part digest, original encrypted preparation and MemoryResumableStore.
Use events to stop the worker after a DURABLE ARMED begin but before the provider
creates its session. Select cancel and assert pending, intact owner/plan and no
provider/catalog deletion. Let begin return, durably settle the exact token, and
prove the next part/complete admission is rejected. Drain/abort known session,
finish bounded cleanup and return one retained cancel outcome. Replay a lost reply.

Then hold a real complete before its effect instead: selected cancel waits; the
returned exact completed receipt is persisted and cancellation refuses without
removing original bytes. Reproduce the same orderings on PostgreSQL with separate
connections. Do not substitute a final state check after provider work.

## Source cut list

| Owning source | Required change |
| --- | --- |
| Catalog models + explicit migration/StateSchema | Construction gate, original pre-begin preparation, bounded unresolved effect rows, terminal request/outcome retained independently of upload deletion; indexes and constraints. |
| `raw_upload.py`, `pack_upload.py` | Separate pre-begin preparation persistence; correlate each effect; no replacement crypto/session after uncertainty; join/account nested writes. |
| `stores/sqlalchemy_archive_upload_checkpoints.py` | Upload-before-volume lock order; atomic exact result/checkpoint settlement; preserve late reports without reopening/lease renewal. |
| `services/collection_uploads.py` | Admit/fence/drain/adjudicate phases; source-aware late seal; exact terminal retries; no shape-only success; no direct unbounded delete loops. |
| `services/collection_registration_cleanup.py` | Freeze at selection, start destructive cleanup only after terminal rights are settled; retain original reservation when cancel is refused. |
| `services/collection_workflows.py` | Claim renewal/restart/output admission interlock, including renewal of expired claims that own output. |
| `ports/archive_objects.py`, storage adapter protocol/support/client/ASGI | Exact call correlation, typed observation versus execution closure/unknown, bounded cleanup-step contract; all implementations updated together. |
| S3/filesystem adapters | Qualified single-attempt/child-settlement profile, read-only observation, per-item cleanup results and bounded paging; unsupported proof stays unknown. |
| Mirrored archive/cache owner | Separate child admissions; handle mutating cache admission inside apparent lookup; durable abandoning transfer rather than forgotten optional effects. |
| Archive maintenance/service execution owner | Nonblocking submit/poll of bounded provider work; avoid shared coordination lock during waits; recovery of retained intents, not blind `discarding -> orphaned` reset. |
| HTTP schemas/client/producer/CLI/FTP/derived callers | Current-only pending versus success/refusal outcomes, retained exact terminal request, no source release on 202, stop or connection loss. |

Activation is coordinated: no old unfenced process may continue using the prefix
while new code treats its rows as fully tracked. Preserve durable pre-v1 state;
untracked existing effects are reconciliation errors, not synthetic settled rows.
A controlled upgrade may discover unresolved legacy work; it cannot erase it or
assume process restart canceled remote operations. No provider/release operation is
authorized by reference publication.

## Failure matrix

| Boundary | Decisive required result |
| --- | --- |
| Prepared, before arm; terminal wins | Arm rejects, zero provider calls. |
| Arm transaction fails or reply is lost | No speculative dispatch; retain/reconcile exact call authority. |
| Arm committed, worker paused before begin | Terminal pending; owner retained; release worker produces one tracked session. |
| Begin effect succeeded, reply/checkpoint commit lost | Exact preparation survives; UNKNOWN blocks teardown; no second begin or new encryption state. |
| Live worker proves it never sent | Only after dispatch children are unable to run can exact no-effect settle. |
| Part paused before/after effect | No terminal cleanup while unresolved; completion cannot newly arm after closure. |
| Complete paused before effect / response body | Late exact seal survives; cancel refuses; no early HTTP-header success. |
| Completion observed, execution closure unknown | Preserve custody fact AND unresolved call; neither deletion nor forced replay. |
| Duplicate outcome report, identical / changed | Identical idempotent; changed identity/result rejected. |
| Old worker epoch after restart | No new call admission. Exact already-armed outcome may still be retained. |
| Process exits after arm, before/after send | Cold restart retains ambiguity. PID death/lock release does not clear remote effect. |
| Repeated empty HEAD/list while old begin is delayed | No terminal success; later begin cannot become unowned. |
| Hidden registration cleanup plus late seal | Cancel refusal leaves recoverable hidden state; no earlier destructive cleanup. |
| Concurrent cancel/discard / differing retry request | One selected exact operation; loser conflict; no silent kind/challenge substitution. |
| Expired challenge before first selection | Rejected without destructive work. |
| Exact selected challenge expires during cleanup | Resume original plan/result; do not reauthorize from shrinking live counts. |
| Late seal or broader custody after discard selection | No authority expansion; retain original, require new explicit challenge. |
| Live claim renewed/acquired during terminal selection | Ordered interlock prevents overlap; current permission/claim guards hold. |
| Permission revoked or target incarnation changed | Pause/reject further effects; retain completed evidence and original scope. |
| Partial provider abort/delete; overall HTTP 200 with item errors | Retain exact failed page; no false emptiness or capacity release. |
| Provider cleanup finished, catalog commit failed | Retry monotone exact cleanup/recording; no replacement writes. |
| Catalog batch failure or lost final response | Child-before-parent rollback/cursor preserved; exact terminal result replayed after upload deletion. |
| Sealed volume, primary/history not yet complete | Preserve volume authority, do not manufacture a member custody receipt. |
| Cache child admission succeeds then archive begin fails | Cache child has retained owner/abandoning evidence, not an orphan token. |
| Finalizer small put/copy writer overlaps | Participates in the same gate/drain or is excluded by a proven owner fence. |
| Empty/zero-effect upload | Same terminal authority/result contract without special unsafe absence shortcuts. |

Use actual process kills for selected cases, separate SQLite/PostgreSQL connections,
actual adapter HTTP boundaries and real locked adapter conformance fixtures. Event
ordering is deterministic correctness evidence; deadlines only prevent a hung test.
Power loss is a separate requirement from process exit. Preserve failed attempts.

## Boundedness and resources

Vary unrelated completed backlog, admitted unresolved effects, volume/part count,
control bytes, provider-page count and retries independently. Include empty/first/
last points, deep cold state, all capacity dimensions, stuck response, partial page,
and a second construction that completes while the first provider call is held.
Instrument real SQL statements/rows, provider attempts/bytes, retained controls,
active and uncertain slots, worker joins, original reservation release and peak
buffers. Count lower-level hidden retries. No gate may depend solely on elapsed time.

Prove point unresolved lookup/index use instead of scanning all checkpoints on every
poll. Provider/catalog cleanup total work is proportional to necessary entries
and attempts (plus declared index/provider costs), not a growing completed-prefix
scan. Prove bounded batch contents and no FK cascade that hides whole-construction
cleanup. Reserve outcome/cleanup space before forward admission; full storage must
retain original intent and allow already reserved control progress or report an
honest blocked state. Do not promise progress on an unwritable provider/device.

The reference's SQLite index assertion is not production I/O/growth evidence.
The existing raw/pack checkpoint vector and multipart completion list traversal
costs remain independently declared; do not claim this packet fixes all transfer
complexity. No arbitrary new total-volume or archive-size ceiling is selected.

## Maintained entrypoints and delivery

Extend existing `test_collection_uploads.py`, `test_collection_upload_unit_http.py`,
`test_raw_upload.py`, `test_pack_upload.py`, `test_archive_write_reaper.py`,
`test_collection_registration_cleanup.py`, and
`test_mirrored_archive_resumable_object_store.py`, plus storage adapter protocol/
HTTP/conformance tests and `tests/integration/test_catalog_schema_postgres.py`.
Keep exact immutable bytes/plans/receipts/root tests and conditional publication.
Finish the coupled registration/client cut only after terminal integration is
reconciled. Run locked lint/focused tests before checkpoint pushes and the required
exact-source/tested-merge, installed/backend and lifecycle qualification before
handing back a qualified implementation. No reference test replaces those gates.

Record finite adoption/reproduction/disposition in #984 with exact source and
remaining qualification. Do not close #964/#981 or authorize release/provider
qualification based on this design packet.
