# G38/G39: one exact question, bounded delivery and acceptance

Recommended design for #964 under #903; not production code or a completed
owner-conformance claim. The scoped current-only public/validator contract change
is explicitly approved by [#964 decision 6118314165](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118314165);
exact owner schemas, bytes/cap vectors, and integrated evidence remain required. Audited main: `152d095038df0c0d0ed642cf265d47b11c65a754`, tree
`991e994457b487a7dca016b7fc0409485cef25c6`. Reference parent:
`4b0b9e2a717a2773f9ee7f68fa33d3fc4a73f176`.
Reconcile the actual post-#963 source, current #948/#953 decisions and owner
contracts before integration. Earlier conversation is calibration, not authority.

## 1. Selected architecture and scope

**Keep one whole-scope question. Segment its representation, execution and
validation, never its asserted meaning.** Use the existing logical question,
exact selections, observer job rail and controller-owned evidence authorities.
Replace bulk request/result/evidence models at this seam with immutable references
and bounded readers. Do not add a second scheduler, general workflow engine,
content-addressed permission system, or server dependency to observer packages.

G38 owns preparing and delivering exact task inputs; G39 owns independent result
acceptance and indexed views. G37 supplies sealed selections and planning storage;
G02/G40 own schema/predicate cost profiles; G22/G24 supply exact provenance readers;
G41 consumes accepted relations. #948 continues to own dormant execution, permits,
control-call budgets and current claim maintenance. #953 owns the compiled
language, named ports and semantics. G38/G39 refine representations and adapters
at these boundaries, not silently replace their authorities.

There are three completion boundaries:
1. Transport complete: exact referenced bytes are durably present and verified.
2. Observer complete: one sealed observed/inapplicable/failed/canceled outcome is
   retained for the exact job. A transport-complete file is not such an outcome.
3. Controller accepted: exact schema, semantics, scope, coverage and source
   evidence have all validated. Only this boundary can expose planning facts.

No physical page, single subject, JSON record, relation, semantic callback or
terminal scan is exempt from work/byte budgeting. Database transactions end at a
bounded load or commit; expensive evaluation and network/provider work execute
without a borrowed database connection. Keep checked progress on immutable inputs;
ordinary restarts or unrelated content updates do not restart a completed phase.
Logical finite totals have no new hidden maximum. Record actual representation
limits and operator capacity; finite-resource rejection is not truncation.

### Alternatives rejected

* Splitting a whole-scope query into independent questions loses relations and
  complete-negative meaning. Parallel internal work is allowed only under one
  complete input authority and one final outcome.
* Raising HTTP limits or sending one giant JSON file through metadata staging
  merely relocates materialization. `execute_model`, `_read_exact`, `observe`,
  `project_interface`, result serialization and status polling all need review.
* A mandatory generic event/fact language replacing every observer schema is
  unnecessary. Preserve each exact logical JSON facts document and interface;
  change how it is read and validated. Do not lose fields not projected into views.
* A new Merkle identity for every observation is not needed here. Reuse exact
  metadata SHA-256 and existing ordered selection/evidence commitments. Durable
  continuation uses a reviewed hash backend, not a new digest-of-digests meaning.
* A full database snapshot, graph-sized transaction, or restarting until content
  stops changing is not the execution default.

## 2. Logical scope and identities

Retain `ObservationQuestion` as the semantic task authority: work, task ID, exact
observer contract/interface, union selection, named subject/evidence ports,
literal options, retrieve policy and read action. Preserve its existing canonical
transcript where the semantic document is unchanged. The stored/read form may be
a document reference; do not hash a reference wrapper and call that the former
question digest. Large semantic documents are streamed once with exact encoding.

Subject ports are an exact disjoint union of the question's scope. Compare the
entire native `WorkArtifactSubject` at this boundary, including role where the
current owner requires it. Source-root/member identity and subject ID are not
payload digest. Equal bytes may belong to distinct subjects. A role-excluding
comparison is only used at the existing explicitly role-independent predecessor
binding, never generalized from that exception.

**Two orders remain distinct.** Native `ArtifactSelection` commits its `_artifact_key`
(source collection/root/artifact tuple); request and port traversal uses subject
ID order. Retain the native selection commitment; build an indexed subject-order
projection with a verified exact bijection. Do not accidentally hash subject-ID
order as native selection order. Ports use the same source-qualified members.

For `whole-scope`, one delivery unit covers the entire question, even when data
arrives over millions of chunks. For `independent-subjects`, reserve disjoint
immutable delivery selections through a monotone traversal of question subjects;
the union must be exact before task acceptance. Batch policy may change physical
request IDs, result grouping and execution evidence, but never the logical
question, recipe/work identity or required task-subject incidence. A fixed
request's transport segmentation must not change its result identity. Do not
promise identical testimony from nondeterministic observers or from different
physical partitions.

`complete-empty` is controller-owned completion with zero observer results.
`inapplicable` empty scope does not manufacture an observed empty result. Global
views and relation rules retain their current exact interface requirements.

### Proposed current-only v2 boundary

Use the existing `/v1/observations/{job_id}` PUT/GET/cancel operations, with revised
closed models, not a parallel v2 deployment API. Reject old request model shapes;
format-domain revision does not imply a compatibility reader. Concrete names:

```
ObservationRequestRefPayload (format stove0-observation-request/v2)
  question_sha256, question_document: MetadataDocumentRef
  delivery_scope: ArtifactSelectionRef
  delivery_manifest: MetadataDocumentRef
  observer_registration_id, observer_descriptor_sha256
  observer_contract_id, observer_contract_sha256, interface exact ref
  read_actions, retrieval_policy, timeout_seconds, maximum_result_bytes
ContentObservationRequest: above + request_id = JCS SHA256(payload)
ObservationInvocation: request + claim_id + fence + transient runtime
AcceptedObservationJob: same non-secret declaration + job_id = JCS SHA256(payload)
ObservationJobStatus: job_id, request_id, attempt, state, revision,
  outcome_ref | null, bounded failure code, detail_ref | null
ObservationResultRefPayload (format stove0-observation-result/v2)
  request_id, observer_descriptor_sha256, contract/interface/profile exact pins,
  delivery_scope, outcome discriminator,
  facts: MetadataDocumentRef | null, execution_evidence: MetadataDocumentRef,
  detail: MetadataDocumentRef | null
ContentObservationResult: above + result_sha256 = JCS SHA256(payload)
```

Observed requires facts and exact facts-profile identity; non-observed forbids
facts, with existing inapplicable/failure/canceled meanings. Schema bodies,
subjects, evidence pages and arbitrary diagnostics are not in status/control
models. IDs/names retain their owning scalar envelopes. Any potentially growing
map or descriptor uses a document ref, not a new list max. Normalized logical
options remain separate from generated port selections. The delivery manifest
contains exact subject-port subset refs, predecessor `AcceptedInput` refs,
literal-options ref, and the pins above. Its whole size is not a control bound.

Keep request semantic binding separate from runtime job binding: claim/fence and
workspace-protection declaration bind the job, not the logical question; secrets,
URLs, clocks, retry/chunk sizes, lease renewal and worker epoch enter no semantic
hash. Same ID/different declaration conflicts. A new claim is not an old job
relabeling, nor permission to overwrite an accepted outcome. Adopt terminal
completion precedence and refresh/retry semantics from current #948, not a second
interpretation. Current authority remains required for disclosure/adoption.

This explicitly changes request/result/accepted-evidence envelope identities.
No archive/member/journal commitment or source-loss permission changes. The
compiled interface and provider pins also change where reader contracts change.
The **scope and current-only identity/validator change are approved** by
[#964 comment 6118314165](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118314165). Owning contract
models, exact golden vectors, error and extent behavior still require
implementation review and qualification; G26's distinct approval is not
authority for any unrelated scope.

## 3. Use bounded metadata transport without a bulk handoff

Reuse `MetadataDocumentRef {sha256, bytes}` and `MetadataChunk`'s 64 KiB decoded
carrier and exact decimal offsets. These commit exact canonical document bytes;
they confer no approval or read permission. Preserve ordinary SHA-256, not a tree
hash under that field. Selected SHA envelopes and exact decimal counter parsing
must agree with native scalar policy; enormous numeric tokens themselves require
bounded parsing or an explicit existing representational envelope.

Extend the focused metadata owner with a **verified document handle** handoff:
`open_verified(ref, access_binding) -> closeable range/record reader`. New G38/G39
operations never route large bodies through `parse_identity_json(_read_exact())`,
Pydantic whole-document models, `read_bytes()`, or `execute_model`. Storage and
transport may reuse the existing package; semantic acceptance stays outside it.
This is a narrow G05 dependency, not authorization to redesign all metadata users.

Append at an exact offset. Repeating identical accepted bytes is idempotent;
conflicting overlap, gaps, overrun, wrong digest/length or a forged terminal fail.
Persist a source-bound hash checkpoint and received offset with segment publication
so a worker restart replays at most the uncommitted chunk. Receiving all bytes
enters VERIFYING, not semantic success. Keep canonical-byte validation incremental
as well. fsync/rename/parent synchronization and database markers have an explicit
recoverable ordering; verify a chunk before advancing a trusted marker. Neither
pickle nor backend-internal memory dumps are checkpoint formats.

The existing Stove0 private `CheckpointSHA256` is concrete reference input. Extract
or reuse a **reviewed neutral** implementation through the normal package boundary,
with codec version, algorithm, byte count, tail, source/root and job/phase binding,
corruption checks and independent native vectors. Observer-support must not import
`stove0_core._checkpoint_sha256`. Do not add another hand-written crypto backend
merely for this feature. Pending exact backend review is an explicit integration
requirement shared with the prior designs, not an unresolved choice of digest.

### Data authority and endpoint routing

Retain the established authenticated component registration, TLS and deployment
allowlist. Stage inputs to the selected component's metadata owner; the controller
pushes bounded chunks in background data-transfer work. No observer-supplied URL,
redirect, arbitrary digest GET or generic fetch service is introduced. Use a
focused observation-transfer reservation bound to `(job, claim, fence, direction,
manifest ref, selected component)` to authorize staging before execution. New
reserve/attach/refresh operations are declared by the observer protocol owner,
return fixed receipts and are idempotent. Transient tokens are excluded from docs.

A new input chunk/document may be attached only after the controller's prepared
manifest has been bound to that transfer. The component builds its allowed-doc
index from this exact manifest in bounded VERIFY_INPUT steps. A ref guessed from
another job grants no read. Blobs may be deduplicated physically, but ACL and
retention ownership are per job. Interrupted attachment is private. Result downloads
are similarly bound to the completed job/result and controller identity. Existing
metadata routes used by this path must consult that binding; a globally public
hash lookup is not an acceptable substitute.

The executor starts only after its required input manifest/selection/predecessor
closure is verified complete. Input transfer/verification uses bounded background
workers without occupying an execution permit while merely waiting. Reusing local
verified immutable bytes avoids retransferring an unchanged root. Charge missing
bytes and per-job incidence checks; do not claim every consumer reads one shared
predecessor only once globally.

Riverhog capabilities continue to authorize actual payload/provenance reads and
current root/claim/fence checks. Metadata reservation is not payload custody or
Riverhog permission. Cached inputs/results cannot bypass current authority. A
revoked job may retain protected audit state but cannot receive fresh data. Denial
is not an empty relation. Lease expiry, deletion, cancellation and resource failure
are explicit states distinct from successful absence.

## 4. G38 preparation and executor integration

Controller phases: PREPARE_SCOPE -> PREPARE_PORTS -> PREPARE_INPUTS ->
SEAL_DELIVERY -> TRANSFER -> ADMIT/REFRESH -> POLL -> FETCH_OUTCOME -> G39_ACCEPT.
Use existing named task/planning context ownership and #948 dispatch/contact rail.
Control passes schedule bounded steps; they do not construct request subjects,
validate arbitrary options, fetch complete evidence or run schema callbacks.
Cap aggregate work across jobs, preserve independent claim renewal, and release
permits while dormant. Data stages and CPU stages have their own accounted budgets.

Preparation reads immutable G37 selections with keyset/ordinal cursors, verifies
port union by merge/indexed equality, persists delivery reservations and exact
coverage obligations. No growing `IN(all_subject_ids)` or rescanning covered prefixes.
Reserve delivery membership before dispatch so retries return the same request.
A concurrent allocator cannot reserve overlapping independent units. A whole-scope
reservation simply refers to the full sealed selection, but the initial selection
and port proof still cost their real traversal once, not zero work.

Provide a neutral `ObservationInputReader`: indexed exact subject lookup, bounded
subject/port iteration, literal options read view, scoped predecessor authority and
record/range readers. It is closeable, with explicit borrowed/owned handle rules.
`runtime.subjects()` becomes bounded iteration rather than a tuple; no implicit
list conversion. `prepare()` must not collect all `ClaimedArtifact` objects.
A component can process several chunks as one logical question and spill shared
indexes; the generic controller does not interpret media or filenames.

Generated `/primary_ids`, `/sidecar_ids` and evidence-slot leaves are represented
by typed named reader ports, **not physically expanded into wire options**. The
selected implementation keeps the existing interface's `option_ids_at` /
`option_slots_at` mapping as the exact logical projection: `logical_options` is a
read-only disk-backed overlay of literal options plus virtual arrays over those
ports. Preserve collision/pointer rules and validate the same logical schema.
Maintained observers use the typed reader instead of `list()`/Pydantic whole-tree
construction. This does not add a second options language, let users replace a
generated leaf, or silently remove an option-schema obligation. Runtime/validator
ABI changes require new exact descriptor/conformance pins where they own that
behavior; a future change to logical option semantics is not part of this design.

Extend independently selected observer and semantic-validator APIs with
`begin(inputs, exact_profile)`, `step(budget, checkpoint)`, `finish()` and explicit
close/cancel. The checkpoint stores opaque owner state by bounded reference, not a
complete facts dictionary. Built-in CPU/tool consumers must use bounded reads and
spill; genuine codec/provider indivisible work has its named delegate bound and
cancellation behavior. A materialized adapter is allowed only for an explicit
contract-limited profile with proven bytes/work envelope. Unbounded `observe(dict)`
in a thread is not a conforming large-collection profile.

Do not change timeout policy silently. Preserve which execution interval a timeout
covers under #948. Expose transfer, input validation, execution, result sealing and
controller acceptance time separately; account total elapsed time as well.
Operational budgets/retention may suspend or refuse work, not reset execution time
on every chunk or advertise completion before acceptance.

## 5. G39 resumable semantic acceptance

Retain one exact original facts document. Parse it incrementally to a disk-backed
JSON node/value index with ordinals, typed values, parent/child links, segmented
large scalars and shared pointer paths. Canonical ordering, duplicate-key rejection,
UTF-16 JCS ordering, escaping, numeric/byte domains and canonical raw preimages
stay with their owners. No giant scalar/record, huge fanout, deep stack, support
array or schema body is an implicit window. The JSON value index is derived, never
replacement testimony. Verify full exact document length/hash before acceptance.

Prepare a compiled traversal of the exact interface record pointers once. Shared
or nested record sources are scanned once per distinct source and referenced by
views; duplicating required view memberships is charged as explicit incidence,
not copying full record bytes into every view. Maintain source result, document,
JSON pointer/record ordinal and selected input support for every derived record.
No hash-only substitution for evidence required by archive/recovery consumers.

Phases and durable tables (exact SQLAlchemy naming may follow owner conventions):

```
accept_attempt(job, result, profile_bundle, epoch, phase, cursor_ref,
               transport_complete, semantic_complete, structural_complete,
               version, state)
asked(delivery, subject_id PK, native_identity, port, source_ordinal)
coverage(attempt, view, subject_id PK, status, source_record)
record(attempt, source_pointer, source_ordinal PK, value_ref)
view_member(attempt, view, logical_ordinal PK, record_ref, subject_id?)
endpoint(attempt, lookup_pin, canonical_value_ref, subject_id)
relation_record(attempt, view, source_ordinal PK, primary?, associated?, record_ref)
relation_edge(attempt, view, primary, associated PK)
view_taint(attempt, view PK, all_covered_unsupported)
accepted(job PK, result_ref, attempt, terminal_summary)
```

LOAD/VERIFY -> SCHEMA -> SEMANTICS -> SUBJECT_VIEWS/STATUS -> ENDPOINTS ->
RELATIONS -> COVERAGE_MERGE -> SEAL_VIEWS -> PUBLISH. Independent phases may
pipeline only into private attempt rows. `step()` loads a bounded chunk, closes
its session, evaluates detached state, and commits rows/cursor using epoch/version
CAS. All writes are attempt-scoped. Duplicate chunk retry is a no-op; conflicting
retry rejects. A short publish checks terminal summaries and current authority,
then sets one accepted pointer. It must not execute COUNT, global anti-join,
whole-document hash, schema validation or per-subject insertion at publication.

Structural parity requirements, not simplified substitutes:

* **Subject views:** exact asked identity membership; one-per-subject rejects
  missing and duplicate records. Many-per-subject retains every original record
  and independently validates coverage status. Do not infer complete coverage
  from the number of records or presence of one row.
* **Statuses:** exact status set for the covered subjects; no duplicates, unknown
  IDs or wire statuses. Unsupported/ambiguous/insufficient remain indeterminate
  for ordinary fact predicates. A complete zero-row domain is distinct from
  missing evidence. All statuses are provisional until final relation taint.
* **Global view:** exactly the declared whole-question value; never emit a
  smaller-scope interpretation as another whole result.
* **Exact endpoints:** build an indexed canonical typed value -> subject map from
  complete subject views. Missing keys, incomplete lookup coverage or one key
  mapping to multiple distinct members reject exactly as native code. Several
  keys for the same member are permitted. Bool, numeric, string and byte encodings
  cannot alias by ad hoc stringification; digests use collision-safe exact-byte
  comparison. Long comparison keys require segmented comparison/index accounting.
* **Relations:** preserve native where/require three-valued logic, original row
  multiplicity/order and deduplicated edge set separately. A FALSE where skips;
  INDETERMINATE is not FALSE. Relevant unusable claims downgrade the resolved
  associated subject, or **all covered subjects** when associated is unresolved.
  Store a view-level taint bit then apply it during the final coverage pass; do
  not update N rows for each such claim (avoidable N*claims amplification).
  Only previously complete statuses downgrade; retain other statuses. Missing
  required endpoint fields and self-relations reject. Preserve the native
  handling of multiple primaries; G41/owning semantics decide ambiguity. Do not
  invent a UNIQUE(associated) restriction from the descriptive cardinality label.
* **Ordering:** native source ordinal order for rows, canonical pair order for
  deduplicated edges, exact accepted-view commitment order. Changing physical
  chunk/page size cannot reorder records or choose another witness.

The schema/semantic callback is an independent validator of provider testimony,
not an assertion that the observer says it passed. Controller resolves exact
registered contract-owned validators in the existing neutral composition model.
Those validators gain resumable read-view access; no importing observer executable
implementations into core or remote provider self-certification. Missing profiles
block acceptance. G02/G40 expressive-profile costs remain explicit delegates;
this design is not a promise of linear arbitrary JSON Schema or regex evaluation.

COVERAGE_MERGE traverses asked/status streams in exact order, verifying identity,
missing/extra/duplicate cases with a persisted pair of cursors. Sealing commits
records and coverage incrementally using existing ordered transcript rules where
unchanged. Independently checked obligations, not provider counts, justify the
terminal. A gap on the last page invalidates the whole attempt. Rejected attempt
rows never become accepted fact sources.

## 6. Evidence forwarding and retention

Replace `AcceptedEvidenceInput.pages: tuple[...]` at runtime with an
`AcceptedInput` authority reference plus verified bounded view readers. That
source retains the original request/result/task/interface and whole scope. A
selected disclosure is a distinct controller-owned acceptance artifact, not a
clipped observer result. Preserve per-view scoping: subject/status rows only for
selected members; relations only when all required endpoints are selected; global
facts require their original permitted whole scope, not automatic subset reuse.
Original unselected facts may remain privately retained but are not disclosed to
the next observer. No forwarding the original whole facts blob to a narrow grant.

Metadata identity verification alone cannot prove a filtered view is faithful.
Controller builds/seals the view, retaining original source support and exact
selection; downstream validates that authority and its delivered root/ordinals.
Nested predecessor descriptors are stored once by reference, not recursively
embedded at every successor. If the legacy view transcript embeds repeated support,
retain its logical meaning but commit/read it incrementally; proposed reference
wrappers must receive distinct current-only formats and freeze tests.

Prepared selections, required input documents, outcome and acceptance spools have
explicit refcount/lease owners. Pin dependencies atomically with a durable job
reference. A pointer deletion retires a root; physical reclamation is bounded
and cannot cascade through an entire history in one transaction. Do not keep one
connection per retained handle. Recover lease/epoch races and file/database commit
interruptions. Resource admission charges unique retained physical bytes AND
per-job references/index rows; deduplication is not unlimited free storage.

An observer's terminal retention must not expire the only result before controller
acceptance/custody transfer. Use bounded renewable read/transfer leases with
explicit expiry outcomes, and a durable controller receipt before releasing its
required local copy. A controller accepted planning result still has archive and
source-release obligations: keep the original exact request/options/testimony and
accepted-view preimages until the owning provenance/custody path verifies them.
Segment canonical provenance evidence through its owning G22/G26/structural
mechanisms; never insert a giant JSON assertion or replace required evidence with
a dangling digest. Metadata staging is not Riverhog durable archive custody.

## 7. Extent and complexity contracts

Record separate axes: tasks T, required task-subject/port incidences I, physical
delivery units D, unique source and evidence bytes B, result bytes F, source records
R, necessary view incidences V, explicit relations E, support bytes S, interface/
program size P, indexed state Z, actual retries/restarts K, control contacts C.

For fixed qualified interfaces: G38 preparation/delivery is O(I+B+D), plus declared
ordering/index and hash/validation costs. Transfers across genuinely distinct
consumers are counted, not labeled free shared work. G39 generic work is
O(I+F+R+V+E+S) plus indexed probes O(k log(2+Z)), canonical sorting/key bytes and
named semantic/schema delegates. If P varies, include P-dependent traversal and
compilation. Relation taint is O(E+I) logical work rather than E*I. Dense legitimate
edges/results still cost their size. Zero-match and last-record failures are
mandatory growth cases. No universal O(N) for arbitrary schemas or relations.

A control poll/refresh/cancel uses fixed-shape IDs/status and indexed state, plus
bounded current-authority work; it does not decode O(I+F) on every call. Complete
lifecycle includes publication, canonical encoding, transfer, validation,
checkpointing, spool reuse, failure replay, retention, cleanup and archive evidence.
At-most-once accepted coverage is not an exactly-once network/execution guarantee.

Bound RAM by explicit read/write/parse/index-cache windows plus profile-owned
workspace. Checkpoint roots/cursors are bounded metadata; large stacks/frontiers
live in paged private state. A byte budget alone is not CPU/SQL fairness: measure
VM/rows examined and cap cooperative substeps; native tool execution has qualified
containment/cancellation. Statement or worker safety deadline does not redefine
valid logical extent. Ordinary recoverable work replays at most uncommitted
quanta; complete input loss or expired required authority is explicit failure.

Preserve intentional contract maxima. `maximum_result_bytes` is not automatically
a page size: selected v2 meaning must charge canonical logical facts, execution
and required outcome detail bytes once, not merely the small reference envelope.
This logical-size rather than reference-envelope accounting policy is approved
for the current-only contract; the exact byte-inclusion/encoding formula and
vectors remain owner-reviewed implementation details. Never silently drop a
non-null advertised/requested cap. `None` introduces no new
logical result max. Control/body chunk limits and capacity quotas remain separate.
Do not hard-code new numerical resource defaults in design prose; bind typed
configuration authorities and qualify the effective conditions before acceptance.

## 8. Integration disposition

INTEGRATION.md gives owner files, provider ports, proof gates and commit order.
The scoped current-only protocol/identity/cap direction and neutral resumable
validator API are **approved** by #964 comment 6118314165. Precise owning
models, error cases, frozen vectors, implementation and tests still require review. Source-aware algorithms, state and test obligations are
specified here; diagnostic SQLite/AST models are not production implementations.
No separate G-family ledger or policy copy goes into AGENTS.md. Keep Alembic,
StateSchema, the single current v1_0001 baseline, explicit initialization and
synthetic future-revision/interruption tests; no abandoned-pre-v1 compatibility,
automatic state loss, dual readers or mainline changes by this reference.
