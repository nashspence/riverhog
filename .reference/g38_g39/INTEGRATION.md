# G38/G39 integration and acceptance map

Read DESIGN.md first. This is #903 reference input for #964, not a mainline task
ledger. Owner decisions and exact integrated source control implementation.
[#964 approval 6118314165](https://github.com/nashspence/riverhog/issues/964#issuecomment-6118314165) authorizes the
specified current-only request/result/status/evidence/validator hard cut.
Treat that scoped policy as settled; exact wire fields, resource/cap vectors,
owner pins, regression tests, independent validator backends and security/
custody/authority gates remain normal reviewed integration obligations.
Do not infer approval for a broad observer language change or new size ceiling.

## 1. Order and scope of integration

1. Implement the **approved scoped** reference-based request/result/status,
   verified metadata handoff, logical result-cap accounting and resumable
   validator API; review exact models and vectors at their owners. Preserve
   #948's actual integrated control semantics and #953's exact language semantics.
   Enumerate exact Closure operations/delegates from source, not this file's G IDs.
2. Introduce verified bounded document handles and exact hash continuation in the
   neutral owner. Replace bulk handoff only for these operations; retain unrelated
   metadata contracts. Land a real subject-selection reader/order-parity witness.
3. Controller preparation/reservation and immutable manifests; component-side
   input staging, verification, fixed status/control persistence and scoped reads.
4. Resumable exact observer execution and validator sessions, starting with core
   provenance and filename-sidecars; then all supplied independent observers.
5. G39 private acceptance phases, relation/status/endpoint indexes, exact coverage,
   short accepted-pointer publication, downstream scoped readers and custody export.
6. Full native vectors and component integration, restart/authority/extent/growth
   witnesses, current-baseline generation and the ordinary exact-SHA rail.

Do not land a mandatory large-scope path whose only supported observer or validator
still takes a whole `dict`, `tuple` or `list`. A temporary integration checkpoint
is not a supported release or evidence of composite conformance.

## 2. Concrete owning files

Paths below are relative to `some-implementations/stove0/` unless otherwise stated.
Add focused modules at these owners rather than importing implementation code
across package or server/component boundaries.

| Owner / existing seam | Selected changes |
| --- | --- |
| `packages/protocol/src/stove0_protocol/models.py`, `observation_evidence.py`, `accepted_inputs.py` | Closed reference envelopes, fixed control status, exact source/result distinction; remove embedded subject/schema/facts/evidence-page totals from control carriers. Retain source testimony identities in scoped acceptance. Add `observation_streams.py` for neutral read-view protocols and exact document-binding models. |
| `packages/protocol/src/stove0_protocol/observation_interfaces.py`, `observation_views.py` | Compile record/port/status/endpoint access once per exact interface; read-view execution, indexed structural validation and view-level unresolved-relation taint. Preserve order, edge dedup versus source multiplicity, and empty semantics. No handwritten second DSL inventory. |
| `packages/observer-protocol/src/stove0_observer_protocol/{validation,interfaces,conformance}.py` | Resumable schema/semantic/status sessions, exact owned conformance vectors, new control/data operation schemas and error maps. Provider validation and controller acceptance remain independent executions. |
| `application/server/src/stove0_core/{compiled_observation_delivery,observation_questions,compiled_observations}.py` | Build question/delivery manifests in background quanta, reserve subject incidence atomically, reuse complete prepared inputs, never build full `subjects`/options/forwarded evidence during scheduler passes. |
| `application/server/src/stove0_core/{metadata_selections,compiled_planning_state,work_metadata}.py` | G37 bridge: sealed native-order selection plus exact indexed subject-order view, port joins and constant reference lookups. Reuse prepared roots; do not scan growing coverage for every next batch. |
| `application/server/src/stove0_core/{accepted_observations,accepted_view_queries}.py` | Private acceptance attempt tables, bounded independent verification, terminal evidence, constant publish; no bulk evidence_json or record_json payload that requires reading a whole variable record. Expose closeable scoped views with retained original support. |
| `application/server/src/stove0_core/{observation_state,control_contacts,coordinator,planning_progress}.py` | Compact delivery status/reference; bounded contacts and aggregate pass; separate continuation due from claim renewal. No complete request decode on poll or callback. |
| `packages/observer-support/src/stove0_observer_support/{persistent,runtime,results,http_binding}.py` | Small durable declaration/status and epoch-fenced execution; input/output handles and streaming result builder; chunked verification/pruning. No `.status.json` containing a complete result, `subjects()` tuple or `.open_evidence()` all-pages tuple. |
| `packages/observer-client/` and `packages/api-client/` where Closure identifies observers/evidence | Resume exact transfer/job/read authorities, distinguish provider completion from controller acceptance, stream export/support, preserve cancellation and refresh. No convenience method that secretly collects the entire default result. |
| root `packages/http-api-contracts/src/http_api_contracts/{metadata_documents,metadata_staging,metadata_binding}.py` | Keep existing document/chunk identity and carrier. Add scoped verified-handle handoff and bounded canonical verifier, not another metadata registry. Avoid `_read_exact`, `execute_model` and whole response capture on the new path. |
| affected owners' `v1_ddl.py`, `state_migrations/versions/v1_0001.py`, models, state guards | Revise current application baseline together; preserve Alembic/StateSchema and synthetic forward-only tests. Never drop operational state automatically or create pre-v1 compatibility chains. |

## 3. Minimal focused wire additions

These are proposed exact operations, not deployed routes. Declare them beside the
existing observer operations; use existing typed error/status and metadata wrappers
where their meanings match. No generic RPC, arbitrary callback path or remote URL.

* `PUT /v1/observations/{job_id}/input-transfer`: small non-secret job declaration
  from DESIGN plus transient runtime authority. Idempotently bind its manifest;
  respond with bounded `pending | ready | denied`, transfer ID and a transient
  scoped credential when ready. Pending verification does not occupy an execution
  permit. Repeated identical PUT refreshes authority without replacing input IDs.
* Existing metadata document PUT/GET/chunk routes carry the transfer credential
  out of band. It binds component, job, direction, exact manifest and current
  claim/fence. The selected worker builds the allowed-document index by bounded
  traversal of verified manifest refs. It cannot attach unlisted data to a job;
  staged partial bytes remain undisclosed until verification. Admission quota
  applies even to incomplete or malicious manifests.
* `PUT /v1/observations/{job_id}` admits/refreshes the fixed job only after input
  verification, or reports pending preparation with no execution. Existing GET
  polls compact status; `/cancel` consumes the compact accepted declaration.
  Retain #948 terminal precedence, interrupted refresh and attempt fencing.
* Completed status contains exact result-header ref. Existing scoped metadata
  reads deliver header/facts/execution detail by offset; a fresh claim check is
  required on acquisition and each chunk. Hash knowledge is not authentication.

Use component-local opaque transfer credentials, issued only after the existing
live Riverhog capability/claim preflight and registered controller/component
binding. Store only protected operational verification material, not credentials
in semantic docs. Refresh is a bounded native control operation. The ordinary
live-root preflight currently checks one selected root; preserve that role as
liveness validation, **not** a proof authorizing every subject. Independently
verify manifest subject/claim scope during preparation, and retain actual current
per-artifact/provenance checks at each data access. Multi-root selection is not
silently narrowed to the first root. A stale transfer or job ID cannot refresh a
new claim. Input metadata must never become a way to read arbitrary Riverhog data.

A lost response retries the same reservation/delivery ID. A claim/fence change
creates a separately bound job; verified immutable input bytes may be reused
under new authorization. Accepted old evidence is not silently reassigned to it.
Failed/canceled/inapplicable provider results keep their own terminal meaning.
HTTP `202` is appropriate for asynchronous verification, but is not semantic
acceptance; typed pending state is mandatory even where existing wrappers use 200.
Return bounded failure codes plus a scoped detail ref, not a giant exception body.
No offset or transfer state in the semantic request hash.

## 4. Supplied observer adapters

The exact installed descriptor/contract/interface pins, not names in this table,
control runtime resolution. Update registered conformance vectors with the ABI.

**Canonical provenance (`observers/riverhog-provenance`).** Core facts remain one
whole-scope question. Capture exact source-selected provenance through G22/G24
readers; close each source handle after bounded capture. Share unique exact
snapshots within the job instead of one full validated journal per member.
Store artifact records, endpoint/claim source coordinates and original evidence
without expanding selected shared history per delivery. Relation resolution can
join across distant input chunks; complete statuses must wait for full declared
history. `bound-and-required-history` remains the supported selected extent where
the existing core observer contract says so; do not convert it to all retained
history or silently weaken a request for a broader extent. The materialization-
hint interface remains independent-subjects, with exact current hint semantics.

**Filename-prefix-sidecars.** Keep read-evidence-only input and exact core-provenance
predecessor pins. Replace the all-artifact list, all-locator dict, generated ID
arrays and all-candidate list with indexed accepted-input readers and output spool.
Compile actual filename pairing keys once at the component, including filesystem
syntax/context and current longest-prefix/suffix rules. Use keyed exact lookup /
external ordering rather than all-primary by all-sidecar comparisons. Preserve
all source support, ambiguity/insufficient statuses, duplicate-equal names in
different contexts and both full-leaf/stem tiers. No controller filename inference,
no rerun over a clipped predecessor posing as a smaller original observation.
Relations crossing any physical chunk boundary must survive.

**Magic, ffprobe, exiftool and other selected profiles.** Independent-subjects
batching remains allowed by the exact interface. Iterate subjects and write
records incrementally; do not accumulate all tool outputs into one dict. Keep
tool/profile native output and subprocess/resource/time semantics separately
qualified. A single subject can have a large metadata/result document; a subject
count is not a byte/memory proof. Temporary provider arrays of declared small
extent are allowed only under that owner’s explicit limit and test. No claim
that arbitrary extensions are automatically streaming because the client is.

**Contract validators.** Begin with record/object/array shapes used by supplied
profiles and exact indexed global invariants. Their independently owned validators
must consume read views and resumable aggregate/index state. Schema validation is
not replaced by trusting projected rows. Bind every advertised required schema and
semantic validator to a qualified implementation. General expressive-profile cost
and restrictions remain G02/G40 decisions; a missing delegate blocks composite
acceptance, never falls back to accepting unvalidated testimony. A slow but honestly
declared, bounded-step delegate is different from an unbounded callback.

## 5. Acceptance matrix

Required native witnesses include:

* Whole-scope provenance-to-filename pipeline with two complete relations whose
  endpoints occur in different chunks and different source roots. Vary physical
  byte/row windows; compare full native accepted facts, statuses, row order, edge
  set, input groups and source support against an independently executed small
  baseline. Fixture growth must include actual valid profiles and journals.
* Subject IDs ordered differently from `_artifact_key`, disjoint ports, empty
  scopes, identical payloads with distinct source members, replaced roles/roots,
  duplicate/missing/extra terminal coverage and multiple tasks using one provider.
* Corrupt/gapped/reordered/duplicated segments; wrong total/digest; same ID changed
  manifest/options/interface; matching row counts but wrong members; valid transport
  plus invalid schema/semantic/profile; final-page error never exposes a view.
* Subject one/many cardinality and statuses; global value subset prohibition;
  exact-endpoint type and ambiguity; rows versus deduplicated edges; relevant
  indeterminate claims with associated-unresolved global taint and late statuses.
  Compare native status downgrades, not a renamed custom Boolean success signal.
* Huge single scalar, record, nested source, support array, generated port, literal
  options and schema document. Cross the existing transport threshold without
  changing logical result; test intentional result maxima below/at/above their
  owning exact byte formula. No merely larger fixture threshold as contract.
* Data read after revoke/expiry, different controller/component/job/claim, guessed
  digest and cross-scope predecessor, secret redaction, no source loss, original
  evidence archival custody and independent recovery before scratch deletion.
* Actual worker kill between segment fsync/database marker and between private
  acceptance commit/accepted-pointer publication; old epochs cannot publish;
  retry resumes exact progress, duplicate execution does not duplicate acceptance.
  Test canceled-before-first-PUT and already-completed evidence precedence.
* Paused/slow consumer, dormant provider/resource wait, small concurrent work,
  current claim renewal, queue/physical-byte quotas, lease expiry and bounded GC;
  instrument pool return and SQL transaction/rows/work, not just page length.
* SQLite and PostgreSQL real plans for deep subject coverage and endpoint lookups;
  no per-poll growing JSON decode, per-batch whole coverage scan, quadratic taint,
  unbounded ready publication or generation-sized cascade.

Use three independent result categories through #963: functional correctness,
growth/evidence validity, and elapsed time. No invented speed SLA or automatic
qualification waiver. All four local pre-push gates, exhaustive local integration
rail where required, exact-SHA independent CI/CodeQL and timing disposition remain.
