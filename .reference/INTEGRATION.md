# Integration entrypoints and challenge map

This is navigation and concrete design input for #969 under #903. The issue owns
scope, decisions and acceptance; this file does not add a second specification.
Keep these reference files out of authoritative main source.

## P00: verify the seam, then implement one real slice

1. Record the current authoritative source/ref, local worktree state and any
   relevant overlap with active integration. Compare it with the audited base in
   HANDOFF. Do not reset another agent's work or reapply old references.
2. In the repository's locked environment, run:

   ```sh
   python .reference/test_annotations.py
   python .reference/verify_schema.py
   python .reference/verify_schema.py --full-core
   ```

   The first two succeeded in the producer's available environment; the last did
   not reach runtime validation. A structural schema pass is not a substitute.
   Check actual attribution, local/foreign references, identity immutability and
   profile dependency closure in the canonical runtime before adopting anything.
3. Implement initial establishment plus one tag/description edit through the real
   storage publication owner and independent recovery. Stage immutable evidence,
   commit one selector conditionally, then update catalog projections. Inject
   interruption on both sides of selector publication and catalog acceptance.
4. Record exact results, limitations and the next focused owner in #969. Expand
   through P01-P04 below; a first slice does not complete the issue.

The new annotation component has its own structural root. Do not loosen rejection
of unselected journals, rewrite original member bindings, or fabricate a member
Artifact to make the existing artifact-specific journal_subject accept it.

## Ontological example to retain

A collection is represented here only in its role as a recordkeeping Context,
bound to the immutable source root. Its member set is archive structure. A
metadata-update Activity records an accepted classification/description action.
The current state is the projection selected by a committed checkpoint. These
are different referents and different kinds of evidence.

The example sequence is initial `unreviewed`, add `reviewed`, remove `unreviewed`,
set a description, replace it, clear it, then add `unreviewed` again. Historical
actions remain asserted; removing a current label does not retract the fact that
it was previously assigned. No artifact bytes, State or original journal changes.
The recorder and initiating software agent are distinct; neither is invented as
a human. No exact storage-commit or historical-content timestamp is fabricated.

Composition adds a new attributed extension relating an exact foreign member
State to that collection Context. It does not assert the collection's labels of
the member, claim new derivation/custody, or turn a historical relationship into
current membership elsewhere. Original journal bytes remain independently named.
The structural package pins the chosen annotation checkpoint and membership proof.

## P01: durable state and publication owner

Start with:

- `packages/riverhog-protocol/src/riverhog_protocol/collection_description.py`
- `packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py`
- `riverhog/src/riverhog_core/services/collection_descriptions.py`
- `riverhog/src/riverhog_core/services/collection_tags.py`
- `riverhog/src/riverhog_core/services/mutable_document_publication.py`
- `riverhog/src/riverhog_core/ports/archive_store.py`

Replace independent description/tag commit heads with one selector to an
immutable coherent checkpoint. Preserve separately typed fields and field-local
conditional operations. Add durable description operation identity as needed;
no-op replay outcomes belong to operational idempotency custody without creating
fictional historical edits. Serialize the common predecessor and fence the actual
provider effect. A stale in-flight worker cannot publish after a newer commit.

The model's conditional assignment is NOT a real adapter CAS or a database
transaction. Its input precondition is deliberately coarser than the required
field-level public behavior. Its successful no-op is not a durable request ledger.
Its Activity outcome describes the modeled accepted action only once the selector
commits it; unreachable prepared objects are not published history.

## P02: storage closure, retention and current authorization

Start with mutable-document reclamation, tag-node/revision cleanup, archive-copy
jobs/retirements/deletion, catalog sync and the state-schema fixtures. Published
ancestry becomes retained archive evidence, not an expiring catalog event feed.
Historical state dependencies cannot be garbage-collected as superseded current
metadata. Uncommitted orphan data can be reclaimed only after its real owner and
in-flight fences settle. Include full evidence in copy accounting and last-current
copy retirement checks.

Use immutable append-stable records/segments and bounded structures, not the
model's arrays, prefix joining, full-history scans or dictionary copies. Current
large initial tag sets need existing bounded tree/record machinery. Protect
ordinary edit growth independently from full recovery cost. Snapshots must not
become a way to discard earlier exact values.

Add the issue's narrow history-read permission, checked together with current
collection authorization on each server continuation. Current catalog-only access
must not automatically reveal deleted labels/prose. Historical tags never become
current grants. Offline possession of keys cannot be retroactively revoked.

## P03: recovery and self-contained composition

Start with:

- `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/member_history.py`
- `packages/riverhog-archive-contracts/src/riverhog_archive_contracts/source_binding_proof.py`
- `packages/riverhog-provenance/src/riverhog_provenance/history.py`
- `packages/riverhog-provenance/src/riverhog_provenance/corpus.py`
- `some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/recovery.py`
- `some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_metadata.py`

Give collection annotation history an explicitly selected root and required
structural dependency closure. Keep original member-history selection semantics;
a prior immutable source-root pin does not authenticate a future annotation head.
Do not union effective graphs or rewrite old journal sequences/identities.

Default recovery shares canonical history bytes. Optional standalone composition
must carry original member provenance, selected annotation history/snapshots,
real membership proof and new attributed composition evidence. Test verification
after deleting all source directories and sibling payloads. The reference uses
an existing real one-frame provenance example but a TOY membership document; it
has not exercised actual SourceBindingProof, archive-root parsing or encryption.

A selected complete old checkpoint is legitimate offline recovery, not proof of
global latestness. Missing committed middle/tail data is failure, not empty state.
An explicit salvage mode cannot publish the complete-recovery receipt. Preserve
metadata/history-only recovery without an unrelated payload read.

## P04: downstream handoffs and full acceptance

Reconcile public metadata/provenance readers, clients, download/materialization,
production admission/finalization, source history import and source retirement.
Carry the exact selected annotation checkpoint wherever complete retained context
is promised. A derived collection gets a new current annotation stream; source
context remains source context. Future source edits do not alter a pinned output.
Do not release source custody based only on an immutable member-history pin when
annotation closure is part of the accepted obligation.

Make the issue's coordinated pre-v1 hard cut: current contracts/state fixtures,
parser/API/client/CLI/recovery behavior, installed closure, fixed-byte vectors,
executable contract discovery and appropriate qualification. No old-format reader,
transitional dual head, version bump preserving retired pre-v1 behavior or silent
operator-state conversion. No live-data or release/provider action is authorized.

## Challenge evidence and its boundary

The 26 model tests exercise initial empty state; add/remove/re-add; descriptions
and clearing; no-op and stale preconditions; staging gaps; provider-before-catalog
interruption; duplicate/conflicting replay; tag/description contention; coherent
old copies and divergent ancestry; timestamps not sorting changes; missing/tampered
history; documentary/snapshot disagreement; retained and pending GC roots;
collection-versus-member scope; fully detached composition; pinned future
independence; duplicate-byte member distinction; wrong membership/closure; and
individual Unicode/value bounds.

These are executable counterexamples for design review, not acceptance evidence
for production concurrency, crypto, permissions, partial-write recovery, backend
semantics, real source proofs, complete history inheritance or growth. Use actual
owner-level positive and negative tests, not metadata assertions that a helper
exists. Run the current required gates and exact-source Actions checks, recording
scope and failures honestly. The issue's acceptance checklist is the stopping
rule; no general merge engine, erasure framework or core-ontology expansion is
required by this reference.
