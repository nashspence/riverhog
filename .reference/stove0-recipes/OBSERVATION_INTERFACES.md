# Exact observation interfaces and accepted views

Proposed companion contract for #953. This document is external reference input, not an accepted contract or an implementation. The source grammar prototype does not validate these documents or execute their semantics.

## Why this contract exists

Simply renaming `AssociationEvidenceSource` would leave recipe authors programming observer serializers. The replacement moves that knowledge into a focused interface owned with the observer/binding contract. A recipe asks for named inputs and consumes named views. The interface defines exactly how those names relate to accepted questions/results, including completeness. It is not an ad hoc runtime plugin or permission to run arbitrary transformation code.

The owner of the observer contract remains responsible for what its options and facts mean. A binding author can publish a separate interface only by pinning that exact contract and its required profiles. Stove0 validates declared structural operations and exact identity; it cannot claim new media, filename, provenance or operation semantics.

## Contract shape

The following closed algebra is the proposed structural authority to implement. `ExactRef` is `{id: SemanticId, sha256: Sha256}`. A schema/profile reference is to an exact supplied document, not a network URL. `Pointer` is RFC 6901. All named maps reject duplicate names and all unlisted fields are forbidden.

```text
ObservationInterfacePayload {
  format: "stove0-observation-interface/v1",
  id: SemanticId,
  observer_contract: ExactRef,
  facts_profile: ExactRef,
  semantic_profile: ExactRef,
  inputs: Map<PortId, SubjectPort | EvidencePort>,
  views: Map<ViewId, SubjectFacts | GlobalFacts | RelationView>,
  partitioning: "independent-subjects" | "whole-scope",
  empty_scope: "complete-empty" | "inapplicable",
  interface_semantics: ExactRef,
  conformance_vectors_sha256: Sha256
}
ObservationInterface = payload + interface_sha256

SubjectPort {
  kind: "subjects",
  option_ids_at: Pointer | null
}
EvidencePort {
  kind: "evidence",
  contracts: nonempty canonical set<ExactRef>,
  interfaces: nonempty canonical set<ExactRef>,
  covers: nonempty canonical set<SubjectPortId>,
  option_slots_at: Pointer | null
}

RecordSource {
  records_at: Pointer,
  nested_records_at: Pointer | null
}
SubjectFacts {
  kind: "subject-facts",
  records: RecordSource,
  subject_at: Pointer,
  record_schema_at: Pointer,
  coverage: "every-question-subject",
  cardinality: "one-per-subject" | "many-per-subject",
  status: StatusSource | null
}
GlobalFacts {
  kind: "global-facts",
  record_at: Pointer,
  record_schema_at: Pointer,
  coverage: "whole-question"
}
RelationView {
  kind: "relation",
  records: RecordSource,
  where: RowPredicate,
  require: RowPredicate,
  primary: SubjectEndpoint | ExactEndpoint,
  associated: SubjectEndpoint | ExactEndpoint,
  coverage: nonempty canonical set<SubjectPortId>,
  status: StatusSource,
  cardinality: "at-most-one-primary-per-associated"
}
SubjectEndpoint { kind: "subject-id", at: Pointer }
ExactEndpoint {
  kind: "exact-endpoint", at: Pointer,
  lookup: {source: "self" | {input: EvidencePortId},
           view: SubjectFactsViewId, keys: nonempty set<Pointer>}
}
StatusSource =
  {kind: "records", records_at: Pointer, subject_at: Pointer,
   value_at: Pointer, values: Map<WireStatus, NormalizedStatus>}
  | {kind: "semantic-profile", profile: ExactRef}
NormalizedStatus = "complete" | "unsupported" | "ambiguous" | "insufficient"
```

`facts_profile` and `semantic_profile` are equality assertions against the selected observer contract, not competing definitions. Reject mismatches. `interface_semantics` defines this structural algebra, normalization and completeness rules; its exact profile and vectors are dependencies of the compiled recipe. An interface digest commits to all of this. There is no unverifiable `trusted: true`, permissive default status or string naming a Python import.

`record_schema_at` selects a schema location inside the pinned facts profile, retaining its local reference environment. The compiler must verify that the selected JSON records conform to that view schema. Pointers are literal selectors, not query syntax. Nested arrays use one explicit nested record source or the closed row-predicate AST; arbitrary flatten/map/filter programs are out of scope.

A many-per-subject view must still prove coverage for every question subject, including subjects with zero records, through the pinned status/completeness profile. Record absence alone is not such proof. A one-per-subject view rejects missing or duplicate subject records. A global record cannot be used as per-member source-loss evidence. Interfaces whose questions have non-subject global semantics must declare `empty_scope: inapplicable` rather than fabricate a zero-input answer.

All view/lookup references are statically resolved and acyclic. If a required status cannot be derived by this algebra and an existing exact semantic profile, change the owning observer contract to expose it. Do not add observer-specific inference to Stove0 core.

## Question construction

For one task, resolve every subject port to an exact canonical member set. The question's subject domain is the union of subject-port members, with each member's immutable identity and role retained. A member may appear in multiple ports only when the interface semantics explicitly permits overlapping roles; the union contains it once. The present reference profile requires distinct relation primary/associated partitions.

Begin with the task's literal options. A subject port with `option_ids_at` inserts the exact subject IDs at that location. An evidence port with `option_slots_at` inserts the generated accepted-evidence slot labels. Verify all generated destinations are valid object fields, pairwise disjoint after JSON Pointer decoding, and do not collide with static options. There is no overwrite precedence. These generated slots are representation details carried by the interface, never repeated in recipes.

The resulting options must validate against the exact observer contract. Their identity, selected subjects, task identity, contract/interface identity and permitted read actions define the logical question. Provider-specific transport settings and physical request partitions are separately bound in the approved attempt. Evidence-input references commit to the predecessor task and exact accepted evidence set; a task does not recompute another task's question from current deployment configuration.

The task graph guarantees predecessors complete before question creation. Validate forwarded evidence against both the predecessor question and the consuming port: required exact contracts/interfaces, selected subject coverage, valid result states and option semantics. Do not accept arbitrary JSON merely because it has a `facts` field.

An interface with `independent-subjects` asserts that its per-subject results can be evaluated independently and assembled without changing meaning. Its vectors must demonstrate that claim. `whole-scope` means partitioning cannot turn absent cross-partition relationships into negatives; physical processing may be paged, but the logical question and completeness authority remain the whole scope. A preferred batch size never changes the scope. Physical quotas may explicitly defer/reject admission, not return partial facts as complete or call a large valid recipe malformed.

## Accepted evidence set and view identity

Separate three objects:

1. **Logical task**: task ID, work ID, exact logical question/scope and selected contract/interface.
2. **Accepted evidence set**: every physical request/result needed to answer that logical question, with exact request IDs, result hashes, descriptor identities and coverage. Sealing requires no omissions, conflicting results or duplicate subject answers unless explicitly declared by the interface.
3. **Accepted view**: a typed, deterministic interpretation of that complete accepted set under one exact interface view and an exact selected scope.

A conceptual evidence-set record is:

```text
AcceptedEvidenceSet {
  work_id, task_id, question_sha256, interface: ExactRef,
  scope: exact paged subject-selection authority,
  state: "complete" | "complete-empty",
  results: canonical paged set<{request_id, result_sha256}>,
  evidence_set_sha256
}
AcceptedViewRef {
  evidence_set_sha256, view_id, selected_scope_sha256,
  view_semantics: ExactRef, view_sha256
}
```

This is a proposed protocol addition, not a claim that those objects already exist. Reuse the project's paged content-addressed authorities rather than inline unbounded records or invent a logical-total limit. Actual retained result objects remain the original objects. The view's support links must identify them; a view is not a new `ContentObservationResult` pretending that the observer answered a smaller question.

A consumer can select a subset of a complete subject-keyed view without depending on producer batch boundaries. Its view reference binds the exact source evidence set plus selected scope. Never copy a subset of `facts` into an original result envelope and reuse its digest. Full-result reads require authority over their full scope. Where only a scoped view is authorized, serve a distinct, verified view artifact/reference with explicit source support under the controller's existing acceptance authority; do not silently widen byte/provenance capabilities. Define and test that read boundary in the focused protocol/support packages during integration.

The controller validates source identity, schema, semantic profile, view extraction and scope before sealing a view. It does not assert new domain facts. Targets/observers consuming a view verify that it comes from the approved accepted set and exact interface, not simply trust a view label. Preserve the original evidence for authorized audit and replay.

A complete-empty record is controller task completion over an exact empty domain, not fabricated observer testimony. It permits empty subject-keyed views only where the interface's empty-scope rule says they are the identity case. It cannot satisfy a global verdict or a per-member affirmative loss rule for a nonempty input.

## Relation semantics

`where` selects a named relation class within otherwise valid records. `require` states extra validity needed to use a selected relation record. A failed requirement on a relevant relation is unsupported/indeterminate, not absence. This preserves the difference between a complete negative and a claimed relation that cannot be interpreted safely.

Subject-ID endpoints must resolve to the exact requested member instance. Exact endpoints are looked up through pinned subject facts using declared keys such as `/state` and `/occurrence`. Hash/compare those endpoint JSON values with the canonical authority. Reject one endpoint resolving to several member instances, missing lookup coverage, unknown subjects, primary=self-association where prohibited, or mismatched request scope. Never fall back to artifact bytes, filename text, collection ID alone or a partial provenance locator.

Normalize statuses only through the declared mapping or exact semantic profile. Unknown wire status values fail. Missing/duplicate status rows fail. Semantic-profile status extraction must prove the same per-subject completeness obligation and have exact vectors; it cannot be an implicit `all complete` shortcut.

For each associated member and preference tier, verify evidence covers the exact primary/associated candidate universe. A supported single matching primary attaches that member. Multiple matching primaries are ambiguous. Unsupported/ambiguous/insufficient evidence blocks that tier's decision; it cannot activate the next tier. A complete supported answer with zero matches permits the next tier. If a blocked result cannot identify which primaries it affects, conservatively block all potentially affected primaries and expose that explanation. Only complete negative tiers can ultimately leave a primary without an attachment.

The facts profile may prove globally complete canonical provenance without explicit status rows. In that case the interface must pin the exact semantic profile that makes absence authoritative; it must not infer it from JSON Schema shape alone. Filename sidecar matching similarly remains in the filename observer, whose profile defines candidate rules and complete statuses.

## Worked mappings, not deployable pins

These sketches omit actual digest values deliberately. Integration must generate interface documents from the maintained exact contracts and vectors; copying fictional 64-character hashes would conceal missing proof.

**Media metadata**: one `subjects` port without option injection; subject-facts view `records` reads `/artifacts`, keyed by `/artifact_id` as the observer's declared question-subject ID, with the existing metadata row schema. Nested metadata arrays remain explicit row data. Classifying XMP uses `items /facts` with name and value tests in one same-row condition. Do not reinterpret an opaque Riverhog artifact ID as an observation subject ID just because both fields historically contain the word artifact.

**Stream probe**: one subject port and a subject-facts `records` view for `/artifacts`. Its row schema exposes `has_audio` and `has_non_attached_video`. The recipe selects the `media` role at the subject port. A producer batch is complete only when every requested subject has its declared answer/status.

**Canonical provenance**: one subject port. A subject-facts view exposes the exact member `/state` and `/occurrence` endpoints. Relation view `describes` reads `/artifacts` then `/claims`, selects the canonical describes predicate, requires a reference-valued claim, and resolves `/subject` and `/object` through exact endpoint keys. Its completeness status comes from the pinned provenance semantic profile. The predicate URI and endpoint semantics belong to this observer/interface, not the Stove0 classifier.

**Filename sidecars**: subject ports `primary` and `associated` inject `/primary_ids` and `/sidecar_ids`; evidence port `provenance` injects `/provenance_slots` and requires the exact canonical provenance interface/contract over their union. Task options carry the explicit suffix policy. Relation views `full_leaf` and `stem` read `/candidates`, filter `/rule` on the corresponding exact wire value, map `/primary_id` and `/sidecar_id`, and consume complete `/statuses` keyed by `/subject_id`. Wire spelling such as `full-leaf` can map to readable view name `full_leaf`; that mapping is pinned once in the interface.

**Materialization hints**: expose declared hint facts as a subject view; absence/availability has the hint observer's exact meaning. Forward accepted evidence/view references to targets. `allow_missing_materialization_hint` remains an explicit target option, not evidence synthesis in core.

**Source-loss policy**: a subject view exposing a declared verdict can support loss only when its exact evidence set covers every discarded member and each required slot has the affirmative verdict. A normalized `complete` status alone is not approval; an `every` condition selecting an action is not a substitute for the per-member settlement proof.

## Required interface qualification

Positive vectors must cover independently batched and whole-scope questions, empty scopes, repeated same-contract tasks, nested records, exact endpoint lookup, complete-negative fallback and view subset selection. Negative vectors must include forged digests, mismatched option schemas, stale semantic profiles, duplicate/missing subjects, wrong role partitions, unknown statuses, missing endpoint support, cross-scope evidence, generated/static option collisions and ambiguity masked as absence.

Generate docs and named port/view schema help from the same production authority. Put pure structural interpretation/validation in a focused shared contract/support package. Keep accepted-set storage, scheduling and approval in Stove0; keep media/provenance/filename meanings in their independent owners. The source language linter's mock `views` summaries are neither an interface implementation nor a substitute for this qualification.
