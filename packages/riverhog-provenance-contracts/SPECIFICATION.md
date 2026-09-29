# Riverhog provenance: normative path-independent contract

**Distribution names:** `riverhog-provenance`, `riverhog-provenance-contracts`  
**Current package versions:** `0.1.0` (unchanged)  
**Wire schema version:** `1.0.0` (unchanged)  
**Profile:** `https://nashspence.github.io/riverhog/v1/provenance`  
**Status:** branch-local hard-cut reference contract; no compatibility or migration layer.

The capitalized terms MUST, MUST NOT, SHOULD and MAY describe requirements of this application profile. This document, the packaged JSON Schemas, and the explicit application invariants jointly define conformance. The Turtle ontology is an alignment, not a replacement validator. A validator establishes coherence of the supplied assertions; it cannot establish historical truth, source trustworthiness, or legal custody.

## 1. Scope and boundary

The subject is an **opaque, bounded digital artifact occurrence**, not necessarily an operating-system file. Valid histories include bytes emitted once, database values, versioned objects, repository items and filesystem objects. A subject need not have a name, address, physical-host identity, native metadata, discoverable predecessor, or surviving payload.

The format records attributed descriptions, primary byte length and fixity when measured or reported, source-qualified native metadata, optional technical profiles, responsible Agents, known processes, contextual addresses, transfer/delivery evidence, availability/custody assertions, and recordkeeping corrections.

The format does not interpret primary bytes as media or documents. Codec, duration, EXIF, page structure, document text, perceptual similarity and semantic equivalence are outside this core. An externally evidenced process label such as transcode is permitted; deriving that label by parsing the payload is not a core observer operation. Parsing source-native ACL or xattr structures is interpretation of metadata evidence and MUST remain distinct from the raw evidence.

No restoration semantics, canonical repository path, archive tree, globally current replica, synchronization protocol, signature format, encryption scheme or storage backend is prescribed.

## 2. Semantic universe

Let A be artifact identities, O occurrences, S fixed states, D description records, X activities, G Agents, C contexts, Q assertions, J journals, and E journal entries. These are identifier roles, not source naming systems.

For every local occurrence o there is exactly one declared membership `artifact(o) ∈ A`. For every local state s there is exactly one `occurrence(s) ∈ O`. These are custom membership relationships under a declared continuity policy. Neither relationship entails `prov:specializationOf`, `prov:alternateOf`, derivation, or `owl:sameAs`.

Every active graph record has an immutable **assertion identity** q and a separate **referent/record identity** r. q denotes the attributed claim bundle; r denotes what that bundle describes. Entry and journal identifiers MUST NOT be reused as graph assertion or referent identifiers. Assertion IDs and referent IDs MUST also be distinct.

An assertion may describe an entity, an activity, an Agent, a description record, or a qualified relationship. An assertion is not automatically the thing it describes.

### 2.1 Artifact

An artifact is a continuing digital-artifact identity assigned under an attributed `continuity_policy_uri`. It groups occurrences without claiming that all have identical bytes, all belong to one intellectual work, or all are revisions of one filesystem object.

Identity may be assigned at first observation. This MUST NOT imply that the subject previously carried that identifier or participated in a managed system. A separately retained derivative will normally receive another artifact ID with an explicit derivation relationship. An archive MAY use another declared continuity policy, but MUST NOT infer continuity from a path, native identifier or digest alone.

### 2.2 Occurrence

An occurrence is one realization or exposure of the artifact. Its kind is `filesystem_object`, `object_store_object`, `repository_object`, `database_value`, `stream_emission`, or `opaque`. Independent retained replicas MUST have different occurrence identities even when their bytes match.

An occurrence may have a source context. Source-native identifiers, when retained, are observations qualified by an identifier scheme and naming authority, not replacements for the occurrence ID. A context-scoped inode, SID, object key, device number or row identifier MUST identify its authority explicitly.

A move within one namespace may preserve occurrence identity according to the declared policy. Materialization into another independently retained object normally creates a new occurrence. The core MUST NOT decide this by comparing addresses.

### 2.3 State

A state identifies fixed aspects of one occurrence. The state's content and native metadata are described by evidence records; they are not a mutable property bag on its identifier. A state has explicit extent semantics: a whole object, a complete finite emission, a bounded segment, or an unknown historical extent.

A segment MUST declare a boundary policy and start/end boundary values. A direct observation of an unknown extent is not conformant. A historical state with unknown extent is allowed and need not have a digest or artificial capture.

New measurements SHOULD use a new state ID unless they intentionally refer to an independently identified fixed state. A real change to state-defining bytes/native aspects requires another state identity. A locator-only change is a separate binding change; it does not by itself establish a content revision. A rename that also changes captured native metadata can additionally produce a new state.

A state ID is not a content address. Equal length and SHA-256 support matching-fixity evidence, not identity, direction of copying, or equality of native metadata.

### 2.4 Description and observation

A description is an immutable attributable record about a state. A direct `observation` requires a local bounded state, an observation activity, complete primary-byte length/SHA-256, coverage, source capabilities, address availability status, and consistency evidence. Multiple observations MAY describe the same state.

A `reported_description` may describe an unavailable predecessor or a foreign state. It requires attribution/evidence, but no manufactured capture, content digest, source location or origin event. Reported length/fixity, when present, follows the same content syntax and remains a report rather than a new direct measurement.

Conflicting content descriptions of one state are retained as separate records and reported as findings. A consumer MUST NOT silently select the latest description as the true state or collapse matching descriptions into one referent. Other graph-inconsistent assertions may require separate bundles or explicit correction before acceptance into one effective graph.

The source is used by a capture; the observation record is generated by that capture. Therefore:

```
capture       prov:used             state
observation   prov:wasGeneratedBy   capture
```

The capture MUST NOT be asserted to generate its observed state merely because it measured it. The entry that later records or aggregates the observation is a separate recordkeeping bundle.

### 2.5 Activity and Agent

An activity describes an actual or reported process: observation, creation, transformation, copy, transfer, materialization, metadata update, rename, removal, custody change, recording, unknown, or URI-defined other. It has at least one responsible Agent association with a role. Missing precise process identity or time MUST remain absent/unknown, not be inferred from difference alone.

Agents are software, persons, organizations, or deliberately responsibility-bearing devices. A source context or host MUST NOT be promoted to an Agent merely because the bytes were stored there. Process execution, source exposure, destination, and recordkeeping contexts are separate qualified roles.

Primary fixity measurement requires a successful or partial observation activity. A failed capture cannot emit a conformant direct observation claiming complete primary fixity. Partial native coverage requires a partial activity outcome. Policy-driven withholding is not automatically a failed read.

### 2.6 Context

A context identifies a naming, source, repository, database, object-service, stream-session, execution, delivery, recordkeeping or opaque context. A context need not identify a physical computer or filesystem. Optional profiles describe known properties without inventing underlying implementation details.

The source context is where/how the occurrence was exposed. The execution context is where the observer ran. A Linux process reading a remote object does not establish that the remote object is a Linux file. A recording context identifies recordkeeping, not necessarily source execution.

## 3. Assertion representation and evidence

All graph rows contain `assertion_id`, `id`, `type`, and nonempty `evidence`. Evidence identifies the asserting Agent and a basis: direct measurement, process record, imported record, attestation, inference, identity assignment, or unknown. Attestation/inference/unknown bases require explanatory notes. A source identifier or exact source reference may accompany the claim. Confidence is optional evidence metadata, not a validator probability or truth ranking.

Graph categories are `artifacts`, `occurrences`, `states`, `agents`, `contexts`, `activities`, `descriptions`, `relations`, `locator_bindings`, `delivery_associations`, `custody_assertions`, `availability_assertions`, `journal_subjects`, and `extensions`.

Arrays are semantic collections, not chronological lists. No absent category implies that nothing of that kind ever existed. All local references MUST resolve in the effective graph, including references introduced atomically in the same entry. Earlier history may instead be represented by an explicit partial historical entity or an exact foreign reference.

The effective graph permits one accepted description row per referent ID. Repeated measurements use distinct description IDs. Fixed identity aspects cannot be changed under the same referent ID by correction. These include occurrence membership/kind/source context; state membership/extent; artifact continuity policy; and context/Agent kind. Other records are immutable apart from replacing their attribution assertion with an equivalent referent description.

## 4. Opaque primary bytes and typed values

`content` contains a canonical decimal-string `size_bytes` and a digest list with exactly one SHA-256. SHA-512 and URI-defined algorithms may be additional evidence. Duplicate algorithm designations are prohibited. The mandatory SHA-256 and size apply only to the declared bounded primary byte sequence, never implicitly to xattrs, alternate streams, resource forks or related replicas.

Direct capture MUST consume the declared boundary, reject an incomplete read, and record complete primary fixity. A source does not need seeking or a second pass. A bounded segment does not imply anything about bytes outside its boundaries.

Native evidence and extensions use a closed tagged union:

- `bytes`: canonical base64 and exact byte length;
- `text`: portable Unicode text;
- `integer` and `decimal`: exact canonical numeric strings;
- `boolean`;
- `timestamp`: normalized UTC with up to nanosecond precision;
- `uri`;
- `reference`: a local or pinned foreign referent;
- `json`: an exact profile reference plus portable JSON data;
- `digest`: byte length plus fixity of unretained native bytes.

Bytes, integer strings and decimal strings MUST NOT be silently converted to potentially lossy JSON numeric values. Base64 alphabet/padding, zero padding bits, and decoded byte length are checked. A digest-only record preserves verification evidence, not the omitted bytes.

The portable wire subset excludes null, floating-point JSON tokens, integers beyond the RFC 8785 interoperable integer range, U+0000, lone surrogates, Unicode noncharacters, duplicate object members, and nesting deeper than 96. Absence is represented by omission or explicit evidence status, not null. Source units that cannot be safely represented in text use a bytes representation with an explicit encoding label. Display text is not identity and MUST NOT replace the source units.

Sequence/count fields use unsigned canonical decimal strings up to 2^63−1. Arbitrary exact metadata integers/decimals are strings and MUST remain text unless a consumer explicitly checks its own numeric range. These rules permit clean PostgreSQL JSONB storage without using JSONB normalization as the authoritative journal representation.

## 5. Native metadata and optional profiles

A native row records a vocabulary `profile_id`, category URI, source-qualified exact name, source interface/field, retention status, sensitivity, optional byte length, optional raw value and separately attributed interpretations. Namespace URI is optional. A literal source namespace/name is preserved through the exact name and source evidence, not invented by URI normalization.

Native row identity includes profile/category, namespace when present, exact name and exact source. A display rendering alone cannot distinguish two native names. Source context, interface and field prevent similarly named observations from different mechanisms being silently merged.

Statuses are `captured`, `digest_only`, `withheld`, `unreadable`, and `not_retained`. The first two require the appropriate value; the latter three prohibit a retained value and require a reason. Unreadable native evidence requires partial/failed coverage. Unknown or unavailable data MUST NOT be converted to an empty byte value.

Interpretations identify an Agent and a method URI, and retain their own typed values. A parsed ACL, SDDL or decoded xattr is not the raw source bytes. Structured interpretations require the same digest-bound profile envelope as any structured extension.

### 5.1 Coverage

Coverage is scoped by `(profile_id, category)`, not a universal filesystem checklist. Statuses are complete, partial, failed, not exposed, not applicable, not requested, withheld and unknown. All but complete require a reason. A direct observation always includes complete primary-content coverage. Each native row must correspond to a declared category; rows cannot contradict not-exposed/not-applicable/not-requested coverage.

Complete means accountable coverage relative to the recorded Plan, interface, privileges and retention policy. It is not a claim to know every possible property in every system. “Not exposed” describes the observation interface; it does not establish that no such metadata exists in the underlying service or earlier history.

### 5.2 Exact profile contracts

A structured profile value includes `(contract_id, contract_sha256, schema_id)`. The pack digest covers the canonical schema pack and its reference policy. All referenced schema resources must be supplied in that pack/catalog. The validator MUST NOT fetch schemas or execute code based on schema URLs. Root resource IDs are explicit; nested `$id` resource rebasing is prohibited.

Unknown exact packs fail strict validation. Inspection mode MAY retain their data and list unresolved profiles, but MUST NOT claim profile verification or interpret lookalike field names as core syntax. A schema URI alone does not identify the hard-cut contract.

The current named filesystem profile contains normalized timestamp observations, principals, native identifiers, and POSIX mode when actually observed. Raw filesystem metadata uses the generic native-row contract, not a separate value language. Every normalized timestamp retains source semantics, resolution when normalized, and raw representation/assumptions when needed. Metadata-change time is not treated as creation time.

A filesystem observation profile requires an occurrence declared as a filesystem object. Filesystem source context and observer execution profiles remain separate and optional. Neither profile is required for a pathless observation or a historical report.

## 6. Location and designation

A locator binding is an attributed relationship connecting an occurrence or state to a typed designator in a naming context, with a temporal scope. There can be zero, one, or many bindings. A binding is not identity and need not remain resolvable.

Designators are filesystem paths, object keys, repository identifiers, database selectors, URIs, or opaque native values. Filesystem paths require a filesystem naming context; object keys require an object-service context; repository identifiers require a repository context; database selectors require a database context. Database selectors are exact profile values. URI/opaque designators do not imply filesystem semantics.

A text or byte name is preserved as observed. The journal MUST NOT normalize away case, separators, prefix semantics or native source units to impose a universal path. A slash in an object key does not turn it into a filesystem path.

Observation address status is `known`, `not_exposed`, `not_observed`, `withheld`, or `unknown`. Known requires at least one locator binding linked to that observation. Not exposed says that the interface exposes no designator, not that the artifact has never been named anywhere. No synthetic "unknown" path is permitted.

Temporal scopes are a known instant, an interval with at least one known bound, or explicitly unknown. With both bounds present, an interval denotes `[start,end)` and requires start < end. An omitted bound is unknown, not negative/positive infinity. An instant observation is not evidence of an ongoing binding. A `locator_binding_end` names one binding; it does not invalidate the artifact or all aliases.

### 6.1 Optional occurrence materialization hint

An occurrence assertion MAY include `materialization_hint: {components: [...]}`.
This is attributed advice about a future human-facing materialization of that
occurrence, NOT identity, an observed locator, an existing location, a payload
binding, authorization, or evidence of derivation. It has no default, inheritance
or required presence. A journal without hints remains valid canonical provenance.

Components are an ordered list of 1..32 portable Unicode strings of 1..128 code
points each. Empty, dot/dot-dot, slash, backslash and C0/C1 control-containing
components are invalid. Duplicate component values are allowed (for example,
`a/a/file`). No case folding, Unicode normalization or platform parsing occurs.
These labels are relative recommendations, not a guarantee of destination validity;
consumers must escape incompatible names, address collisions and enforce limits.

The occurrence assertion's ordinary evidence attributes the recommendation.
`materialization_hint` is not in the occurrence's immutable identity signature.
A correction can replace the assertion with the same occurrence referent and a new
hint or no hint, while preserving every prior journal byte. This is a descriptive
correction, not a relocation or content transition. Multiple simultaneously active
occurrence assertions for the same referent remain invalid; never pick the latest
journal record or a matching ancestor name to resolve conflicting claims.

A delivered member's hint comes only from its exact anchored delivery State's
Occurrence assertion. Adding it never changes a collection member ID or payload
identity. A changed hint changes the journal/corpus/root commitments that include
it. A sealed collection is not retroactively renamed by a later assertion.

Publication software may separately require an explicit decision to omit a hint.
That decision is not a required provenance field, coverage status, or assertion
that the occurrence was never named. Invalid hints remain invalid even when a
caller permits omission. Profile lookalike fields do not become core hints.

## 7. Processes, transitions and documentary relations

Usage, generation and invalidation are qualified by activity, state and optional event time. Known event times must lie within declared activity bounds. Known usage/invalidation may not precede known state generation, and known usage may not follow known invalidation. Contradictory known lifecycle events for one state are rejected in a single effective graph.

Derivation relates a used state to a generated state and is acyclic. It may be asserted with an unknown process when supporting evidence says so; the schema does not demand an invented activity. When an activity is named, matching usage and generation relationship IDs are required. A state cannot be its own derivative. Branches and multiple-input/multiple-output processes are represented by ordinary relations, not array order.

Explicit specialization is acyclic and stronger than continuity membership. It is never inferred from artifact/occurrence/state membership. A continuity assertion records an attributed judgment, not an identity merger. A content comparison cites descriptions and states matching fixity, different, or indeterminate. It does not establish process, custody, ancestry, native-metadata equality or copy direction.

For unavailable foreign evidence, a non-indeterminate comparison is retained as a dependent claim/finding. When the referenced journals are supplied, its content result is rechecked against exact cited descriptions. Local comparisons without cited complete content evidence must be indeterminate.

### 7.1 Movement and replication

A copy/transfer may use a source state and generate a destination occurrence state. Byte-preserving transfer can have matching primary fixity while changing source-native metadata. A destination observation does not prove source deletion. Source removal must have its own occurrence-scoped evidence.

The journal itself may move between systems without changing its identity. Historical addresses and observations are immutable. A receiving writer can continue an exact verified prefix under a single-writer handoff. Two independent writers MUST NOT append divergent histories under the same journal ID; a new independently writable journal must have a distinct ID and MAY commit its common parent prefix.

### 7.2 Custody and availability

Custody is an attributed, temporally qualified relation between an artifact/occurrence and a responsible Agent in a role/context. It is not a filesystem owner field, a storage location, or a claim of authorship.

Availability/removal claims target one occurrence. Removal of one replica does not invalidate the continuing artifact or all other copies. Availability evidence is not silently derived from an old locator becoming inaccessible.

## 8. Journal subject and delivery associations

A journal-subject assertion identifies which artifact a journal describes. This does not bind the journal to a path or force a single primary artifact.

A delivery association identifies a state and a verifying observation inside one explicit delivery context, under an opaque slot designator and role. A slot is unique only within that delivery instance. Different delivery contexts and concurrent occurrences can coexist. The format has no global current-state or current-payload register.

The verifying observation must directly describe the selected state. Verification checks supplied primary bytes against that observation's size and SHA-256, not the entire historical state, native metadata, transfer process, or source custody. A delivery association can be supplied by an envelope, repository or caller without any filenames or filesystem adjacency.

Adjacent `<payload-name>.fprov.jsonseq` remains a possible external discovery convention when both objects happen to be files. It is not an authoritative core relation and is not required by either package.

## 9. Journal serialization and integrity

A journal is an RFC 7464 JSON Text Sequence. Every complete record is exactly:

```
0x1E || RFC8785-canonical-UTF8-JSON-object || 0x0A
```

The profile is stricter than general RFC 7464 recovery: no BOM, unframed bytes, whitespace outside canonical JSON, duplicate members, embedded record separators, malformed complete records, or trailing incomplete append is conformant. A reference admission limit of 8 MiB applies to one JSON text. Oversized native evidence must use an appropriate retention policy or governed external representation.

The envelope contains the unchanged schema/profile/version identifiers; entry and journal IDs; a decimal-string sequence; recording time/Agent; optional recording context; entry kind; predecessor reference when noninitial; and body.

Sequence starts at zero and is contiguous. Exactly the first entry is `journal_init`. The predecessor commits the preceding entry ID, sequence and SHA-256 of the preceding canonical JSON text **excluding** separator and terminating LF. A checkpoint commits the exact framed bytes through the immediately preceding entry, including all separators and LFs, and their byte length.

Recording time is not subject-creation time or event chronology. Clock adjustments and late recording can make wall times nonmonotonic. Sequence asserts append order only. Hash chaining is not a derivation relation.

A complete prefix can validate independently. Only an independently retained expected prefix/tail anchor can establish that the expected history is present. An unanchored checkpoint or chain cannot detect removal of its own suffix. Hashes do not authenticate writers, attest historical truth, or replace signatures.

### 9.1 Entry kinds

`journal_init` declares journal identity, single-writer policy, serialization/hashing policy, optional exact parent prefix, and an initial atomic assertion graph.

`assertion` adds fresh assertions. Referent descriptions must not conflict with already accepted local definitions. Exact repeated identity declarations may be omitted by the append helper, but states are never deduplicated by content.

`correction` names exact prior assertion IDs and their origin entry references, explains the correction, and optionally adds atomic replacements. Previously retired assertion IDs cannot be reused or retired twice. The effective graph after the transaction must remain locally resolved and coherent.

`checkpoint` commits the immediately preceding framed prefix. It contributes no artifact change or current-payload assertion.

### 9.2 Correction versus change

The original bytes of all entries remain immutable. Corrections change acceptance of claims, not history. A replaced description or relationship receives a fresh record identity when its immutable content changes. Fixed referent aspects cannot be redefined under the same ID. A label correction may retain an Agent/context/artifact identity, but an occurrence change or altered state boundary requires a new appropriate identity.

There is no "undo retraction" operation. A fresh assertion can restate a claim with fresh attribution/identity without reviving the retired assertion. A physical change, transformation, transfer or reobservation MUST NOT be encoded solely as retraction of the previous subject state.

## 10. Foreign references and distributed histories

A local reference contains its referent ID and type. A foreign reference additionally pins journal ID, exact entry ID/sequence/JSON hash, and assertion ID. It identifies documentary evidence, not a fetch URL and not an authenticated claim. The referenced entry may be supplied through any transport.

Unresolved foreign references are allowed in a locally coherent journal and MUST remain explicit. Supplied journal sets can resolve them, verify exact statement matches, detect incompatible immutable definitions and cross-journal derivation/specialization cycles, and recheck content comparisons. A pinned assertion remains historical evidence even when its foreign journal later retracts it; the resolver reports that fact rather than silently retargeting the reference.

Journal forks commit common prefixes without importing their graphs automatically. Entries imported into another journal MUST preserve source evidence or exact foreign references. Array order and "latest received" MUST NOT decide which independent writer is authoritative.

## 11. Materialization and PostgreSQL

A materialized view records its journal prefix anchor, effective assertions, retired assertion IDs, unresolved foreign references and unresolved profiles. It is derived, reproducible and non-authoritative. It MUST NOT replace the exact journal bytes or be used to recompute predecessor hashes by ordinary JSON serialization.

The suggested database landing model retains exact JSON octets alongside `jsonb`, journal/entry ordering, an append-only assertion ledger, and separate retractions. Native values stay row-shaped; large base64 payloads are not automatically indexed. Normalized views expose artifacts, occurrences, descriptions, contextual bindings and delivery associations. A digest index is a content-search aid, not an identity uniqueness constraint.

UUID URNs may be projected to PostgreSQL `uuid`; wide exact integers/decimals remain text unless deliberately range-checked. UTC source text is authoritative for nanoseconds; PostgreSQL `timestamptz` is a convenience projection with lower precision. No global current-state, canonical path, host foreign key or filesystem column is required.

The SQL reference is not a complete database-only conformance validator. The application must validate before committing an atomic entry and all its assertions/retractions. Database authorization, durable transaction boundaries and writer coordination remain repository responsibilities.

## 12. Conformance levels and limitations

1. **Raw admission:** portable JSON subset and exact canonical framing.
2. **Structural:** closed schema shapes, local choices, cardinalities, tagged values and exact schema-pack closure.
3. **Application:** attribution, references, identity separation, bytes/lengths, timestamps, coverage, contextual binding, graph/lifecycle constraints, corrections, sequencing and commitments.
4. **Resolved set:** exact supplied foreign evidence, cross-journal identity/cycle/comparison checks, and parent-prefix resolution.
5. **Payload verification:** independently supplied bytes match one explicitly selected description/delivery association.

These levels MUST NOT be conflated. Profile-unverified inspection and foreign-unresolved local conformance must report the missing evidence. Payload verification does not establish truth of process/custody assertions. No full PROV reasoning, OWL consistency certification, OS certification, or universal archival completeness is claimed.

The reference implementation revalidates effective graphs at every prefix for clarity and correctness checks; it is not an optimized incremental database engine. Implementations may optimize while preserving the same acceptance rules.

## 13. Standards alignment

W3C PROV provides the Entity/Activity/Agent distinctions, named provenance bundles and explicit qualified relations. This profile deliberately does not infer specialization or alternateness from its weaker membership/continuity relations. Captures use subject states and generate observation records, not subject states.

PREMIS informs preservation terminology: fixity, technical evidence, events and responsibility. Its `File` and `Bitstream` categories have narrower definitions than the generic core. No universal PREMIS File/Bitstream equivalence is asserted; additional classification requires evidence that its definition applies. A generic bounded stream is not renamed a PREMIS Bitstream merely to obtain an existing class.

A future RDF exporter must retain per-assertion named graphs, attribution, exact statement references and qualified temporal scopes. It must not flatten historical locations into timeless location facts, custody into authorship, retractions into artifact invalidation, or all conflicting descriptions into an unqualified union graph. The wire format itself is ordinary schema-constrained JSON, **not implicitly JSON-LD**.

Primary references:

- W3C PROV-DM: https://www.w3.org/TR/prov-dm/
- W3C PROV-O: https://www.w3.org/TR/prov-o/
- W3C PROV-Constraints: https://www.w3.org/TR/prov-constraints/
- PREMIS ontology: https://www.loc.gov/standards/premis/ontology/owl-version3.html
- RFC 7464, JSON Text Sequences: https://www.rfc-editor.org/rfc/rfc7464.html
- RFC 8785, JSON Canonicalization Scheme: https://www.rfc-editor.org/rfc/rfc8785.html
- JSON Schema Draft 2020-12: https://json-schema.org/draft/2020-12

The supplied hard cut changes only the two package implementations/contracts. It includes no migration, version bump, downstream propagation, public namespace deployment or repository-wide integration.
