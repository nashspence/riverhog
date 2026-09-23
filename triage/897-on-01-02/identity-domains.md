# NON-AUTHORITATIVE #897 ON-01/02 identity-domain map

Audited main: `1b6ee5e8a2f1df39852465050df3efa96d6773e4`. All source links below
are pinned to that commit. Observations are design input, not acceptance or a
complete repository call-graph audit. **Read** means the relevant declaration or
implementation was inspected; **located** means source-search evidence only and
requires expansion during integration. No absence of other consumers is implied.

## Canonical-value domains and adjacent byte authorities

| Domain / evidence | Current producer and consumer surface | ON-01/02 cut and invariants |
| --- | --- | --- |
| Workflow documents — read [collection_workflows.py][workflow] | `ProducerEvidence.to_json_bytes` / `.sha256`, `canonical_json_bytes` / `canonical_json_sha256`, mapping reconstruction, collection/artifact/recipe identities; client transform writer and derivation-evidence callers located | Sorted compact Unicode Python JSON becomes JCS. Coordinate evidence emitters, sealing, parsing and verification. Exact `collection_id`, `revision`, artifact `bytes`, counts and opaque `source_context` / `effective_intent` reach the domain. Preserve existing input ordering, fields, formats and identity subjects. |
| Create-or-resume identity — read [collection_creation_identity.py][creation] | `CollectionUploadCreationIdentityDocument.seal` and `verify_identity` share workflow hash | Preserve `creation_identity_sha256` exclusion and `exclude_none=True`; do not rename the field. Treat arbitrary `event_context` as an explicit numeric-admission boundary, not recursive numeric stringification. |
| Upload summary and custody receipts — read [collection_upload_transport.py][upload] | `CollectionUploadVolumeSummaryDocument`, `CollectionUploadArtifactCustodyReceiptDocument.seal` / `validate_receipt`; service and client upload consumers located | Current upload sequence is numeric with a 256-bit validator, while the volume ID embeds 64 hex digits. `emit_upload_volume` / `read_upload_volume` demonstrates full-domain string conversion plus the same binding. Other exact numbers include unit IDs, offsets, byte totals, first/accepted part counts, receipt `bytes`, `archive_object_count`, collection IDs. Receipt digest excludes itself. Ordered archive-object commitment retains eight-byte big-endian length prefixes and existing order, not a new array recipe. |
| Archive root / volume metadata — read [archive_manifest.py][archive] | `_canonical_json_bytes`, `CollectionTreeIdentity`, `AgeUploadState`, `StoredPartIdentity`, `SegmentFilePlacement`, `PackArchiveVolume`; server archive creation and independent recovery consumers located | Default-ASCII compact JSON becomes JCS for newly authored canonical values; canonicality validators and independently recoverable readers must agree. Existing 256-bit hex ordinals already fit JCS. Exact file/byte totals, offsets and age plaintext sizes do not gain a 53-bit ceiling. Keep existing bounded part/member counts distinct from logical totals. Stored part hashes remain hashes of their actual bytes. |
| Archive recovery descriptor — [recovery_descriptor.py][descriptor] located | Independent recovery package and descriptor decoding require follow-through | Revalidate exact byte-vs-value recipe and descriptor version/consumer expectations before changing emission. Do not import server, client or database code into recovery. This reference does not modify a descriptor or existing archive. |
| Pack/raw plans and checkpoints — [pack_ingress.py][pack], [raw_volume.py][raw], [pack_volume.py][packvolume], [incremental_plan.py][incremental] located | Separate `canonical_json_bytes`; plan-payload serializers, planner checkpoint byte comparisons, archive catalog and raw verification callers | Expand plan/control-payload fields and hashes, then convert named ordinal/count/size domains with producers and readers. Checkpoints serialized in private state need an explicit cutover, not silent reinterpretation. Opaque payload, plaintext and ciphertext hashes must not be redirected through JCS. |
| Portable provenance semantic values — read [schema.py][provschema] and [journal.py][journal]; [common.py][common] helper located | Policy-digest validation, canonical entity/state comparison and new journal-emission helper share `canonical_json` | Separate value identity/comparison from immutable entry-byte identity. JCS convergence here may affect captured policy data and typed extensions. Exact sizes/counts and extension-owned numeric domains need owned codecs; do not infer schema meaning from key spelling. |
| Portable provenance journal integrity — read [journal.py][journal] declaration and parser/hash snippets; [journal schema][journal-schema] sequence declaration read | `_journal_frame` rejects duplicate decoded keys already and hashes recorded `json_bytes`; incremental validation binds previous entry ID/sequence/hash and physical sequence | **Not a competing canonical-JSON profile.** Keep exact-byte verification/prefix preservation unchanged. New sequence and predecessor-sequence scalar projections must preserve 0..2**63-1 and coordinate initialization `const:0`, noninitial constraints, parser comparisons, schemas, emitters and fixtures. Historical bytes are never reserialized before verification. The prototype byte helper is not a full journal validator. |
| Stove0 work/schema/evidence — read [jcs.py][jcs] and [models.py][models] | Existing `rfc8785.dumps`; `JsonSchemaDocument.from_schema` / `verify_digest`, local schema fallback identity, `_without_digest`; work and evidence family declarations | Move primitive ownership to focused support only through normal integration; no Riverhog-to-Stove0 implementation dependency. Preserve complete schema profile referent (`format`, dialect, format policy, schema), digest exclusion and omitted nulls. Raw opaque JSON admission must occur before Pydantic conversion. Large numeric schema constraints can themselves fail JCS; reject or redesign the owning semantic representation, never stringify arbitrary JSON Schema keywords. |
| Target protocol/support — [target JCS][targetjcs], [target-support JCS][targetsupport] re-exports located | Target protocol re-exports Stove0 protocol helper; runtime support re-exports target protocol helper | One shared JCS rule, with declared independent dependency direction. Trace target schema and plan/evidence seals when integrating; do not turn reference application implementation into common infrastructure. |
| HTTP control declarations / browsing — read [http-api-contracts][http]; [browse.py][browse] located | Another canonical helper beside JSON-sequence / opaque framing declarations; browse has `_canonical_bytes` | New canonical JSON control declarations need strict raw parsing and agreed scalar domains. Framing and opaque payload bytes remain separate authorities. Browse-token serialization must be traced through signer/decoder before changes; do not replace a token format or widen numeric acceptance merely by swapping the encoder. |
| Retrieval request identity — [services/retrieval.py][retrieval] located | Sorted JSON construction over normalized collection ID/path pairs found by search | Expand producer/hash/consumer recipe before conversion; keep semantic ordering, logical paths and named identity subject unchanged. No ON-11 path work is included. |
| Generated closure / qualification — [atlas][atlas], [atlas validation][atlas-validation], [freeze][freeze], [extent][extent], [provider qualification][provider] located | Canonical hashes and sorted JSON appear in boundary/contract and qualification machinery; extent hashing includes numeric normalization | Map as later coordinated consumers of changed schemas and identities, **not authorization to regenerate or freeze**. Numeric constraints and normalization require deliberate reconciliation. This reference does not modify any machinery, generated baseline, qualification evidence, release requirement or pinned branch. |
| Ordinary JSON display and externally specified bytes — boundary classification | Formatting alone does not establish an identity recipe. Archive payloads, journal entry text, encrypted/signed external objects retain actual byte authority | Do not apply blanket `json.dumps` replacement. Identify the actual consumer and whether canonical-value identity is required. No digest names, external standards or object referents are changed. |

## Numeric/admission closure to carry into integration

The three executable scalar codecs preserve the current capacities while testing
one stable representation per named domain. They are not a claim that every
integer-valued field has already been dispositioned. Audit collection IDs,
revisions, lengths, offsets, page ordinals, work/evidence fields and schema
metadata at their actual canonicalization entry points. Small bounded carrier
indices can remain numbers where the whole declared domain is lossless; do not
use a single scalar type to impose a 53-bit cap on unbounded totals.

The prototype's exact-versus-binary64 decision is explicit and occurs before
parsing can lose information. Unicode normalization is not part of that decision.
Object sorting does not replace a domain's existing ordered-set preparation or
framing. `null` versus absent, default insertion, self-digest exclusion and
sequence order remain producer-owned semantics. ON-03 digest renaming and the
remaining ontology register are deliberately untouched.

Integration must expand located callers, inspect their executable tests, update
raw input adapters and semantic models together, regenerate relevant schemas and
fixtures on the normal implementation rail, and verify both fresh emission and
independent consumption. The issue retains authority over codec selection,
shared-package placement, opaque-schema policy and any private-data cutover.
This map is not an acceptance checklist or a new maintained contract document.

[workflow]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-protocol/src/riverhog_protocol/collection_workflows.py
[creation]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/riverhog/src/riverhog_core/collection_creation_identity.py
[upload]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-protocol/src/riverhog_protocol/collection_upload_transport.py
[archive]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-archive-contracts/src/riverhog_archive_contracts/archive_manifest.py
[descriptor]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-archive-contracts/src/riverhog_archive_contracts/recovery_descriptor.py
[pack]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-protocol/src/riverhog_protocol/pack_ingress.py
[raw]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/riverhog/src/riverhog_core/raw_volume.py
[packvolume]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/riverhog/src/riverhog_core/pack_volume.py
[incremental]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/riverhog/src/riverhog_core/incremental_plan.py
[provschema]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-provenance/src/riverhog_provenance/schema.py
[journal]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-provenance/src/riverhog_provenance/journal.py
[common]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-provenance/src/riverhog_provenance/common.py
[journal-schema]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-v1-journal-entry.schema.json
[jcs]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/reference/stove0/packages/protocol/src/stove0_protocol/jcs.py
[models]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/reference/stove0/packages/protocol/src/stove0_protocol/models.py
[targetjcs]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/reference/stove0/packages/target-protocol/src/stove0_target_protocol/jcs.py
[targetsupport]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/reference/stove0/packages/target-support/src/stove0_target_support/jcs.py
[http]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/http-api-contracts/src/http_api_contracts/__init__.py
[browse]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/packages/http-api-contracts/src/http_api_contracts/browse.py
[retrieval]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/riverhog/src/riverhog_core/services/retrieval.py
[atlas]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/scripts/contract_atlas/__init__.py
[atlas-validation]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/scripts/contract_atlas/validation.py
[freeze]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/scripts/contract_freeze.py
[extent]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/scripts/extent_contract.py
[provider]: https://github.com/nashspence/riverhog/blob/1b6ee5e8a2f1df39852465050df3efa96d6773e4/scripts/provider_qualification.py
