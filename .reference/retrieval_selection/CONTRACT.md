# Issue #951 retrieval-selection reference

This document is non-authoritative reference material under #903. The durable scope and any accepted decision live in issue #951.

Audited source: `main@e854feebf9cb8491ae42e3d743e9514f6780e27b`.

## Recommended v1 contract

The smallest coherent change is to separate **archive fallback selection** from **cache read placement**.

1. Resolve one archive fallback copy using an explicit plan-wide `source_store` when supplied, otherwise the existing `archive_read_order`.
2. Preserve the current archive eligibility rule: complete, non-retiring custody and an exact usable storage incarnation. An explicit source is strict and never silently falls back.
3. Plan artifact/object topology against that selected archive copy.
4. For each bounded payload object (`pack` or `segment`), look for an equivalent `ready` cache placement across all historical archive-source names.
5. Prefer a usable equivalent cache placement over archive I/O. Rank cache candidates by configured retrieval-cache store order, then a stable source identity tie-breaker.
6. If no cache placement is reusable, preserve the selected archive adapter's current `immediate` / `restore_required` behavior.
7. Preserve `restore_policy=allow|never` exactly. Cache hits can make an otherwise cold plan opportunistically available; misses remain governed by the selected archive source and existing restore policy.

There is no caller-facing cache bypass in this v1 slice. Cache preference is ordinary retrieval behavior, not a new policy choice. Forced-origin reads are a diagnostic/qualification concern and should not enlarge the normal retrieval contract without a demonstrated public need.

## Why retain source-scoped cache records

Current cache state records the archive source store/incarnation that established a placement, and leases/accounting/GC are built around that durable key. Keep that provenance rather than globally re-keying cache state in #951.

The production plan needs two distinct identities for a direct cache hit:

- **archive fallback:** `source_store` + `source_incarnation_id` already pinned by the retrieval plan;
- **selected cache object:** its historical archive `source_store` + `source_incarnation_id`, plus the existing cache store/incarnation.

A direct cache hit from another archive copy must therefore add a nullable `cache_source_store` / `cache_source_incarnation_id` (or an equivalently exact cache-object reference) to `RetrievalPlanObjectRecord`. Later cache reads, leases, renewal checks, and GC protection must use that exact cache key instead of assuming the archive fallback source and cache provenance are the same.

A cold `restore_required` object does not need these extra fields: when its job populates cache, that cache object is naturally source-owned by the selected archive source, as today.

Do not globally deduplicate equivalent source-scoped cache rows, migrate existing cache identities, or rebind cache provenance when an archive copy is retired. Cache is rebuildable; existing retirement/deletion cleanup may discard a source-owned cache row. Permanent-loss/purge semantics remain #909.

## Copy-equivalence rule

A global cache lookup must not treat `object_id` alone as content identity. It also should not introduce a new mandatory whole-object `stored_sha256` solely for #951: current pack/segment catalog rows intentionally may leave that field null, and adding a new archive hash/transfer contract would broaden v1 and its qualification surface.

Instead, make the exact archive-copy invariant already exercised by Riverhog executable at cache selection. For a cache candidate's historical source object and the selected fallback object's same collection/object id, compare copy-invariant sealed state:

### Common

- `kind`
- `object_id`
- `plaintext_bytes`
- `stored_bytes`
- canonicalized `age_state_json` identity

### Pack

- `plan_sha256`
- `index_sha256`

The pack plan identity already binds its source artifact IDs, byte counts, SHA-256s, and canonical unit/layout recipe.

### Raw segment

- unique artifact placement identity (`artifact_id`, artifact offset, object offset/bytes as applicable)
- the referenced collection artifact's immutable byte count and SHA-256

Provider-local `object_path`, provider `revision`, and provider write segmentation/part boundaries are deliberately excluded; archive replication may change those while streaming the same stored encrypted bytes.

If supposedly equivalent complete copies disagree on these copy-invariant fields, fail closed as invalid archive/catalog state rather than silently ignoring the mismatch.

## Source anchors on the audited base

These are research anchors, not a replacement for current code/tests during integration.

- `riverhog/src/riverhog_core/services/archive_records.py`: `select_readable_archive_copy()` currently walks `archive_read_order`, requires complete/non-retiring copies, and skips unavailable exact incarnations.
- `riverhog/src/riverhog_core/services/retrieval.py`: `_advance_plan_record()` currently selects one archive copy first, records it on the artifact/object plan rows, then checks cache only by `(selected_source_store, collection_id, object_id)`.
- `riverhog/src/riverhog_core/services/retrieval.py`: `_current_cached_object()` and cache-protection/renewal queries assume the planned archive source is also the cache object's source.
- `riverhog/src/riverhog_core/catalog_models.py`: `RetrievalPlanObjectRecord` pins archive source/incarnation and cache store/incarnation, but has no independent cache-source identity; `RetrievalCacheObjectRecord` is keyed by source store + collection + object and fenced by both source and cache incarnations.
- `riverhog/src/riverhog_core/services/archive_copy_jobs.py`: archive copy streams stored encrypted bytes and copies the source object's immutable archive metadata into the destination record; destination provider path/revision and segmentation may differ.
- `riverhog/src/riverhog_core/services/collection_uploads.py`: payload volume rows persist canonical age state and pack/raw archive identities; volume-level `stored_sha256` may be null.
- `riverhog/src/riverhog_core/runtime_config.py`: deployment owns ordered archive reads and ordered cache registrations.
- `riverhog/src/riverhog_api/schemas/collections.py` and the maintained client: upload already has an optional per-operation `archive_store`, resolved from a deployment default when omitted. #951 should give retrieval analogous explicit source control without making deployment policy API-mutable.

## Suggested production shape

The integration agent should reconcile exact names with current code, but the narrow target is:

- `RetrievalPlanRequest.source_store: ArchiveStoreName | None = None`;
- maintained `riverhog-client.plan_retrieval(..., source_store=None)` parity;
- a persisted nullable requested source on `RetrievalPlanRecord`, bound into idempotency/creation identity;
- resolved `source_store` on `RetrievalPlanArtifactOut` so bounded plan inspection truthfully shows the selected archive fallback;
- nullable exact cache-source identity on `RetrievalPlanObjectRecord` for `read_mode == "cache"`;
- one helper for strict/default archive selection and one helper for global equivalent-cache selection;
- a single helper that resolves the actual cache-object key for direct-cache versus restored-cache plan objects, reused by serving, job leases, renewal, and GC protection.

Do not expose storage-incarnation IDs as caller choices. Keep them durable internal fences from #908.

## Side-effect and policy boundaries

- Cache selection performs no archive provider read/restore side effect.
- An unavailable historical archive adapter may still have a reusable cache placement; serving that placement does not require rebinding or probing the historical archive source. Its durable copy/object/incarnation provenance must still match catalog state and must not be retiring.
- The selected archive fallback still must be normally usable at plan time. #951 does not turn cache into standalone archive authority when no readable archive copy exists.
- Cache hits are accounted as retrieval-cache reads; archive download allowance/cost applies only to actual selected-source archive reads.
- Retrieval authorization is unchanged. Selecting a source does not mutate archive state and should not require archive-management authority beyond the existing retrieval-management contract.
- `archive_write_store`, `archive_read_order`, cache registration/order/admission, and provider topology stay deployment configuration under the existing architecture/#492 boundary.
- Upload/copy cache-population semantics from #869 stay unchanged. Global read reuse can reduce later duplicate population naturally but does not redefine population identity.

## Required integration proof

At minimum, production tests should prove:

- cache from a lower-priority or unavailable historical archive source beats archive I/O through the selected usable source;
- explicit source remains strict while an equivalent cache from another source still wins;
- no explicit-source fallback occurs on miss;
- cache-store ordering is honored across equivalent source-scoped rows;
- partial cache coverage reads only misses from archive;
- full cache coverage of a restore-required source leaves `requires_restore == false`;
- a restore-required miss under `restore_policy=never` still cannot initiate restore;
- archive and cache storage-incarnation fences remain independent and exact;
- copy-invariant disagreement fails closed;
- restart, idempotent replay, and config drift preserve the sealed choices;
- cache lease/renewal/GC references protect the actual selected cache row, not the archive fallback key;
- #763 admission/accounting/eviction behavior and archive-copy retirement/deletion behavior remain otherwise unchanged.

## Explicit non-goals

- cache bypass / force-origin mode;
- runtime API mutation of deployment-wide storage defaults;
- global cache re-keying or deduplication;
- cache provenance rebinding across source retirement;
- a new mandatory whole-object stored digest;
- primary/mirror archive roles;
- storage-adapter protocol or provider-policy changes;
- cache as archive authority or independent recovery material.
