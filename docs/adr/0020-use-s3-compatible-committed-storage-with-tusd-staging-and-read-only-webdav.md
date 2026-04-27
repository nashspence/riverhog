# ADR 0020: Use S3-compatible committed storage with tusd staging and read-only WebDAV

## Status

Accepted.

## Context

Riverhog needs durable byte storage for two user-visible workflows:

1. Keeping committed hot collection files available after collection ingest or
   fetch recovery completes.
2. Accepting resumable upload progress for collection ingest and encrypted fetch
   recovery without exposing incomplete bytes as committed hot files.

The catalog remains the source of truth for collections, files, images, copies,
pins, fetches, upload state, verification metadata, and TTL boundaries.

## Decision

Use one S3-compatible object store for both committed hot files and incomplete
upload staging.

Committed hot collection files are stored at:

```text
collections/{collection_id}/{path}
```

Incomplete collection-ingest and fetch-recovery uploads stage under:

```text
.arc/uploads/{upload_id}
```

Collection ingest and fetch recovery both use Riverhog-managed tus-compatible
upload resources. Riverhog maps each logical upload resource to an internal
`tusd` upload, tracks the mapping in the catalog, and treats `tusd` hooks as
notifications rather than authority.

Riverhog promotes bytes from staging to the committed collection path only after
it verifies the full staged payload and the workflow-specific completion rules
allow publication.

Read-only WebDAV is the supported day-to-day browsing surface for committed hot
files only. It must expose the committed `collections/` namespace and must not
expose `.arc/` staging paths. WebDAV is not an upload path.

The runtime configuration uses:

```text
ARC_OBJECT_STORE=s3
ARC_S3_ENDPOINT_URL
ARC_S3_REGION
ARC_S3_BUCKET
ARC_S3_ACCESS_KEY_ID
ARC_S3_SECRET_ACCESS_KEY
ARC_S3_FORCE_PATH_STYLE
ARC_TUSD_BASE_URL
ARC_TUSD_HOOK_SECRET
ARC_WEBDAV_ENABLED
ARC_WEBDAV_ADDR
INCOMPLETE_UPLOAD_TTL
UPLOAD_EXPIRY_SWEEP_INTERVAL
```

`INCOMPLETE_UPLOAD_TTL` and `UPLOAD_EXPIRY_SWEEP_INTERVAL` remain authoritative
for resumable upload expiry. Storage-side multipart cleanup is only a safety net
for abandoned multipart parts.

## Consequences

- committed hot bytes live in a collection-shaped namespace that matches the
  logical file contract
- incomplete, failed, and expired uploads stay outside the committed hot
  namespace
- completed hot files become browseable only after Riverhog verification and
  promotion
- runtime and deterministic test harnesses need an S3-compatible object store,
  `tusd`, and a read-only WebDAV sidecar
