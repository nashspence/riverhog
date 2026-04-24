# ADR 0016: Use staging filesystem as object store for file content

## Status

Accepted.

## Context

File content must be accessible at manifest-generation and upload-verification time. Keeping
that content only in the staging directory (which is already written during collection close)
avoids duplicating bytes in SQLite and keeps DB size proportional to metadata.

Upload progress must survive service restarts without re-uploading from the beginning. Writing
incoming recovery bytes to a per-entry buffer file under `.arc_uploads/` and truncating to the
committed DB offset on resume satisfies TUS-compatible resumption without storing binary payloads
in the database.

The total encrypted size of each fetch entry (its `recovery_bytes`) must be queryable without a
disk read so that `GET /v1/fetches/{id}` and upload-state checks remain fast. Computing this at
fetch-entry-creation time (when both file content and the registered copy/part structure are
known) and storing it as an integer column achieves that.

## Decision

1. **Collection file content**: `CollectionFileRecord` carries no `content` column. The staging
   directory is kept on disk after `close()`. File bytes are read from
   `staging_root / <collection_staging_subpath> / <file_path>` whenever needed (manifest
   generation, upload verification).

2. **Upload buffers**: Incoming recovery bytes are written to
   `staging_root / .arc_uploads / {fetch_id} / {entry_id}` and appended sequentially. Offsets
   are truncated to the committed DB offset on resume, giving TUS-compatible resumption after
   crashes. Buffers are deleted on upload expiry; they persist after `complete()` until the pin
   is released.

3. **Recovery-bytes precomputation**: `FetchEntryRecord.recovery_bytes` stores the total
   encrypted size computed at fetch-entry-creation time (when both the file content and the
   registered copy/part structure are known). This allows `GET /v1/fetches/{id}` and upload
   state checks to return accurate `recovery_bytes` and `missing_bytes` without a disk read.
   If copies are registered after a fetch is created, `recovery_bytes` will be stale; this
   is an accepted limitation for the current implementation.

## Consequences

- DB size is proportional to metadata only, not file content.
- The staging directory must remain accessible for the lifetime of any active pin that
  references a file in that collection.
- Upload buffers accumulate under `staging_root/.arc_uploads/` until expiry or pin release.
- Recovery-bytes values are accurate when copies are registered before pinning (the
  standard workflow); they may be stale if copies are registered after a pin is created.
