# Architecture overview

The runtime uses four cooperating surfaces.

## Catalog

The catalog is the durable authoritative metadata layer. It tracks collections,
logical files, file hashes, archival coverage, pins, fetches, upload state, and
hot presence across service restarts.

## Upload staging

Collection ingest and fetch recovery both stream bytes through Riverhog-managed
tus-compatible upload resources. Incomplete bytes stage under `.arc/uploads/`
inside the S3-compatible object store and remain outside the committed hot
namespace until Riverhog verifies them.

## Committed hot storage

Committed hot files live in one collection-shaped object namespace:

```text
collections/{collection_id}/{path}
```

Only promoted, verified files count as hot. Staged upload keys and other `.arc/`
paths are not committed hot files.

## Read-only browsing

Read-only WebDAV exposes the committed `collections/` namespace for day-to-day
browsing and download. It must not expose `.arc/`, and it is never an upload
surface.

## Why pins exist

Users do not delete or restore by mutating storage surfaces. Instead they:

- pin a selector into hot
- release a previously pinned target
- let the system materialize or reconcile hot state based on active pins

This keeps intent explicit and makes the system safer than inferring meaning
from storage mutations.

## Restore flow

1. The user pins a target.
2. The system creates or reuses one fetch manifest for that exact selector.
3. If all selected bytes are already hot, the fetch manifest is immediately satisfied.
4. If some bytes are archived but not hot, a companion recovery tool reads the indicated optical copy and streams
   raw encrypted recovery bytes into resumable upload resources.
5. The server stages the uploaded recovery bytes under `.arc/uploads/`, verifies
   and decrypts them, then promotes the recovered logical file into
   `collections/{collection_id}/{path}`.
6. The explicit pin remains active after fetch completion, and the satisfied fetch manifest remains readable until
   release.

## Release flow

1. The user releases an exact selector.
2. The system removes that exact pin, if present.
3. The committed hot namespace is reconciled against the remaining union of pins.
4. Unneeded committed hot files become eligible for cleanup immediately.
