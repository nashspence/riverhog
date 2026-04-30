# ADR 0019: Use explicit collection upload sessions

## Status

Accepted.

## Context

Collection ingest flows through `arc`. Riverhog needs a
collection contract that:

- gives the client an explicit collection identity up front
- supports resumable Riverhog-managed uploads backed by internal staging
- keeps collection visibility separate from staged in-progress upload state
- preserves the existing prefix-free collection-id namespace

## Decision

- collection ids are explicit client-supplied canonical ids
- collection ids may be slash-delimited, for example `tax/2022`
- collection ids remain prefix-free on path segments:
  - if `tax` exists, reject `tax/2022`
  - if `tax/2022` exists, reject `tax`
- collection ingest uses a collection-upload session plus one resumable upload resource per logical file
- each collection-file `upload_url` is a Riverhog-managed tus-compatible resource rather than a raw storage-owned URL
- incomplete collection bytes stage outside the committed `collections/{collection_id}/{path}` namespace until Riverhog verifies them
- collection-upload sessions persist enough state to survive service restart and repeated CLI runs
- collection-upload files use the same `INCOMPLETE_UPLOAD_TTL` expiry model as fetch recovery uploads
- a collection remains invisible until every required file upload completes,
  Riverhog verifies the advertised hashes, and the whole-collection Glacier
  archive package uploads and verifies
- the terminal successful collection-file upload chunk may auto-advance the
  collection into `archiving`; it must not report final success before Glacier
  archive verification
- collection-upload state uses `uploading`, `archiving`, `finalized`, and
  `failed`
- once the last resumable collection-file state expires, Riverhog forgets the collection-upload session instead of keeping an empty pending record
- optional ingest-source metadata is descriptive only and is never part of collection identity

## Consequences

- `arc upload COLLECTION_ID ROOT` becomes the canonical collection-ingest command
- slash-bearing collection ids are first-class throughout API, CLI, planner, search, and file introspection
- collection ingest and fetch recovery share one resumable-upload lifecycle model
