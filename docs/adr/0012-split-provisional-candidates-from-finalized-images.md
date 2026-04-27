# ADR 0012: Use provisional candidate ids and finalized image ids

## Status

Accepted.

## Context

Operators need one stable identifier for finalized media across:

- finalized image lookup
- ISO download
- copy registration
- webhook payloads
- disc inspection and labeling
- future finalized-image listing

The contract also needs a stable identifier for provisional planner entries.

## Decision

- `GET /v1/plan` returns provisional candidate summaries only
- each provisional candidate exposes `candidate_id`
- a provisional candidate may be re-allocated by the planner until it is explicitly finalized
- `POST /v1/plan/candidates/{candidate_id}/finalize` is the only operation that turns a provisional candidate into a
  finalized image
- finalization assigns a unique compact UTC identifier in basic form `YYYYMMDDTHHMMSSZ`
- that assigned identifier is the finalized image's canonical `id`
- that finalized image `id` is the media identifier carried on the disc
- repeated finalization of the same `candidate_id` is idempotent and returns the same finalized image summary
- a finalized candidate does not appear in `GET /v1/plan`
- after finalization, the planner must not re-allocate that finalized image's represented bytes
- `GET /v1/images/{image_id}`, ISO download, copy registration, ready-image webhooks, and future finalized-image
  listing all use the finalized image `id`
- `GET /v1/images` lists finalized images only and never re-exposes provisional candidates
- finalized-image listing uses conventional pagination, sorting, and filtering over finalized-image metadata
- finalized-image listing and finalized-image lookup expose the same finalized-image summary shape
- the finalized image summary exposes that canonical id as `id`
- the finalized image summary also exposes `filename`, `finalized_at`, `collection_ids`, and archive-protection metadata
- a registered physical disc is identified by the tuple `(volume_id, copy_id)`
- `copy_id` is an arbitrary operator-supplied string scoped to one `volume_id`
- registering a `copy_id` that already exists for the same finalized image and `volume_id` is rejected
- `copy_id` and the associated `volume_id` are immutable after registration
- `location` is not part of disc identity; it is mutable operational metadata

## Consequences

- planner output uses candidate summaries and finalized-image output uses image summaries
- operators have one finalized identifier to use across API calls, disc labels, burned-copy registration, and webhook
  notifications
- finalized-image list APIs can key directly on the same identifier that appears on the disc while also supporting
  operator workflows such as finding images by filename, contained collection, and copy presence
- copy and fetch contracts continue to expose the physical-media identifier in fields named `volume_id`
- follow-on implementation work must update the production planning/image/ISO/copy slice to use the new provisional
  finalization route and finalized-only image lookup semantics
