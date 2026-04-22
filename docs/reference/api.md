# API reference

This document summarizes the MVP HTTP and CLI contract. The canonical machine-readable shape is
`openapi/arc.v1.yaml`.

## HTTP API

All JSON endpoints are under `/v1`. Requests and responses use JSON unless otherwise specified.
Resumable fetch-entry upload URLs are returned by the JSON API and use tus-compatible transport semantics.

### Collections

#### `POST /v1/collections/close`

Closes a staged directory into a new collection.

Request body:

```json
{
  "path": "/srv/archive/staging/photos/2024"
}
```

Required behavior:

- scans and freezes the staged directory
- derives the collection id from the canonical relative path beneath the staging root
- allows slash-bearing collection ids such as `/staging/photos/2024 -> photos/2024`
- rejects a collection id if it would be an ancestor or descendant of an existing collection id
- creates one new collection
- materializes all files into hot storage immediately
- makes the collection eligible for planning

### Search

#### `GET /v1/search?q=<query>&limit=<n>`

Returns collection and file selectors that can be used directly with `pin` and `release`.

Required behavior:

- search is case-insensitive substring match over collection ids and full logical file paths
- file results include current hot availability
- file results include available copies, if any
- returned selectors use projected-path syntax only
- `limit` is honored

### Collections summary

#### `GET /v1/collections/{collection_id}`

Returns a collection summary with byte coverage values.

Required behavior:

- collection ids may span multiple path segments, for example `GET /v1/collections/photos/2024`
- API and CLI collection lookup treat slash-bearing ids as first-class

### Planning

#### `GET /v1/plan`

Returns the best current provisional planner output and readiness status.

Required behavior:

- every image returned by the plan is still provisional
- a provisional image may still be re-allocated by the planner
- finalized images are not returned by `GET /v1/plan`
- plan image objects do not expose `volume_id`

### Images

#### `GET /v1/images/{image_id}`

Returns one image summary.

Required behavior:

- `image.id` is the stable API handle for one known image, whether still provisional or already finalized
- `volume_id` is `null` while the image remains provisional
- after explicit finalization, the stored `volume_id` is returned for that same `image.id`

#### `POST /v1/images/{image_id}/finalize`

Explicitly finalizes one ready provisional image.

Required behavior:

- this is the only operation that may assign and store `volume_id`
- finalization assigns a unique immutable `volume_id` in UTC basic form `YYYYMMDDTHHMMSSZ`
- if more than one image would otherwise finalize in the same second, later assignments advance in whole seconds until
  an unused `volume_id` is found
- after finalization, the planner must not re-allocate that image's represented bytes
- after finalization, that image is no longer returned by `GET /v1/plan`
- repeated finalization of the same `image.id` is idempotent and returns the same stored finalized summary

#### `GET /v1/images/{image_id}/iso`

Returns ISO bytes if the image is ready.

Required behavior:

- ISO download does not finalize the image
- ISO download requires the image to have already been explicitly finalized
- subsequent downloads for the same finalized `image.id` reuse the same `volume_id` and the same represented bytes

#### `POST /v1/images/{image_id}/copies`

Registers a physical burned disc for an image.

Required behavior:

- copy registration is only valid for an image that has already been explicitly finalized and has a stored `volume_id`
- the physical copy identity is `(volume_id, copy_id)`
- the user-supplied `copy_id` must be unique within that finalized image/`volume_id`; duplicates are rejected with
  `conflict`
- `location` is mutable operational metadata and is never part of copy identity

### Pins

#### `POST /v1/pin`

Pins a target into hot storage.

Required behavior:

- the `target` field carries one canonical selector over the projected hot namespace
- successful pin keeps the target desired in hot until explicitly released
- every successful exact pin creates or reuses one fetch manifest for that same selector
- if all targeted bytes are already hot, the returned fetch manifest is already in state `done`
- if some targeted bytes are archived but not hot, the returned fetch manifest is created or reused in a non-`done`
  state
- repeated pin of the same canonical selector is idempotent

#### `POST /v1/release`

Releases exactly one canonical selector pin.

Required behavior:

- releasing a non-existent exact pin is a successful no-op
- releasing a broader pin must not remove narrower remaining pins
- releasing a narrower pin must not remove broader remaining pins
- releasing the last exact pin for one selector abandons and removes that selector's fetch manifest
- releasing one exact pin also removes any hot files that are no longer selected by a remaining pin

#### `GET /v1/pins`

Lists active pins.

Required behavior:

- every returned pin includes its associated fetch id and current fetch state

### Fetches

#### `GET /v1/fetches/{fetch_id}`

Returns one pin-scoped fetch summary.

#### `GET /v1/fetches/{fetch_id}/manifest`

Returns a stable manifest for the exact pin lifetime.

- the fetch manifest is the source of truth for automated multipart recovery
- multipart logical files include part-level recovery hints
- `entries[].parts[]` are ordered by zero-based `index`
- every manifest entry includes logical plaintext `bytes` / `sha256` plus `recovery_bytes` for the ordered upload
  stream
- every part hint includes logical plaintext `bytes`, logical plaintext `sha256`, `recovery_bytes`, and at least one
  candidate recovery copy
- every candidate recovery copy includes `disc_path`, `recovery_bytes`, `recovery_sha256`, and opaque `enc`
- `arc-disc` uploads the raw encrypted bytes stored at `disc_path`, not reconstructed logical plaintext
- logical plaintext hash and size fields remain server-side verification anchors after decryption and reconstruction
- each manifest entry exposes current upload state, uploaded bytes, and upload expiry if partial state exists
- those hints drive disc sequencing and resumable recovery in `arc-disc`
- incomplete upload state expires after `INCOMPLETE_UPLOAD_TTL` since the last accepted chunk and the manifest returns to
  `waiting_media`
- fetch summaries expose an audit field such as `upload_state_expires_at`

#### `POST /v1/fetches/{fetch_id}/entries/{entry_id}/upload`

Creates or resumes the resumable upload resource for one manifest entry.

Required behavior:

- the response returns one upload URL bound to exactly one logical file entry
- the returned upload URL uses tus-compatible resumable upload semantics
- the response includes current offset, total length, transport checksum algorithm, and expiry time
- offset and length are measured in the entry's ordered recovery-byte stream
- repeated calls while the upload remains resumable return the current upload resource rather than creating duplicates
- the server owns any required decryption and final logical-file validation behind that upload resource

#### `POST /v1/fetches/{fetch_id}/complete`

Marks the fetch manifest satisfied once all required entries have been uploaded, verified, and materialized. The
manifest remains readable while the exact pin remains active.

## Error model

All non-2xx responses return JSON with at least:

- `error.code`
- `error.message`

Suggested error codes:

- `invalid_target`
- `not_found`
- `conflict`
- `invalid_state`
- `hash_mismatch`
- `bad_request`

## CLI parity

### `arc`

The `arc` CLI is a thin API client and should provide at least:

- `arc close PATH`
- `arc find QUERY`
- `arc show COLLECTION`
- `arc plan`
- `arc iso get IMAGE_ID [-o FILE]`
- `arc copy add IMAGE_ID COPY_ID --at LOCATION`
- `arc pin TARGET`
- `arc release TARGET`
- `arc pins`
- `arc fetch FETCH_ID`

`arc fetch FETCH_ID` should provide a concise human-readable listing of:

- files still pending upload
- files currently partial and still resumable
- the expiry time for each partial upload

### `arc-disc`

The `arc-disc` CLI is a fetch-fulfillment client for a machine with an optical drive and should provide:

- `arc-disc fetch FETCH_ID [--device DEVICE]`

For multipart recovery, one invocation should continue across successive discs until every required
part has been recovered, streamed, and uploaded.

Required behavior:

- complete files stream straight from optical recovery into the upload resource rather than being materialized to disk
  first
- split files stream into the same logical-file upload resource in ascending part order
- the upload resource receives raw encrypted recovery bytes exactly as stored in the hinted payload object(s)
- `arc-disc` treats the upload resource as opaque and does not own decryption or final logical-file hash validation
- resumable offsets remain valid only for the exact recovery-byte stream accepted so far for the current span
- any temporary buffering used during recovery is an internal implementation detail
- progress output is precise and continuous, including current transfer rate, percent complete for the current file, and
  percent complete for the whole manifest

## Behavioral invariants

- pinning the same selector twice results in exactly one active pin
- pinning the same exact selector twice reuses the same fetch manifest while that exact pin remains present
- releasing a target not currently pinned is a successful no-op
- a file is logically required in hot if and only if at least one active pin selects it
- immediately after collection close, every file in the collection is hot
- every active pin has exactly one associated fetch manifest, even when the selected bytes are already hot
- a file restored by a completed fetch is hot
- upload-state expiry for a manifest discards incomplete partial uploads and returns that manifest to `waiting_media`
- `INCOMPLETE_UPLOAD_TTL` defaults to `24h`
- fetch upload progress is tracked per logical file, not per disc fragment
- every image returned by `GET /v1/plan` is provisional and omits `volume_id`
- an explicit finalize operation is the only path that stores immutable `volume_id` for an `image.id`
- once finalized, an image is no longer returned by `GET /v1/plan`
- ISO download requires an already finalized image and uses the same stored `volume_id` and represented bytes on every
  later download
- registering a copy cannot reduce archived coverage
- a physical copy is identified by `(volume_id, copy_id)`, never by `location`
- duplicate `copy_id` values are rejected within one finalized image/`volume_id`
- no collection id is an ancestor or descendant of another collection id
- the same canonical selector string means the same projected file set everywhere in API and CLI
