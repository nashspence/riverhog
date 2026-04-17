# Archive Storage MVP

Minimal, self-hosted archive backend for large file sets that need to move from
an online hot buffer into partitioned offline media, with strong cataloging and
re-hydration behavior throughout the lifecycle.

This README is written to make the product surface easy to understand and easy
to test rigorously.

## What This Service Does

The service accepts resumable uploads for archive jobs, validates and catalogs
them, feeds them into a partition planner, emits partition roots suitable for
offline archival media, optionally authors downloadable ISOs for those
partitions, and lets known partition roots be cached back into hot storage
later so archived files become online again.

The core product promise is:

- uploads are resumable and validated
- the catalog always knows what a job contains
- offline files stay visible in the API even when their bytes are unavailable
- partition roots are deterministic enough to be verified by content hash
- cached partition roots are accepted only when they exactly match what was
  archived
- hot-buffer retention is explicit and conservative

## Key Features

### 1. Minimal auth for a private deployment

- All public `/v1/...` endpoints require a shared bearer token.
- Internal tus hook callbacks require a separate shared hook secret.
- The design stays intentionally small because the service is expected to run on
  a trusted self-hosted network behind VPN.

### 2. Resumable job uploads through `tusd`

- Jobs are created first, then individual file upload slots are reserved.
- Each upload slot binds a path, size, and optional expected SHA-256.
- `tusd` hook validation rejects uploads whose metadata does not match the
  reserved slot.
- Upload completion computes SHA-256 and records the finished file into hot
  storage.

### 3. Catalog-first job model

- Jobs can contain explicit directories plus uploaded files.
- Paths are normalized and prevented from escaping the job root.
- A job tree endpoint lists the full logical structure of the job, including
  files that are offline on archived discs.
- File access endpoints return content when online and a structured offline
  explanation when not.

### 4. Real-time progress streams

- Upload progress is streamed per upload.
- Aggregate upload progress is streamed per job.
- Cache-session upload progress is streamed per cache session.
- ISO download progress is streamed per download session.
- Streams are replayable SSE feeds backed by Redis streams.

### 5. Planner-driven partition closure

- Sealing a job ingests its hot-buffer files into the planner state.
- Jobs may close zero, one, or multiple partitions depending on fill rules.
- Large files may be split across partitions when necessary.
- Closed partitions are emitted as rooted directory trees under
  `partitions/roots/<disc_id>/`.
- Closed partitions are also imported back into the catalog as discs, entries,
  and archive-piece mappings.

### 6. Archive metadata and sidecars

- Every closed partition contains an encrypted `MANIFEST.jsonl`.
- Every closed partition also contains a plaintext `README.txt` with manual
  recovery instructions for that specific disc.
- Payload files live under `files/...`.
- Each payload has an encrypted YAML sidecar using the requested
  `sidecar/v1` schema.
- Split files carry part metadata so they can be reconstructed later.
- Except for `README.txt`, every leaf file emitted into a partition root is
  encrypted individually with `age` plus `age-plugin-batchpass`.
- The batchpass scrypt work factor can be tuned with
  `AGE_BATCHPASS_WORK_FACTOR` and `AGE_BATCHPASS_MAX_WORK_FACTOR`; tests use a
  lower factor so encrypted-disc coverage stays fast enough to run routinely.

### 7. Offline-aware file access

- If a job file is still in hot buffer, it is directly available.
- If a whole archived payload is cached back online, it is directly available.
- If a split file has all required chunks online across cached partitions, the
  service materializes it on demand and serves the reconstructed file.
- If required disc data is unavailable, the API responds with a clear offline
  error that includes candidate disc IDs.

### 8. Verified partition-root cache uploads

- A cache session can be created only for a known disc.
- Upload slots can be created only for paths already known to belong to that
  partition root.
- Completing a cache session computes a canonical hash over the uploaded tree.
- The uploaded root is accepted only if hash, file set, file sizes, and file
  digests all exactly match the archived partition contents.
- Once accepted, that cached root can satisfy job and disc content reads again.

### 9. Disc browsing and content access

- Disc tree endpoints expose the logical file tree of a closed partition.
- Disc content endpoints serve bytes directly from a cached partition root when
  online.
- Disc reads remain catalogable even when the partition is offline.

### 10. ISO authoring and download

- The service can author an ISO directly from a closed partition root.
- It uses `xorriso` with ISO9660, Joliet, Rock Ridge, and UDF options to favor
  broad cross-platform compatibility for file-based Blu-ray workflows.
- An external ISO can also be registered instead of authored internally.
- Download sessions are tracked separately from the ISO file itself.
- ISO content downloads emit their own progress stream.

### 11. Conservative hot-buffer retention

- By default, hot-buffer originals are removed only after all archived bytes for
  a job are represented on discs and every related disc has been
  burn-confirmed.
- Jobs can opt out of that cleanup at creation time with
  `keep_buffer_after_archive=true`.
- A manual buffer-release endpoint also exists for explicit operator control.

### 12. Online export tree for local mounts

- The service maintains `exports/jobs/<job_id>/...` as an online-only view.
- Only currently available files appear there.
- This makes it safe to bind mount exported jobs into other local workflows
  without pretending offline bytes still exist.

## Stack

- FastAPI for the API
- `tusd` for resumable uploads and hook callbacks
- Redis for replayable SSE progress streams
- SQLite for the catalog
- NumPy and SciPy for the partition MILP logic
- plain local files for hot buffer, partition roots, cached discs, materialized
  reconstructions, exports, and cold ISOs

## Filesystem Layout

```text
/var/lib/archive/
  catalog/
    catalog.sqlite3

  tusd/
    incoming/

  hot/
    buffer/jobs/<job_id>/...
    cache/staging/<session_id>/...
    cache/discs/<disc_id>/...
    materialized/jobs/<job_id>/...

  exports/
    jobs/<job_id>/...

  partitions/
    state/state.json
    state/pool/<job_id>/...
    roots/<disc_id>/
      README.txt
      MANIFEST.jsonl
      files/...
      files/...meta.yaml

  cold/
    isos/<disc_id>.iso
```

## API Surface

### Jobs

- `POST /v1/jobs`
  Creates a job. Supports the retention override
  `keep_buffer_after_archive`.
- `POST /v1/jobs/{job_id}/directories`
  Registers an explicit directory entry for a job.
- `POST /v1/jobs/{job_id}/uploads`
  Reserves a file upload slot and returns tus metadata plus progress URLs.
- `POST /v1/jobs/{job_id}/seal`
  Verifies uploaded files are present, ingests the job into planner state, and
  imports any newly closed discs.
- `GET /v1/jobs/{job_id}/tree`
  Returns the full logical job tree, including offline files.
- `GET /v1/jobs/{job_id}/content/{relative_path}`
  Serves file bytes when online, otherwise returns a structured offline error.
- `POST /v1/jobs/{job_id}/buffer/release`
  Explicitly removes hot-buffer originals for the job.

### Discs

- `POST /v1/discs/flush`
  Forces pending planner state to close discs when requested.
- `GET /v1/discs/{disc_id}/tree`
  Returns the logical tree of a closed partition.
- `GET /v1/discs/{disc_id}/content/{disc_relative_path}`
  Serves bytes from a cached partition root.
- `POST /v1/discs/{disc_id}/cache/sessions`
  Starts a verified cache-upload session for a known partition.
- `GET /v1/discs/{disc_id}/cache/sessions/{session_id}/expected`
  Lists the exact expected files for that cache session.
- `POST /v1/discs/{disc_id}/cache/sessions/{session_id}/uploads`
  Reserves an upload slot for one expected cache file.
- `POST /v1/discs/{disc_id}/cache/sessions/{session_id}/complete`
  Verifies and activates the uploaded partition root.
- `DELETE /v1/discs/{disc_id}/cache`
  Evicts an active cached partition root.
- `POST /v1/discs/{disc_id}/iso/create`
  Authors an ISO from the partition root.
- `POST /v1/discs/{disc_id}/iso/register`
  Registers an externally created ISO file.
- `POST /v1/discs/{disc_id}/burn/confirm`
  Confirms successful burn and triggers eligible hot-buffer cleanup.
- `POST /v1/discs/{disc_id}/download-sessions`
  Creates a tracked ISO download session.
- `GET /v1/discs/downloads/{session_id}/content`
  Streams ISO bytes.

### Progress streams

- `GET /v1/progress/uploads/{upload_id}/stream`
- `GET /v1/progress/jobs/{job_id}/stream`
- `GET /v1/progress/cache-sessions/{session_id}/stream`
- `GET /v1/progress/downloads/{session_id}/stream`

### Internal upload hooks

- `POST /internal/tusd-hooks`
  Handles tus pre-create, post-create, post-receive, post-finish, and
  post-terminate events.

## Core Flow

### 1. Create a job

```bash
curl -X POST http://localhost:8080/v1/jobs \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "content-type: application/json" \
  -d '{
    "description": "photos from trip",
    "keep_buffer_after_archive": false
  }'
```

### 2. Reserve file uploads

```bash
curl -X POST http://localhost:8080/v1/jobs/20260417T060811Z/uploads \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "content-type: application/json" \
  -d '{
    "relative_path": "photos/raw/frame001.dng",
    "size_bytes": 104857600,
    "sha256": null,
    "mode": "0644",
    "mtime": "2026-04-17T06:08:11Z"
  }'
```

Upload the file through tus using the returned metadata.

### 3. Watch upload progress

```bash
curl -N http://localhost:8080/v1/progress/uploads/<upload_id>/stream \
  -H "Authorization: Bearer $API_TOKEN"

curl -N http://localhost:8080/v1/progress/jobs/20260417T060811Z/stream \
  -H "Authorization: Bearer $API_TOKEN"
```

### 4. Seal the job

```bash
curl -X POST http://localhost:8080/v1/jobs/20260417T060811Z/seal \
  -H "Authorization: Bearer $API_TOKEN"
```

This moves the job into planner state, may close new partitions, and imports any
closed partitions into the catalog.

### 5. Browse the job tree

```bash
curl http://localhost:8080/v1/jobs/20260417T060811Z/tree \
  -H "Authorization: Bearer $API_TOKEN"
```

### 6. Author or register an ISO

Author directly from a partition root:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/iso/create \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "content-type: application/json" \
  -d '{"volume_label":"ARCHIVE_20260417"}'
```

Or register an externally produced ISO:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/iso/register \
  -H "Authorization: Bearer $API_TOKEN" \
  -H "content-type: application/json" \
  -d '{"server_path":"/var/lib/archive/somewhere/20260417T091500Z.iso"}'
```

### 7. Track ISO download progress

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/download-sessions \
  -H "Authorization: Bearer $API_TOKEN"

curl -N http://localhost:8080/v1/progress/downloads/<session_id>/stream \
  -H "Authorization: Bearer $API_TOKEN"
```

### 8. Confirm a successful burn

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/burn/confirm \
  -H "Authorization: Bearer $API_TOKEN"
```

### 9. Cache a known partition root

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/cache/sessions \
  -H "Authorization: Bearer $API_TOKEN"
```

Then reserve cache upload slots for expected files and finish the session:

```bash
curl -X POST \
  http://localhost:8080/v1/discs/20260417T091500Z/cache/sessions/<session_id>/complete \
  -H "Authorization: Bearer $API_TOKEN"
```

## Running Locally

```bash
cp .env.example .env
docker compose up --build
```

Public API calls require:

```bash
-H "Authorization: Bearer $API_TOKEN"
```

Primary local endpoints:

- OpenAPI: `http://localhost:8080/docs`
- tusd: `http://localhost:1080/files`

## Coverage Map

These are the major areas a rigorous test suite should cover.

### Auth and request boundary

- missing bearer token
- wrong bearer token
- missing hook secret
- wrong hook secret
- public route allowed with correct token
- internal hook route allowed with correct secret

### Path and metadata validation

- invalid relative paths
- duplicate file path reservation within a job
- invalid file mode format
- invalid timestamp format
- mismatched upload size
- upload metadata path not matching reserved slot

### Upload lifecycle

- happy path from slot reservation to completed file
- post-receive progress updates
- SHA-256 mismatch failure
- terminated upload failure
- missing incoming tusd file on finish

### Job sealing and planner ingestion

- sealing nonexistent job
- sealing already sealed job
- sealing with missing uploads
- sealing empty job
- sealing job that closes no discs
- sealing job that closes one or more discs
- planner output imported into catalog

### Job and disc read semantics

- online job file read from hot buffer
- online job file read from cached disc
- split-file reconstruction from cached chunks
- offline job file response with disc IDs
- online disc file read from cache
- offline disc file response

### Cache-session verification

- cache session creation for known disc
- cache session creation for unknown disc
- cache upload slot for known path
- cache upload slot rejection for unknown path
- cache completion hash mismatch
- cache completion file-set mismatch
- cache completion success
- cache eviction success

### ISO flow

- ISO authoring success
- ISO authoring failure when tool is unavailable or root is missing
- ISO overwrite guard
- external ISO registration success
- download session creation for registered/authored ISO
- ISO streaming progress transitions

### Retention and cleanup

- default cleanup after all related discs are burn-confirmed
- no cleanup before burn confirmation
- no cleanup when archived bytes are incomplete
- no cleanup when job opted into keeping hot buffer
- manual buffer release

### Export tree behavior

- exports tree includes online files only
- exports tree updates after upload completion
- exports tree updates after cache activation
- exports tree updates after cache eviction
- exports tree updates after buffer release

## Current Non-Goal

Still not implemented:

- ingesting entire jobs as tar streams instead of file-by-file tus uploads
