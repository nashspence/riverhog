# Archive storage MVP

A minimal, self-hosted archive backend with:

- a single shared bearer token for API auth plus a shared tus hook secret
- resumable large file uploads through `tusd`
- replayable real-time upload and ISO download progress streams
- a catalog-first job view that includes offline files and explains why a file is unavailable
- an online-only `exports/jobs/...` tree for local mounts
- partition closure based on the provided MILP reference implementation, adapted into the service runtime
- verified partition-root cache uploads matched against a database-stored complete root hash
- ISO authoring from closed partition roots with `xorriso` using ISO9660 + Joliet + Rock
  Ridge + UDF for broad data-disc compatibility
- automatic hot-buffer cleanup after user-confirmed successful burns unless the job opts
  out at creation time

The partitioning logic in this MVP is derived from the user-provided reference implementation. fileciteturn0file0

## Stack

- **FastAPI** for the API
- **tusd** for resumable uploads and hooks
- **Redis** for SSE progress streams
- **SQLite** for the catalog
- **NumPy + SciPy** for the partition MILP
- plain local files for hot buffer, cached partitions, generated partition roots, and exports

## Layout

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
      MANIFEST.jsonl
      files/...
      files/...meta.yaml

  cold/
    isos/<disc_id>.iso
```

## Run

```bash
cp .env.example .env
docker compose up --build
```

All public API calls require:

```bash
-H "Authorization: Bearer $API_TOKEN"
```

API docs:

- OpenAPI: `http://localhost:8080/docs`
- tusd endpoint: `http://localhost:1080/files`

## Core flow

### 1. Create a job

```bash
curl -X POST http://localhost:8080/v1/jobs   -H 'content-type: application/json'   -d '{"description":"photos from trip"}'
```

Set `"keep_buffer_after_archive": true` if the original uploaded files should stay in the
hot buffer even after their archived discs have been burned successfully.

### 2. Reserve file uploads

```bash
curl -X POST http://localhost:8080/v1/jobs/20260417T060811Z/uploads   -H 'content-type: application/json'   -d '{
    "relative_path": "photos/raw/frame001.dng",
    "size_bytes": 104857600,
    "sha256": null,
    "mode": "0644",
    "mtime": "2026-04-17T06:08:11Z"
  }'
```

Then upload through tus using the returned metadata.

### 3. Stream upload progress

```bash
curl -N http://localhost:8080/v1/progress/uploads/<upload_id>/stream
curl -N http://localhost:8080/v1/progress/jobs/20260417T060811Z/stream
```

### 4. Seal the job and let the planner ingest it

```bash
curl -X POST http://localhost:8080/v1/jobs/20260417T060811Z/seal
```

This copies the uploaded job into the partition planner state, may close one or more new partitions under `/var/lib/archive/partitions/roots/<disc_id>/`, and imports every closed partition into the database. The on-disc sidecars follow the requested `sidecar/v1` schema.

### 5. Browse a complete job tree, including offline files

```bash
curl http://localhost:8080/v1/jobs/20260417T060811Z/tree
```

### 6. Author or register a burnable ISO for a closed partition

Author the ISO directly from `/var/lib/archive/partitions/roots/<disc_id>/`:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/iso/create \
  -H "Authorization: Bearer $API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"volume_label":"ARCHIVE_20260417"}'
```

If an external ISO step writes `/var/lib/archive/partitions/roots/<disc_id>.iso` or
another server-visible path, register it instead:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/iso/register \
  -H "Authorization: Bearer $API_TOKEN" \
  -H 'content-type: application/json' \
  -d '{"server_path":"/var/lib/archive/somewhere/20260417T091500Z.iso"}'
```

Then create a tracked download session:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/download-sessions
curl -N http://localhost:8080/v1/progress/downloads/<session_id>/stream
```

After the user has successfully burned the ISO, confirm it so the service can release
the hot-buffer originals for eligible jobs:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/burn/confirm \
  -H "Authorization: Bearer $API_TOKEN"
```

### 7. Cache a known partition root

Create a cache session for a known partition:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/cache/sessions
```

Create upload slots for expected partition-root files such as `MANIFEST.jsonl`, `files/0`, or `files/0.meta.yaml`, then upload them through tus. When all files are present:

```bash
curl -X POST http://localhost:8080/v1/discs/20260417T091500Z/cache/sessions/<session_id>/complete
```

The service hashes the complete uploaded root directory and rejects it unless it matches the database-stored partition contents exactly.

Remove a cached partition:

```bash
curl -X DELETE http://localhost:8080/v1/discs/20260417T091500Z/cache
```

## Mounting

Bind mount this read-only anywhere you want:

- `/var/lib/archive/exports/jobs`

Only files that are online right now appear there. Offline files remain visible through the API tree endpoints.
