# Configuration Reference

## `ARC_SEAWEEDFS_FILER_URL`

- type: URL
- default: `http://localhost:8888`

This is the SeaweedFS Filer base URL Riverhog uses for committed collection files and fetch recovery upload targets.

Committed hot collection content lives at `/collections/{collection_id}/{path}` within the filer namespace.

Completed encrypted recovery uploads live at `/.arc/recovery/{fetch_id}/{entry_id}.enc`.

## `ARC_DB_PATH`

- type: absolute or relative path
- default: `.arc/state.sqlite3`

This is the SQLite catalog path used for durable authoritative API state.

## `INCOMPLETE_UPLOAD_TTL`

- type: duration
- default: `24h`

This controls how long incomplete server-side upload state for one collection-upload file or one fetch-manifest entry
may remain resumable after the last successfully accepted chunk.

Service restart does not shorten this TTL or discard unexpired upload state by itself.

When the TTL expires:

- for collection ingest, the incomplete collection file upload target is deleted and that file returns to `pending`
- the pending SeaweedFS TUS session is cancelled
- any incomplete recovery target object is deleted
- the fetch entry returns to `pending`
- the fetch manifest returns to `waiting_media` if any selected bytes are still not hot
- `upload_state_expires_at` becomes `null` until a new upload session is opened

## `UPLOAD_EXPIRY_SWEEP_INTERVAL`

- type: duration
- default: `30s`

This controls how often Riverhog's background expiry reaper sweeps collection-upload and fetch-upload state looking
for entries whose published `INCOMPLETE_UPLOAD_TTL` has already elapsed.

Lower values reduce how long expired upload state may remain present after its TTL boundary. Higher values reduce
background sweep frequency at the cost of slower cleanup after expiry.
