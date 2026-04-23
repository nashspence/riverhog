# Configuration Reference

## `ARC_STAGING_ROOT`

- type: absolute path
- default: `/staging`

This is the filesystem directory the API treats as the backing root for logical request paths beneath `/staging/...`.

For example, with `ARC_STAGING_ROOT=/srv/archive/staging`, the request path `/staging/photos/2024` resolves to the
real directory `/srv/archive/staging/photos/2024`.

## `ARC_DB_PATH`

- type: absolute or relative path
- default: `.arc/state.sqlite3`

This is the SQLite catalog path used for durable authoritative API state.

## `INCOMPLETE_UPLOAD_TTL`

- type: duration
- default: `24h`

This controls how long incomplete server-side upload state for one fetch-manifest entry may remain resumable after the
last successfully accepted chunk.

Service restart does not shorten this TTL or discard unexpired upload state by itself.

When the TTL expires:

- incomplete upload bytes are discarded
- the entry returns to `pending`
- the fetch manifest returns to `waiting_media` if any selected bytes are still not hot
- `upload_state_expires_at` becomes `null` until a new upload session is opened
