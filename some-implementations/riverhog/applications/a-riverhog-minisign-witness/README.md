# Minisign collection witness

This supplied application independently witnesses immutable Riverhog collection identities.
It reads the official catalog sync API with an application key carrying `catalog:read`,
stores its own SQLite progress and observations, and signs the shared
`a-riverhog-collection-witness/v1` statement. The statement binds the source identity,
collection ID, archive-root SHA-256, and content identity. Mutable descriptions and tags do
not change that statement.

Its database retains statements, signatures, key identities, and old authorization-view
generations after a same-view departure or an explicit rebaseline. A same-view departure
records whether the collection was deleted or visibility was lost. On a grant/view change,
catalog sync stops; `rebaseline` starts a new generation without inventing departures for the
old one. An operator can independently compare old and new views if another application
needs effects for lost visibility.

## Run

Set `RIVERHOG_BASE_URL` and `RIVERHOG_TOKEN` for a generic Riverhog instance. Explicitly
upgrade the state file before running application work:

```sh
a-riverhog-minisign-witness --state ./witness.sqlite3 state upgrade
a-riverhog-minisign-witness --state ./witness.sqlite3 ingest
a-riverhog-minisign-witness --state ./witness.sqlite3 sign \
  --secret-key ./keys/secret.key --public-key ./keys/public.key
```

`ingest` accepts at most one catalog page and commits its observations, sign intents, and
cursor together. `sign` attempts one due job. Schedule both commands repeatedly, or run the
`run` command with the same key paths and a bounded `--poll-seconds` interval. An error exits
the worker; a supervisor can restart it against durable state. `state status` and
`state verify` are read-only. After a reset, inspect the reason and explicitly run
`rebaseline`; old signatures remain available.

The signer uses a mounted, unencrypted Minisign secret key readable only by its owner
(mode `0600` or tighter). It never prints key material. It verifies the signature with the
configured public key before retaining it. `evidence <statement-sha256>` emits JSON with
base64 statement and signature bytes plus the SHA-256 identity of the decoded public key.
The key identity is evidence of which key verified the signature; deployments must retain
the corresponding public key separately.

The app and image are published independently from Riverhog core. The image owns `/state`
for its SQLite file; mount a durable volume there and supply the Riverhog token and key files
through deployment-owned secrets. No real deployment identities belong in this repository.
