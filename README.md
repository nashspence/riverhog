# cold-archive stack

A deliberately small self-hosted stack for your optical cold-archive workflow:

- files are uploaded with their original names recorded in SQLite
- payloads are renamed into conventional content-addressed object keys in Garage
- when staged bytes exceed the threshold, the app seals one ~50 GB archival ISO
- the full package contents are encrypted with a password using `age -p`
- the plaintext manifest, its `.ots` proof, BagIt manifests, and payload files are all inside the encrypted archive blob
- the ISO itself only contains that encrypted blob
- a Home Assistant webhook is called when a disc is ready
- each disc gets a stable manifest URL like `/d/<iso_sha256>` that works well as a QR label target
- after you manually confirm the burn, the local ISO is deleted and the staged online copies are removed
- you can later upload the same ISO again to re-expose the objects through the same S3 bucket/object keys until you remove it

## quick start

```bash
cp .env.example .env
mkdir -p data/{garage-meta,garage-data,archive-state,packages,rehydrated}
docker compose up -d --build
```

Exposed services:

- Garage S3 API: `http://localhost:3900`
- Archive API / docs: `http://localhost:8000/docs`

## main API calls

Upload a file:

```bash
curl -F file=@movie.mkv http://localhost:8000/ingest
```

Seal a disc immediately:

```bash
curl -X POST http://localhost:8000/seal
```

Download the ISO for package 1:

```bash
curl -OJ http://localhost:8000/packages/1/download
```

Open the QR-style manifest page for a disc:

```bash
open http://localhost:8000/d/<iso_sha256>
```

Get the same manifest as JSON:

```bash
curl http://localhost:8000/d/<iso_sha256>.json
```

Mark package 1 as physically burned:

```bash
curl -X POST http://localhost:8000/packages/1/burned
```

Rehydrate from a previously burned ISO:

```bash
curl -F file=@<iso-sha>.iso http://localhost:8000/rehydrate
```

Remove a rehydrated package again:

```bash
curl -X DELETE http://localhost:8000/rehydrated/1
```

## notes

- `DISC_PASSPHRASE` is used to encrypt and decrypt the per-disc archive blob with `age` passphrase mode.
- `manifest.json` is timestamped with OpenTimestamps before the encrypted archive is sealed.
- the public disc URL is database-backed, so it remains readable even after the local ISO has been disposed.
- this is intentionally minimal: Garage + one small FastAPI service + SQLite.

## testing

The integration suite boots the real compose stack in dind with isolated temp
storage, uploads fixture data, seals a disc, burns it, rehydrates it, and then
verifies cleanup.

```bash
uv pip install --system -r requirements-dev.txt
pytest tests -q
```
