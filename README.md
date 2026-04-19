# Riverhog

Riverhog is a small, opinionated self-hosted archive service for me. I want to safely move files out of instant-access storage and onto optical containers. You stage a full collection tree anywhere under the uploads mount, then seal it by giving Riverhog that uploads-relative path, a description, and whether the buffered copy should be kept after archive. Riverhog catalogs the sealed collection, packs it into fixed-sized containers, and prepares on-demand ISOs. The UI points you at the relevant filesystem paths instead of serving individual collection files or hash-proof bundles directly.

## Runtime Notes

Configure ownership for bind-mounted data with `PREFERRED_UID` and `PREFERRED_GID` in `.env`. The API container bootstraps `/var/lib/archive` and `/var/lib/uploads`, fixes ownership recursively, applies group-shared directory permissions, sets a writable runtime home under `/var/lib/archive/runtime-home`, and then drops privileges to that UID/GID before starting the service.

Managed archive paths are intentionally flat:

- `/var/lib/uploads/<your-upload-dir>`
- `/var/lib/archive/buffered-collections`
- `/var/lib/archive/collection-exports`
- `/var/lib/archive/collection-hashes`
- `/var/lib/archive/container-roots`
- `/var/lib/archive/registered-isos`
- `/var/lib/archive/activation-staging`
- `/var/lib/archive/activated-containers`

If you want container-finalization notifications, set `CONTAINER_FINALIZATION_WEBHOOK_URL` in `.env`. Reminder behavior is configured with `CONTAINER_FINALIZATION_REMINDER_INTERVAL_SECONDS`; there is no UI or API endpoint for managing webhook subscriptions anymore.

## Testing

Use the Docker-based test path as the default way to run tests. The test image has all necessary dependencies for testing. `docker-compose.test.yml` bind-mounts the live repo into `/workspace` so targeted runs always use current source files without requiring a rebuild for every code edit.

`scripts/run-tests-in-dind.sh` prints the current phase, a rough expected duration, and a low-frequency heartbeat during long build or test stretches so the run never looks stalled.

Run the full suite:

```bash
./scripts/run-tests-in-dind.sh
```

Run a targeted file:

```bash
./scripts/run-tests-in-dind.sh tests/test_ui_smoke.py
```

Run a single test or filtered subset:

```bash
./scripts/run-tests-in-dind.sh tests/test_ui_playwright.py -k collection_seal_and_flush_flow
```
