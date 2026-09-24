# Source audit and provenance

All implementation reads were pinned to
`8ccdf279a3afc67a6287d70a5f76424bde7834c9`, not a floating branch.
The GitHub reader returned the exact `main` head and root tree at preparation.
The local environment could not clone the repository; unaffected baseline objects
are therefore represented by their exact audited tree entries in the incremental
bundle. The publisher needs an existing clone with the base commit and its objects.
This is not a complete offline repository backup.

## Authority reviewed

- https://github.com/nashspence/riverhog/issues/869 — accepted v1 scope and historical intake; issue reported no comments at read time.
- https://github.com/nashspence/riverhog/issues/903 — external-reference naming, handoff, native Development relationship, exact-SHA identity, and non-authority rules.
- https://github.com/nashspence/riverhog/issues/903#issuecomment-5789652477 — process correction; no AGENTS.md change authorized by that convention update.

## Pinned implementation coverage

| Path | Blob SHA | Read scope / purpose |
| --- | --- | --- |
| `AGENTS.md` | `73c0da955c0f35aca8d8d7fa741ef68b4af5f724` | Full; boundaries, database ownership, validation, integration rail |
| `README.md` | `a5f105ce208d248bf42efcc47c7ae598fcbad795` | Full; product and repository entrypoint |
| `.github/workflows/ci.yml` | `d37d6f91c6cf8010cacbe4423115fd825876f5d1` | Full; actual CI gates; not executed |
| `riverhog/src/riverhog_core/services/collection_uploads.py` | `273197ea3fffd7bbf48a91e39ad1e46da1bcc2f1` | Lines 1–570; imports, constructor, creation/resume identity, reserved collection ID, initial tags, upload access |
| `riverhog/src/riverhog_core/services/app_keys.py` | `97602a937bce1d545e1fa53dadfd038dce9a2c62` | Lines 1–260; key/grant loading, current active/expiry/revocation semantics, Principal construction |
| `riverhog/src/riverhog_core/app_permissions.py` | `c677160348b50198319a19632a529e53e555872b` | Full; Principal and shared access interfaces |
| `riverhog/src/riverhog_core/collection_access.py` | `0c357b0442d271d063491487d49005c3636f45cd` | Full; create access, published collection access, resource/tag scope, capability distinction |
| `riverhog/src/riverhog_core/services/archive_copy_jobs.py` | `6f5278d3ab861bb26b06d40843c055593f289534` | Visible prefix through create/resume, cancel/get/list, startup requeue, and beginning of process_due; exact pinned path, not a whole-file audit |

The copy-job prefix establishes that `create_or_resume` owns its own session,
uses `(collection_id, destination_store)` identity, emits a requested event, and
resets terminal non-transfer jobs on explicit resume. It also establishes the
helper/model names used by `riverhog_bridge.py`. That is why the handoff must not
blindly call it after publication or on replay.

Final publication/discard implementation, the complete source selector/emitter,
complete catalog model and state-baseline definitions, HTTP router authorization,
and integration tests were **not** fully read or executed. The reference uses
explicit proposed server hooks, not purported patches to unseen code. Before
integration, audit those paths at the then-current authoritative base, including
all lifecycle, cleanup, and lock-order interactions.

The root tree was read in full, is not truncated, and is preserved for all baseline
paths. The delivery's exact commit adds only `.reference/`; no implementation,
client, CLI, provider, policy, or baseline files are deleted or replaced.
