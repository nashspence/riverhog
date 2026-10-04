# Integration map: destination-scoped tag subscriptions

This is non-authoritative external implementation input under #903. The owning
issue linked from `../HANDOFF.md` owns scope, decisions and acceptance. This map
is not a production interface inventory and must not be copied to main as one.
The branch changes only `.reference/`; **the product feature is not implemented**.

## Start here

1. Read the owning issue and subsequent maintainer decisions, then current
   `AGENTS.md`, `release.toml` and the implementation at current main.
2. Run the standalone reference checks below. Treat their outcomes as evidence
   about this model, not the Riverhog SDK/server/CLI or any native platform.
3. Port/reproduce the useful invariants at the actual owners. Do not import
   `model.py`, adopt its SQLite DDL, or merge this directory into production.
4. Reconcile concurrent Stove0 work (including #953) and existing Gogurt
   lifecycle work (#515); do not create a second integration rail.

```sh
python -m unittest discover -s .reference/tag-subscriptions -p 'test_*.py' -v
python -m compileall -q .reference/tag-subscriptions
```

The model has no third-party dependencies. It opens only caller-created local
SQLite files in its tests. It does not read credentials, contact Riverhog,
materialize payloads, install a provider, schedule a task, or modify production
files. `placement-vectors.json` contains hand-authored golden paths and negative
syntax/calendar cases. No reference test is a substitute for the gates below.

## 1. Publication-time field and SDK extraction

Start at:

- `riverhog/src/riverhog_core/services/collection_uploads.py`:
  `_publish_catalog_collection`; keep `created_at=upload.opened_at` unchanged.
- `riverhog/src/riverhog_core/catalog_models.py`, `domain/models.py`,
  `services/collections.py`, `services/catalog_sync.py`, `catalog_events.py`.
- `riverhog/src/riverhog_api/schemas/collections.py`, `mappers.py`.
- `packages/riverhog-protocol/src/riverhog_protocol/catalog_sync.py`.
- `packages/riverhog-client/src/riverhog_client/{following,_catalog_sync,catalog_sync,client}.py`.
- `some-implementations/stove0/application/server/src/stove0_core/{catalog_predicate,admission,departure}.py`.

Add canonical `finalized_at` at the one-time publication transition, carry it
through summary/bootstrap/upsert/replica, and retain immutable consistency
checks. It is not a new archive-root or inventory field. Update the current
PostgreSQL baseline/fixture in place, without a new revision/version.

Extract exact tag-response validation and a persistence-free observation/match
reducer into `riverhog-client`. Have CLI and Stove0 use that one implementation;
keep each application's SQL, policy and effects outside the SDK. Preserve the
all-visible Stove0 selector separately from a required-tags predicate.

The current public follower drops the change page's `caught_up` flag. Add it
explicitly to `CatalogFollowBatch` (`None` for non-change batches). While already
following, a partially consumed horizon still has `after.phase == following`.
Do not use phase or an empty page as a finite-run completion signal.

Focused existing witnesses to extend/re-express:

- `packages/riverhog-client/tests/test_catalog_following.py`
- `packages/riverhog-client/tests/test_catalog_sync_engine.py`
- `tests/unit/test_catalog_sync.py`, `test_collection_tags.py`
- `tests/unit/test_collection_uploads.py`, `test_collection_descriptions.py`
- `tests/unit/test_public_interface_parity.py`, `test_response_contract_exactness.py`
- `some-implementations/stove0/application/tests/test_classification_admission.py`
- `some-implementations/stove0/application/tests/test_departure_effects.py`
- Both witness application test suites and their CatalogFollower wiring.

Port the reference membership, duplicate/stale, immutable metadata, reset-fence
and explicit run-boundary cases. Production must validate the complete actual
wire models and canonical JSON; the model deliberately does not duplicate those.

## 2. Materialization service and destination state

Start at `some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/`:
`local.py`, `local_state.py`, `state_migrations/v1_ddl.py`, `main.py`, `output.py`,
`cli_support.py`, `upload_progress.py`.

Separate service execution from Typer/environment/formatting. Take explicit
root, owned connection/state, expected exact collection identity, options and
progress sink. Keep one shared service for manual and subscription work. Replace
superseded global-root syntax/configuration; do not silently lose local audit,
repair, quarantine, eviction, list/show/filter/sort/IDs or provenance discovery.

Destination metadata needs root kind/UUID, frozen selector/source/partition and
layout, observation generations/serials, immutable collection identity, relative
path reservations, intents, retrieval-plan/job continuations, publication
receipts, suppressions and bounded run history. Use one application state owner,
one current baseline, multiple root instances. The reference DDL is a transaction
example only, not the production schema. Integrate with `state-schema` and its
current immutable DDL snapshot/metadata verification patterns.

Preserve `riverhog-materialization`'s actual `files/`, `artifacts/`, `provenance/`
layout and full retained history. The date directory is an outer wrapper.
`plan_materialization_spooled` already offers a bounded large-selection path.
Do not put a full arbitrary collection or its history into one JSON manifest.

Temporal policy stays application-owned; canonical UTC validation stays in
`time-formats`. Full nanoseconds are retained. Reserve a path with intent in the
root transaction. Collision-only suffix allocation is intentionally root-local;
it does not promise the same bare-name winner in independently built roots.
Never treat an unowned existing filesystem entry as a completed collection.

Extend `tests/unit/test_local_materialization.py`, `test_cli_help.py`,
`test_cli_json_output.py`, `test_cli_list_ids.py`, state fixture tests and the
shared materialization planner tests. Add actual filesystem fault tests for:

- absent/empty/nonempty/foreign-kind/corrupt/retired-state destinations;
- symlinks, junctions/reparse points, path budget and case-equivalence conflicts;
- missing mounted destination and replacement root UUID;
- manual and scheduled simultaneous invocation with lock + serial fencing;
- crash before and after publication, before and after DB completion;
- changed local files, quarantine, suppression surviving tag reentry;
- two roots containing the same collection without writable shared inodes;
- exact complete provenance, hints/fallbacks, lease renewal and acknowledgment;
- disconnected network, cache misses, quota errors, lost plan/job responses;
- inventories larger than page/window budgets, including one very large file.

A whole-directory rename is not universally a safe create-only publish. Qualify
actual filesystem primitives and retain explicit logical completion state. The
reference tests do not exercise this boundary.

## 3. Subscription commands and run truth

Add explicit create, reconcile, status, rebaseline and paginated runs commands.
The root owns the tag; reconcile and schedules take the initialized destination,
not a repeated tag or `--backfill` invocation option.

Use bounded CatalogFollower proposals, exact membership observations, one atomic
observation/intent/cursor transaction, and a separate due-work path. Capture
`caught_up` explicitly. Alternate work classes and retain per-intent retry state.
Do not let a poisoned collection or cold restore stop catalog progress or starve
other work; a run budget is not a logical-total limit.

Persist resets; fence delayed pages and errors. An explicit same-source
rebaseline preserves placements/obligations but reauthorizes unresolved remote
work. Never bind another source to the same root merely because IDs match.
Observe initialization is not an as-of snapshot or a guarantee of every event
since the create command's wall-clock time. Expired history requires explicit
rebaseline and cannot promise reconstruction of all historical matches.

`--json` keeps one final stdout result; incremental stderr JSONL is independently
framed and declared. Output must distinguish successful bounded pending work
from convergence, reset, conflict, busy and failure. Progress counters and history
are bounded presentation, not archive/cursor authority. Test parsing failures,
interruption, missing terminal receipts and read-only status under live work.

## 4. Scheduling: provider and application boundaries

Proposed new component roots:

- `some-implementations/riverhog/packages/a-riverhog-cli-scheduling/`
- `some-implementations/riverhog/scheduler-host/linux/`
- `some-implementations/riverhog/scheduler-host/macos/`
- `some-implementations/riverhog/scheduler-host/windows/`

Use distributions and the entry-point group named in the issue. The portable
component exposes typed bindings/lifecycle/host-state machinery, not tag logic.
Native providers remain explicitly selected component distributions. No component
imports the CLI application; the application composes them and provides the
reconciliation invocation. No Gogurt runtime dependency or semantic refactor.

Use Gogurt's `providers.py`, provider-reference model, `platform.py`, staged file
promotion/private-state helpers and native adapters as patterns. Reproduce only
appropriate semantics: timers/oneshots are not continuously healthy processes.
An idle schedule should not require heartbeat publication. Inspect the current
#515 decisions before adopting a status/settlement mechanism.

Persist one exact job/root/executable/provider/connection binding. Use a stable
namespaced job identity, safe argv encoding, current-user scope, explicit autorun,
coalesced due work and a destination execution lock in addition to native
non-overlap. A hidden invocation adapter records host failure evidence even when
the destination cannot be opened, then enters the same reconciliation service.
No registration should repeatedly run create/backfill or silently recreate a
missing disk mountpoint. Schedule removal retains destination data/state.

Run native tests only on the corresponding qualified platforms. Cover actual
register/inspect/enable/disable/remove, replacement rollback, process settlement,
multiple roots and coexistence with Gogurt, login/resume semantics, unavailable
roots, executable/credential failures, ACLs, and durable failure artifacts. Merely
rendering service/plist/XML text is not this proof.

## 5. Contract, installation and release reconciliation

Start at:

- root `pyproject.toml`, `uv.lock`, component `pyproject.toml`/licenses;
- `release.toml` state owners, component roles, installation and qualification;
- `scripts/release.py`, `release_installation.py`, `qualify_installation.py`;
- `scripts/contract_discovery.py`, `contract_freeze.py`, `extent_witnesses.py`;
- `scripts/documentation_readouts.py` and current contract/render machinery;
- `tests/unit/test_dependency_policy.py`, `test_workspace_boundaries.py`,
  `test_release.py`, `test_release_installation.py`, `test_qualify_installation.py`,
  `test_contract_freeze.py`, `test_configuration_connectivity.py`,
  `test_state_v1_fixtures.py`, `test_github_actions.py`;
- `.github/workflows/ci.yml` and its installed client-platform qualification.

Add application-specific schedule installation/reference/provider qualification
alongside the existing Gogurt listener model. Keep native provider packages out
of the mandatory CLI installation closure. Account for all new public exports,
configuration values, bounded extents, state, CLI output/exit selectors, artifacts
and licenses through actual executable owners. Remove retired global-local-root
surfaces instead of leaving stale discovery tokens.

Do not copy the issue/reference's Markdown command tables to main. Produce
current API/help/schema/readouts from implementation plus tested examples. Keep
README/architecture edits exceptional and source current-baseline changes from
their owners. No version bump or compatibility scaffolding. No weakening a frozen
boundary test to make the new topology appear already accepted.

Run focused tests while iterating, then the current repository gates. At the
pinned base, AGENTS names `make lint`, `make unit`, `make dist-smoke`, `make build`;
CI also includes compile, c2sp vectors, PostgreSQL concurrency, Compose smoke,
filesystem recovery, native client platforms and CodeQL/image checks. Reconcile
required scope against then-current policy and validate the actual integration
SHA. Do not enable external provider-resource provisioning/publication.

## What this reference has and has not proved

Modeled and exercised: temporal syntax/calendar/nanosecond paths, exact tag
receipt matching, root-local collision allocation, observation/revision/match
semantics, transaction rollback/reopen, stale-worker generation/serial rejection,
reset preservation, eviction suppression, more than one catalog page of work,
and the distinction between idle schedule registration and an active process.

Not implemented or proved: production DDL/API changes; use of the real SDK and
canonical models; real artifact/provenance/retrieval behavior; destination locks
and safe filesystem publication; structured CLI progress/run ledgers; credentials
or ACLs; native scheduler components; release/discovery/installation wiring; full
repository tests; native CI or release qualification. The integration agent owns
these steps and must not convert model results into passed release claims.
