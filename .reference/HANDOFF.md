# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: nashspence/riverhog#869 — Make upload cache choice and post-upload copy intents durable.
Convention: nashspence/riverhog#903.
Audited base: `refs/heads/main` at `8ccdf279a3afc67a6287d70a5f76424bde7834c9`.
Audited base tree: `69cdd7a0ec3f3447997248acdc82a5b6ab1f11ce`.
Reference branch: `reference/external/869-durable-copy-to-handoff`.

Produced by: OpenAI ChatGPT.
Model: GPT-5.6 Sol (user-visible identity).
Configuration / effort: no additional configuration or effort setting exposed.
Requested by: the Riverhog repository user, acting as requester/maintainer; not the producing model.
Prepared: September 24, 2026.

## Purpose and contents

An isolated, executable server-side reference for durable upload-time `copy_to`
acceptance and publication-to-ordinary-copy-job handoff. All additions are under
`.reference/`; this branch does not change the live server, HTTP contract, client,
CLI, providers, release policy, generated contract atlas, or production schema.

`copy_to/handoff.py` supplies the SQLAlchemy transaction/state-machine prototype;
`copy_to/riverhog_bridge.py` supplies a concrete, transaction-scoped adapter against
inspected Riverhog classes/helpers. `copy_to/harness.py` is explicitly a reduced
catalog/authority/job test double, not the production catalog. The tests exercise
real file-backed database transactions, actual SIGKILL crashes, restarts, retries,
concurrent processes, permission revocation, attribution, and job observation.

This is a **prototype, not an end-to-end implementation**. The bridge has not been
executed against the full application. The upload-creation/finalization/discard
hooks, creation-identity integration, background scheduling, HTTP presentation,
and authoritative schema integration remain to be reconciled and wired. Client,
CLI, and provider integration are deliberately untouched as requested. See
`copy_to/README.md` for exact integration seams and design choices.

## Validation actually performed

- Focused reference suite: **81 passed, 1 skipped** (PostgreSQL runtime test).
- Eight parameterized actual SIGKILL/reopen tests: before/after acceptance commit,
  before/after publication commit, after job insertion, after event insertion,
  before handoff commit, and after handoff commit/lost response.
- Four competing subprocess sweepers: one job and one requested lifecycle event.
- SQLAlchemy PostgreSQL DDL compilation, including receipt and state constraints.
- Python syntax compilation of the prototype, harness, tests, crash worker, and bridge.
- Exact audited root-tree preservation and parent-commit identity are verified by
  the archive builder. The archive includes the Git bundle, exact-SHA manifest,
  patch, test evidence, and import instructions. See the external archive evidence
  for packaging checks performed after this handoff file's commit was assembled.

Execution environment: Python 3.13.5; SQLAlchemy 2.0.50; pytest 9.0.2;
SQLite 3.46.1. SQLite writer serialization is explicitly `BEGIN IMMEDIATE`.
It is not passed off as PostgreSQL row-lock or distributed-server validation.

## Not validated / known limitations

- Full repository checkout and dependency installation were unavailable in the
  execution environment (network/DNS access failed). Source inspection used the
  connected GitHub reader at the exact base above; see `AUDIT.md` for coverage.
- PostgreSQL execution/concurrency, actual Riverhog ORM/schema/service integration,
  HTTP behavior, provider operations, cache segmentation compatibility, migration
  application, and all client/CLI behavior are not validated.
- Repository `make lint`, `make unit`, `make dist-smoke`, `make build`, and GitHub
  Actions were not run. Focused reference tests do not replace those gates.
- Process-crash tests are not machine/power-loss, disk-corruption, HA, or multi-host
  failover qualification.
- Bootstrap, anonymous, and processing-capability actors are rejected for durable
  copy intents. The prototype supports attributable application-key principals.
- The registry port requires a stable, non-secret binding identity supplied by
  server configuration integration. The existing provider protocol is unchanged.
- `use_cache` is frozen as an operation choice only. Cache execution, policy
  reconciliation with #492, and provider qualification under #442 are not implemented.
- This reference introduces no independent durable database owner and no runtime
  schema upgrade. The reduced SQLite database exists only in tests.

## Authority and publication

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an
authorized repository agent does not itself establish or extend an accepted
design, contract, release requirement, or authorization to integrate these changes.
Authority remains with the owning issue, subsequent maintainer decisions, and the
repository's normal integration rail. Reconcile against then-current authoritative
state and issue decisions; do not apply this material mechanically.

The exact reference commit recorded in the archive's `MANIFEST.json` and in the
owning issue is the handoff identity. The branch name is navigation. This record
cannot contain its own commit hash without creating a circular identity.

No GitHub writes were made during local preparation. Remote publication must create the reference branch,
link it to #869 using GitHub's native **Development** relationship, and record the
exact reference SHA in #869. A Markdown link is not the native relationship. Do
not use an issue-closing keyword or create an integration PR as a substitute.
Do not rewrite a reference SHA once published.
