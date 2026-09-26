# #509 retrieval owner and checkpoint reference

## Status and provenance

This is non-authoritative external reference material for
[nashspence/riverhog#509](https://github.com/nashspence/riverhog/issues/509), following
[#903](https://github.com/nashspence/riverhog/issues/903). It is not an integration,
issue-closure claim, release approval, or provider qualification result.

- Reference branch: `reference/external/509-retrieval-owner-checkpoints`.
- Audited base: `refs/heads/main` at
  `18c5f287e7acb81ceaa84f5a3bf8deadbc3852e8`.
- Base tree: `8962d898f48401cb3107a3d7e4f2975866d4d2b8`.
- Producer: OpenAI ChatGPT; exposed model identity: GPT-6 Astra Pro.
- Model configuration / effort setting: not exposed.
- Requester: the Riverhog integration agent, relayed by the maintainer in this chat.
- Publisher: the connected GitHub account on the maintainer's behalf. Publication
  does not change the producer identity.
- Prepared: 2026-09-26. The owning-issue comment records the actual published commit
  SHA; the containing commit is the immutable identity of this handoff.

The original workflow and existing workflow-policy test were reconstructed from
pinned GitHub reads and verified byte-for-byte against their Git blob identities
before modification: `791f2540e59abb5b892f45486f3b55c776a75dbf` and
`952a52b03be25a93d263d903540c8a727ce36060`, respectively. Unchanged files are inherited
from the exact base tree, not recreated from a partial local checkout.

## Findings and implementation

The audited `_qualification_api` already creates once, persists the qualification
key ID, rotates the same key on continuation, and closes clients without revoking
a pending owner. This reference preserves that behavior rather than reintroducing
a parallel credential lifecycle. The missing pieces are checkpoint/DB pairing,
pre-upload owner/liveness validation, and terminal-only revocation wiring.

`scripts/provider_qualification_checkpoint.py` adds an internal, off-provider
checkpoint helper. It continues to use `provider_qualification.load_checkpoint`
and its policy constants rather than inventing a second checkpoint parser.

Before packaging a nonterminal continuation, the workflow stops the disposable
app, captures a size-bounded custom dump, restores that exact dump into a temporary
database, and reads an explicit ownership/lifecycle projection. It verifies the
checkpoint key, application principal, collection, job, consumed plan, plan etag,
and plan collection membership. It rejects missing rows, wrong owners, revoked or
under-lived keys, lost qualification access/quota, nonlive retrievals, expired
ready leases, and expired pending lifetimes. A `verified` checkpoint is deliberately
cleanup-only and requires a completed acknowledged job, not a live retrieval.

The proposed key-expiry margin is 24 hours beyond the recorded restore deadline.
A nonexpiring key covers this window; finite expiries must cover the full window
and still be active. This is an explicit conservative policy choice for review,
not a measured provider timing guarantee. Terminal `cleaned` / `failed` checkpoints
revoke only their recorded owner through the local Riverhog API; nonterminal
returns and failures do not construct a revocation client. Revocation failure
blocks packaging. No raw token or token hash is selected by the snapshot query.

A new `continuation.json` binds the run, source, logical checkpoint digest, exact
checkpoint-file bytes, and exact dump bytes. Failed capture/restore/validation/
cleanup cannot retain an old valid seal. Packaging rechecks the copied pair;
polling verifies it before B2 checks and again after copying state, before restore.
Terminal checkpoints cannot be resumed as active pairs. These hashes detect mixed
artifacts; they are not signatures, a trust boundary against a malicious artifact
producer, or release evidence. Only trusted workflow-produced dumps may be restored.

The workflow preserves its existing main-only authority, source pinning, manual
restart path, provider environments, permissions, secret scopes, schedules, and
release restrictions. No schema, public API, or provider policy is changed.

## Validation performed

In a partial local source tree, with no provider credentials or live API calls:

```sh
python -m pytest -q tests/unit/test_provider_qualification_checkpoint.py \
  tests/unit/test_github_actions.py::test_provider_qualification_is_resumable_dummy_only_and_cloudfront_required
```

Result: **96 passed**. This includes 95 focused helper cases plus the updated
existing provider-workflow policy test. Subprocess tests use a command spy and
synthetic dump bytes, verify that the captured bytes are the restored bytes,
and cover failure of dump, database creation, restore, query, cleanup, changing
checkpoints, mixed artifacts, owner mismatches, and terminal-only revocation.

All 23 workflow `run` blocks passed `bash -n` without execution. The three changed
Python files passed Python byte-compilation. The modified-file diffs were reviewed
against the verified pinned originals. These checks are not full-workspace CI.

## Not performed / integration obligations

No real PostgreSQL dump/restore/SQL execution, Docker run, live provider request,
GitHub workflow dispatch, release qualification, release branch update, tag, or
issue closure was performed. The local environment lacks Docker/PostgreSQL, mise,
and the locked workspace. Ruff was unavailable and its installation attempt did
not resolve a package, so repository lint/format/mypy and full unit/build gates
are not claimed. The original provider module's full test suite was not run.

The integrating agent must run the focused command in the locked workspace, the
normal repository gates, and a disposable PostgreSQL round-trip test with the real
schema before adopting this workflow. In particular, validate the SQL projection,
permission requirements, pending-timeout semantics, terminal cleanup retry, and
failure-gated artifact behavior against the integrated source SHA.

Two pre-existing workflow/runtime discrepancies were observed but intentionally
not changed in this narrow reference: the workflow calls `runtime-env` while the
pinned provider script exposes `runtime-config`, and it inspects an inline database
URL despite the script's file-mounted runtime configuration. Reconcile those in
the planned provider session. The selected qualification source must contain the
new helper; old pinned sources/artifacts are not retrofitted. Legacy continuations
without the pairing manifest must be explicitly discarded/restarted, not silently
accepted. No restart or stale-artifact cleanup was executed here. The existing
explicit restart rail discards the superseded disposable database; this reference
does not add restoration of that old database merely to revoke its key.

Integrate or selectively rework this material through the current normal rail on
`main`, after review and local validation. Do not promote this reference branch to
release authority. Leave live provider runs, fresh canary handling, and release
qualification to their planned session; #509 remains open until its actual
acceptance criteria are demonstrated.

## GitHub association limitation

The available GitHub actions can create the branch and comment on its owner issue,
but expose no native issue-Development branch-link action. This limitation must be
recorded explicitly on #509; a Markdown link is not claimed as the native link
required by #903. A maintainer/integration agent should establish that native
Development association through GitHub's supported UI/API when available.

## Research basis

Pinned repository sources: `scripts/provider_qualification.py`,
`.github/workflows/provider-qualification.yml`, `tests/unit/test_github_actions.py`,
`tests/unit/test_provider_qualification.py`, the Riverhog `catalog_models.py`,
`services/app_keys.py`, `services/retrieval.py`, and application-key API schemas.
The principal is the application name itself; job ownership additionally binds
the stable key ID. Secrets and token material are intentionally outside the query.

PostgreSQL primary documentation consulted for exact-dump restore, fail-fast
restore options, and psql variable quoting:
[pg_restore](https://www.postgresql.org/docs/18/app-pgrestore.html) and
[psql](https://www.postgresql.org/docs/18/app-psql.html).
