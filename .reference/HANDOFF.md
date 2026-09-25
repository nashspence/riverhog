# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: [#908](https://github.com/nashspence/riverhog/issues/908)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Audited base: `main` @ `06a5153f166c1611e94d4a3637e8f934348628ed`
Audited base tree: `6e3f9e1780b55b8d788c3fdaf3e783d5be208936`
Reference branch: `reference/external/908-storage-incarnation`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration / effort level: not exposed
Requested by: Riverhog integration agent, conveyed by the maintainer in this conversation
Published by: the same producer, through the connected GitHub publication tools
Prepared: September 24, 2026 (America/Los_Angeles)

## Purpose and contents

An executable storage-incarnation reference, narrowly focused on restart-stable adapter
identity, durable catalog ownership, verified rebinding, and the invariant that reusing a
configured name or URL cannot reassign historical archive ownership.

- `storage-incarnation/model.py`: real Linux filesystem witness with explicit provisioning;
  SQLite-backed executable registration/ownership model; expected-incarnation read fence.
- `storage-incarnation/test_model.py`: 36 focused tests, including independent processes,
  competing provisioners, removal/re-addition, unchanged name/URL pointing to another backend,
  explicit replacement, late receipts, stale binding generations, and probe/effect races.
- `storage-incarnation/CONTRACT.md`: candidate mechanism, exact source anchors, external
  primary-source research, trust assumptions, integration seams, and explicit limitations.

All additions are under `.reference/`. No production module, schema baseline, configuration,
generated contract, workflow, release checkpoint, or tag is changed. This is a concrete
executable experiment, not a partially wired production implementation. SQLite is used only
for the experiment; production ownership remains with Riverhog's existing PostgreSQL catalog.

## Authority

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an authorized
repository agent does not itself establish or extend an accepted design, contract, release
requirement, or authorization to integrate these changes.

Authority remains with the owning issue, subsequent maintainer decisions, and the repository's
normal integration rail. In particular, the accepted #908 comment controls over proposals
in this reference. This is not issue completion and does not authorize the #900 configuration
cut or #909 permanent-loss/purge behavior.

The exact published reference commit recorded in #908 is the handoff identity; the branch
name is navigation only. Reconcile this material against then-current authoritative repository
state and current issue decisions; do not apply it mechanically or import `.reference` modules
into production. Full lifecycle integration and its acceptance remain with the integration agent.

## Validation actually performed

- Read #903, #908 and its accepted decision, #900, AGENTS.md, README.md, and the pinned source
  anchors listed in CONTRACT.md. Reviewed official AWS conditional-write/bucket-owner/consistency
  documentation and Python filesystem primitive documentation. This was a focused audit, not
  an exhaustive review of every storage-dependent workflow.
- Ran `python -W error::ResourceWarning -m unittest discover -s .reference/storage-incarnation
  -p 'test_*.py' -v`: **36 tests passed** in the local Linux execution environment.
- Ran `python -m py_compile` for both Python files.
- Tests use real temporary filesystem roots and an on-disk SQLite catalog. They launch fresh
  adapter/catalog Python processes and six concurrent provisioning processes. Test bytes are
  stand-ins for ciphertext; cryptography is not exercised.
- Runtime: Python 3.13.5; SQLite 3.46.1; Linux. Only the Python standard library is required.
- Publication uses GitHub Git-data objects over the exact audited base. The publisher compares
  returned blob hashes against the locally tested files before creating the reference commit.
  The actual GitHub commit SHA, not an invented pre-publication SHA, is recorded in #908.

## Not validated / known limitations

- No full repository checkout/dependency environment was available locally. `make lint`,
  `make unit`, `make dist-smoke`, `make build`, PostgreSQL concurrency, generated-contract,
  container, release, and provider qualification suites were not run. Local reference tests
  are not substitutes for those checks.
- At the audited base, the CI push filter includes only `main` and `release/v1`; no PR is
  opened and no workflow is changed or dispatched by this reference publication. The observed
  checks/runs for the final published SHA are recorded in the owning-issue handoff, not assumed.
- No native Development branch-to-issue linking action is exposed by the current publication
  interface. This limitation is explicitly recorded in #908; a capable publisher may establish
  the relationship later without changing the reference commit. A Markdown link is not claimed
  to be that native relationship.
- The witness assumes a trusted authenticated adapter and a correctly mounted, administratively
  controlled backend. Copying the marker can clone its logical identity; this is not malicious
  operator detection or hardware attestation. Actual power-loss and network-filesystem behavior
  were not tested.
- Only the filesystem read effect fence is implemented. Generic S3 marker checks do not prove
  an atomic cross-key effect fence, and AWS/Backblaze behavior was not exercised. See CONTRACT.md.
- The catalog model does not implement production receipt validation, object revisions/digests,
  uploads, writes, deletion/retirement, restore sessions, publication workflows, cache lifecycle,
  readiness/error mapping, leases, or in-flight administrative revocation. It defers late
  receipts for unbound owners instead of implementing their durable reconciliation workflow.
- No accepted API/configuration field inventory or migration path is established. Integration
  must apply identity to all persisted ownership/work, preserve independent archive recovery,
  and qualify each effect path before claiming the full #908 invariant.
