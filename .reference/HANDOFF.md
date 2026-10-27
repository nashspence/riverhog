# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#982](https://github.com/nashspence/riverhog/issues/982)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/982-native-windows-durability`
Audited base: `codex/964-session-owner@8c94eb3e5e349147d46b8f10fc25e161cb7106be`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: no additional configuration/effort identity attested
Requested by: Riverhog maintainer, for the integration agent
Published through: connected GitHub integration

## Start here

Read [WINDOWS_BACKEND.md](WINDOWS_BACKEND.md). **Choose an application-owned,
per-construction SQLite storage adapter on native Windows/local NTFS, with
persistent rollback journaling and synchronous commits.** Keep the existing
POSIX adapter and shared producer protocol. This is a specific proposed platform
mechanism, not a recommendation to make production POSIX-only or a claim that a
Windows backend has been implemented.

Logical allocation/member namespaces, immutable generations and chunk rows replace
per-member physical mkdir/link/rename/unlink barriers on Windows. The initial
container is created as a file under an already-established local state directory;
no new physical directory chain is put on the publication critical path. Native
share-denied ownership, private ACLs and handle identity checks remain necessary.
SQLite is the byte store, not a parallel workflow registry or archive authority.

**First integration/verification step:** in a separate authoritative worktree,
implement the Windows container bootstrap/owner and one bounded transactional
byte publication. Run it as a non-elevated Windows process on NTFS, terminate
before/after the successful selection commit, reopen, and prove absence or the
same complete winner, live-owner exclusion, and no remote mutation before the
durable barrier. Include a genuinely fresh container, not only a warmed database.
[ACCEPTANCE.md](ACCEPTANCE.md) then expands this into maintained store/producer tests.

The scope includes the unavoidable adapter seams: explicit database initialization,
logical namespaces, inventory scratch, physical-space accounting and disposal.
It does not reopen the implemented receipt-phase ruling or authorize unrelated
Native authoring, source-memory, discovery, archive or release changes.

## Source and evidence

This is the initial #982 reference; it supersedes no earlier #982 SHA. All product
paths match the audited base exactly. Only `.reference/` material is added. The
base is the exact integration checkpoint identified by the owning issue, not a
claim that draft #979 is merged or fully qualified.

[RESEARCH.md](RESEARCH.md) separates primary-source facts from design deductions.
[VALIDATION.md](VALIDATION.md) records actual audit and reference checks. The small
[transaction probe](transaction_probe.py) is an executable SQL ordering experiment,
not the maintained backend, Native/authentication evidence, or a Windows durability
test. Its source imports no Riverhog implementation. No Windows execution,
power-cut qualification, or production performance result is claimed here.

## Authority

This externally produced branch is concrete input for #982. Its creation,
publication, validation or request does not establish or extend an accepted
contract, support promise, release requirement or authorization to integrate.
The owning issue and subsequent maintainer decisions control. Reconcile the
material with then-current authoritative source and integrate through the normal
rail; do not mechanically merge this reference branch.

The exact published SHA recorded in #982 is the durable handoff identity. The
branch URL is navigation; no native Development association is requested. Record
one finite adoption/reproduction/disposition result in #982 rather than maintaining
an alternate implementation. #981 retains its separate recovery acceptance.
