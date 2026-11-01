# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#984](https://github.com/nashspence/riverhog/issues/984)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/984-provider-write-quiescence`
Audited base: `codex/964-session-owner` checkpoint
`533ac4ee06def4692288c39c9279bf9387525a3e` (the source named by #984).

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: no additional identity attested
Requested by: Riverhog maintainer, for the integration agent
Published through: connected GitHub integration

## Start here

Read [QUIESCENCE_DESIGN.md](QUIESCENCE_DESIGN.md). It selects durable effect
admission before provider dispatch, closed admission plus drainage, late-seal
adjudication, and bounded exact terminal cleanup in existing owners.

**First useful slice:** the maintained upload service/MemoryResumableStore witness,
with an ARMED begin held before its effect. Cancel must stay pending and retain
its owner; let begin finish, record the exact session, reject the next write, then
abort/drain and finish once. Repeat with an armed complete: retain the late seal
and refuse cancel. See [QUALIFICATION.md](QUALIFICATION.md).

**Important selected boundary:** ambiguous remote calls remain pending. Empty
listings, elapsed time and process death are not remote execution-closure proofs.
The current S3 APIs do not establish a universal finite-time drain for a lost begin
response. This packet explicitly chooses safe retained uncertainty, not a fictional
provider fence. That liveness choice and the operational adapter/outcome cut need
issue adoption; publication is not approval on the maintainer's behalf.

[PROVIDER_PROFILES.md](PROVIDER_PROFILES.md) scopes S3/filesystem/HTTP/mirror behavior.
[RESEARCH.md](RESEARCH.md) pins sources and primary external documentation.
The two Python files are a limited SQLite model, not product implementation.
`python .reference/probe.py` runs its deterministic fault/race vectors.
[PROBE_RESULT.json](PROBE_RESULT.json) records 22 passing tests, including a real
child process exit after durable admission. [VALIDATION.md](VALIDATION.md) separates
that evidence from unperformed maintained-provider/backend qualification.

## Source relationship and authority

Initial #984 handoff; no previous #984 SHA is superseded. It supplies the terminal
interlock left to upload/provider ownership by #964's bounded registration packet
`239e2d8b9bc5120cb0d8d3b2d14930e5c8679614`; it does not reopen the planner design.
Additional source `03dfc3c433372e5a965d37bbc5faf336feb05f17` was inspected for drift,
not used as a product test source. Product paths/modes equal the audited base;
only `.reference/` material is added. No protected identity or product file is edited.

This is non-authoritative design input under #903. Authority remains with #984,
subsequent maintainer decisions and the normal integration rail. Reconcile current
source/decisions; do not merge this branch mechanically. The exact published SHA
recorded in #984 is the handoff identity; branch navigation is not native Development
linkage. No PR, auto-close, release or provider qualification is requested. Record
finite consumption and remaining qualification in #984 rather than maintaining a
parallel reference implementation.
