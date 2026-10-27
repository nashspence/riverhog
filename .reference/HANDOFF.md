# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#982](https://github.com/nashspence/riverhog/issues/982).
Convention: [#903](https://github.com/nashspence/riverhog/issues/903).
Request: [resource-admission follow-up](https://github.com/nashspence/riverhog/issues/982#issuecomment-6455046191).
Branch: `reference/external/982-sqlite-resource-admission`.
Audited base: `codex/964-session-owner` checkpoint
`f41bae6e1456d4ae16952de293320cbe4a6ce460`.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro.
Requested by: Riverhog maintainer, for the integration agent.
Published through: connected GitHub integration.
No additional configuration/effort identity is attested.

## Read this first

[RESOURCE_ADMISSION.md](RESOURCE_ADMISSION.md) selects the **enforced-envelope**
option requested by the agent. Keep the existing SQLite container and normal
Windows VFS; add a narrowly scoped native resource gate around that VFS, plus an
explicit application-owned aggregate SQLite heap ceiling. Do not infer a small
universal dirty-page bound from the four measured fixtures.

The design derives a fixed-extent control bound, separates resident dirty frames
from cumulative file writes, and specifies independent forward/recovery budgets,
allocated journal/database readiness and safe structural-budget rejection.
**Trade-off:** arbitrary structural cleanup is not promised to complete under
any small configured quota. A too-small quota preserves pending state and needs
an explicit larger grant; it never causes an unguarded retry or partial success.
Fixed control progress remains preallocated and independent of payload admission.
This change to the prior resource promise is explicit, not an implementation guess.

**First integration step:** on the exact current integration source, turn one
existing VFS write-failure witness into a pre-delegation quota witness. Refuse the
third distinct database-page write, allow the separately reserved rollback bank,
and prove exact original recovery. Cover automatic rollback *inside* the failing
SQLite call before extending the gate to initialization, all file methods and
normal Windows execution. No producer activation or broad backend rewrite first.

[QUALIFICATION.md](QUALIFICATION.md) supplies the finite boundary vectors and
source placement. [RESEARCH.md](RESEARCH.md) explains the primary-source basis.
[VALIDATION.md](VALIDATION.md) records actual reference-only evidence and limits.

## Revision and source identity

This supersedes only the unresolved resource-admission recipe in reference
`cd2a10e8972ed79a2641faf2bd68f1d4c2036000`. Its platform/custody decisions remain
context, already partially implemented; its old source tree is not rebased or
merged here. The previous branch and commit remain intact.

Product paths in this branch equal the audited base exactly. New reference
code is a mathematical gate and limited Linux experiment, not a product patch.
`93a3208056a98887682db0f5c16f6e868d5dc141` was additionally inspected: the only
source delta is privatizing two schema-helper names in two files. Experiments
use the pinned base's test VFS fixture, not a claim of testing that newer source.

The exact published reference SHA recorded in #982 is the handoff identity.
Publication/request/testing does not establish an accepted contract, completion,
permission to merge or release, or authority over #981/#964. Reconcile with current
issue decisions and integrate through the normal rail; do not merge this branch
mechanically. Record one finite adoption/reproduction/disposition result in #982.
No native Development association or reference-status registry is requested.
