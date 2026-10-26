# NON-AUTHORITATIVE EXTERNAL REFERENCE

- Owning issue: [#981](https://github.com/nashspence/riverhog/issues/981)
- Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
- Branch: `reference/external/981-prepared-primary-recovery`
- Audited base: `codex/964-session-owner` @ `f0a7c2e263bb96533d71c9a3b4d43544b77d7b85`

- Produced by: OpenAI ChatGPT
- Model: GPT-6 Astra Pro
- Configuration: Default collaboration mode; parallel source audits and design review
- Requested by: Riverhog maintainer, for the integration agent
- Published by: OpenAI ChatGPT through the connected GitHub integration

## Start here

Read [RECOVERY_DESIGN.md](RECOVERY_DESIGN.md). It supplies a focused proposed
contract for durable prepared member-primary journals: one shared producer
owner, application-owned storage, exact replay, and receipt-gated reclamation.
It does not implement the repair.

**First integration step:** in a separate authoritative worktree, use
`tests.unit.test_incremental_collection_producer._bounded_service_api` to retain
one prepared primary, interrupt after successful journal staging and before
binding, reopen the producer and its store, and prove one identical staged
journal, one selected primary, and exact-primary custody before cleanup.
The first row of [ACCEPTANCE.md](ACCEPTANCE.md) specifies the complete witness.

The design chooses a small file-backed store, not a second database or workflow
registry. Point operations consume existing application member identities.
FTP uses its durable claim; CLI uses its upload allocation; derived writers use
the target execution state root. Load retained intent before invoking an
observer. Retain compact input/primary commitments after reclaiming large bytes.
Preserve the server's existing exact custody receipts through finalization for
indexed readback under current upload permissions. Settle materialization
acknowledgements before completion; custody and decision acceptance are distinct.

## Source and evidence

This is the initial handoff for this branch; no earlier #981 handoff is superseded.
Only four `.reference/` Markdown files are added. Product paths match the audited
base exactly. No historical patch or prototype implementation is included.

The base is the exact checkpoint named by #981 and the current draft #979 head;
[#964's checkpoint](https://github.com/nashspence/riverhog/issues/964#issuecomment-6397844323)
identifies that integration source. This is not a claim that it has merged.
`main@209481c189f7387557a85e1c249184642816e103` was also inspected for relevant
source differences; it was not used for experiments or merged into this branch.

[VALIDATION.md](VALIDATION.md) records source audit, integrity checks, environment
limits, and the distinction between existing issue evidence and checks performed
for this handoff. No production recovery or growth qualification is claimed.
[ACCEPTANCE.md](ACCEPTANCE.md) is the focused integration proof plan, not a new
repository gate or a claim that those tests passed.

## Authority

This branch is externally produced reference material for #981. Its creation,
validation, publication, or request by an authorized agent does not establish or
extend an accepted design, contract, release requirement, or permission to
integrate changes. Authority remains with #981, subsequent maintainer decisions,
and the repository's normal integration rail. Reconcile against then-current
authoritative source and issue decisions; do not merge this reference mechanically.

The exact published commit recorded in #981 is the durable handoff identity.
The branch URL is navigation. No native GitHub Development association is
requested or expected. Record finite consumption/disposition in #981 rather than
maintaining this reference as an alternate implementation.
