# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #515
Convention: #903
Audited base: main @ 99179cbb94a96d8ea2e633ab60c9e34be1763527

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: No additional mode or effort configuration is exposed.
Requested by: Maintainer nashspence, for the Riverhog integration agent.
Published by: OpenAI ChatGPT through the connected GitHub interface.

## Preparation checkpoint

This initial checkpoint establishes an exact source snapshot for the external
producer's isolated workspace. The reference implementation and final validation
record are not yet supplied at this checkpoint. No implementation or CI success
is claimed by this commit.

The branch-local workflow exports its exact tracked tree, without credentials,
for inspection and testing. It neither changes protected branches nor qualifies
a release candidate. Its source artifact expires after one day.

## Authority

This branch is externally produced reference material for #515 under #903.
Its creation, testing, publication, or request by an authorized repository agent
does not itself establish or extend an accepted design, contract, release
requirement, or authorization to integrate these changes. Authority remains with
the owning issue, subsequent maintainer decisions, and the repository's normal
integration rail. In particular, the audited AGENTS.md keeps release/v1 pinned
until an explicitly selected freeze candidate; this reference does not move it.

The exact final reference SHA recorded in the owning issue is the handoff
identity. The branch name is navigation only. Reconcile against then-current
repository state and owning-issue decisions; do not apply mechanically.

## Validation at this checkpoint

Performed: read #903, #515 and its latest recorded failures, audited main identity,
AGENTS.md, relevant CI, listener runtime, listener tests, filesystem promotion,
and staged installation qualification source.

Not performed: locked repository gates, native qualification, image builds,
CodeQL, managed-consumer qualification, or release acceptance.

Publication limitation: the available GitHub interface has no native issue-to-
branch Development mutation. The final owning-issue handoff will record that
limitation explicitly unless a capable publisher establishes the relationship.
