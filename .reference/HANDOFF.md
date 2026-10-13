# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#969](https://github.com/nashspence/riverhog/issues/969)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Audited base: `main@e43df5b5acce5d730c28b48b084e87445fbaac3b`
Audited tree: `9546274bcd5918db378d1d12311e7e9cf06aa309`
Reference branch: `reference/external/969-collection-annotation-history`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, in the conversation commissioning #969
Published by: the producer through the maintainer-connected GitHub interface

## Authority and first action

This branch is externally produced reference material for the owning issue.
Creation, testing, publication, native linkage, or a request by an authorized
repository agent does not itself establish or extend an accepted design,
contract, release requirement, or authorization to integrate these changes.
Authority remains with #969, subsequent maintainer decisions, and the repository's
normal integration rail. Earlier conversation is intent calibration only.

Use a separate authoritative worktree. Do not merge this branch into main.
Reconcile the exact reference against then-current source and issue decisions.
The exact GitHub reference commit recorded in #969's publication comment is the
handoff identity; the branch name is navigation. This publication reconstructs
exact local file contents through Git tree/commit APIs, rather than claiming to
publish a pre-existing producer commit. Verify the tree as recorded in that
comment. The publishing actions do not expose native Development-branch linkage;
a textual link is not represented as that relationship.

Read [INTEGRATION.md](INTEGRATION.md), then run the canonical runtime probe in a
properly locked environment before adopting a profile representation. The first
product slice is initial annotation establishment, one tag/description change,
one conditional selector publication, and database-free recovery of the exact
selected history. Escalate a concrete semantic conflict, not another broad study.

## Contents

- `annotation_reference.py`: small-fixture model of staged immutable evidence,
  one conditional selector, historical replay/retention, ancestry comparison and
  standalone member-context composition. It is deliberately not production code.
- `test_annotations.py`: 26 executable model challenges, including missing/tampered
  history, publication interruption, exact retries, divergent copies and loss of
  the entire original shared recovery tree after composition.
- `profile.schema.json`: narrow fixture-only context/activity profile examples.
- `verify_schema.py`: checks real unchanged core schemas and offers an explicit
  optional check using the real canonical runtime. It never substitutes a stub.
- `VALIDATION.md`: exact commands, observed scope and unperformed qualification.

## Validation and limits

The final model run passed 26 tests. Nine journal frames and eight profile values
passed schema validation with the five relevant installed schema-library versions
verified against `uv.lock`. Reference Python compilation and staged whitespace
checks passed. These checks do not establish full semantic or product conformance.

`verify_schema.py --full-core` was attempted and blocked at import by missing
`rfc8785`. The repository's five requested Make gates were attempted and could
not start without `mise`. The complete locked environment was unavailable;
Python was 3.13.5, not the repository's pinned 3.12.3. See VALIDATION for details.
No storage, encryption, PostgreSQL, process-crash, installed recovery, production
permission, source-retirement or growth qualification was performed.

The model deliberately uses whole snapshots/prefixes, in-memory dictionaries,
fixture-only canonical encoding and a toy membership proof. Those are not
proposed production algorithms, an archive format, or substitutes for existing
canonical serialization, bounded trees/segments and actual SourceBindingProof.
Its `/v0` discriminators and namespace are reference-local, not adopted contracts.
No original source, core schema, ontology, dependency lock or workflow is changed.
