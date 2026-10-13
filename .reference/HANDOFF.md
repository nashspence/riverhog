# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#969](https://github.com/nashspence/riverhog/issues/969)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Original audited base: `main@e43df5b5acce5d730c28b48b084e87445fbaac3b`
Original audited tree: `9546274bcd5918db378d1d12311e7e9cf06aa309`
Fresh read-only audit: `main@9a485bc174b7b8d703ac05159e3df9be4f36be98`
Fresh audited tree: `8946a8cba0e978d774c97c791759dcc3a964cfca`
Preserved reference parent: `ce0d34fa8da9e423adcf681c30f518d5949042f8`
Reference branch: `reference/external/969-collection-annotation-history`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, in the conversation commissioning this update
Published by: the same producer through the maintainer-connected GitHub interface

## Start here: exact guarantees, not implementation preferences

Read the newest **Exact growth guarantees** comment in #969, then
[EXACT_GROWTH.md](EXACT_GROWTH.md), [validation](exact-growth/VALIDATION.md), and
the existing [P00-P04 owner map](INTEGRATION.md). The issue chooses the required
asymptotic classes now. Integration must implement and substantiate them; it may
not select weaker classes, defer their choice, or hide work in an unnamed delegate.
Earlier conversation is intent calibration, not another specification.

The exact refinement distinguishes byte/node/index work, authored-input storage,
initial construction, grant/copy/request multiplicities, distinct semantic
selections, output expansion and trusted-state restart. Native generated annotation
validation is included in its tight bound; arbitrary imported provenance retains
its separate qualified profiles. Core ontology, original journal identities,
current authorization and complete history remain unchanged.

## Contents and demonstrated scope

- `EXACT_GROWTH.md`: exact requirement explanation, dimensions, derivations,
  boundary qualifications and real-owner proof obligations.
- `exact-growth/basis.py`: normalized arithmetic bases for finite diagnostics,
  NOT numerical product limits or a second growth-declaration framework.
- `exact-growth/test_contract.py`: 20 arithmetic, native tag-slice, prefix and
  generated-profile-shape challenges.
- `exact-growth/run.py` / `results.json`: reproducible observations and source hashes.
- `exact-growth/VALIDATION.md`: actual checks, blocked gates and remaining proof.

The original model, schema, tests, earlier GROWTH.md and growth diagnostics are
unchanged. Their candidate-formula wording is historical where the newest issue
comment refines it. Both earlier reference commits remain in ancestry; this update
supersedes `ce0d34fa8da9e423adcf681c30f518d5949042f8` as the active handoff.

Twenty new diagnostics passed. The original 26 model tests were rerun and passed;
nine frames/eight profile values again passed schema validation with the checked
schema-library pins. Full canonical runtime validation was attempted and remains
blocked by missing `rfc8785`. All five required Make gates were attempted afresh
and could not start without `mise`. Python 3.13.5 is not the pinned complete project
environment. No serializer, dependency lock or policy was substituted to pass.

These are scoped reference results, not an integrated implementation, asymptotic
proof by finite sampling, full-module tag conformance, storage/crypto/PostgreSQL
proof, authorization validation or installed-recovery qualification. See validation
before attributing stronger meaning to a result.

## Authority, publication and reconciliation

Use a separate authoritative worktree. Do not merge this reference branch into
main. Reconcile the current issue decisions and then-current source; do not reset
another agent's work. This update changes reference material only and does not
merge/rebase fresh main into the older audited reference tree.

Creation, testing, publication, issue linkage or a request by an authorized agent
does not confer implementation, release or integration authority on this branch.
The owning issue and repository's normal integration rail control. Preserve the
producer/requester distinction; Git author/committer account data is publication
metadata, not a substitute for producer attribution.

The publisher reconstructs the supplied tree through Git object APIs and verifies
its blob/tree identities, sole parent and actual branch tip. The exact resulting
GitHub commit recorded in #969 is the handoff identity; no unpublished local commit
is claimed imported. Keep all earlier handoff SHAs in branch history.

Native GitHub Development linkage remains unavailable through the publisher's
exposed actions. The issue explicitly records this #903 limitation; textual links
are not described as native linkage. No closing PR or issue completion substitutes
for it. A capable publisher can add the relationship without changing these bytes.
