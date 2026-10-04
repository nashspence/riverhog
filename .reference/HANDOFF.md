# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: https://github.com/nashspence/riverhog/issues/953
Convention: https://github.com/nashspence/riverhog/issues/903
Reference branch: `reference/external/953-stove0-recipe-language`
Audited base: `main` @ `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`
Audited base tree: `079c49f5ea400d891d2ee5e6453a38afebfb51b9`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort level: not exposed
Requested by: Riverhog maintainer, in this conversation
Published by: the same producer through the connected GitHub publishing interface
Date: 2026-10-04 UTC

Purpose: a formal ground-up Stove0 recipe replacement design and generous integration-agent handoff for #953. The maintainer explicitly requested pre-v1 hard cuts with no backward compatibility or migration. The issue owns durable scope and acceptance; these files supply concrete design and checked authoring input.

Contents under `.reference/stove0-recipes/`: README, formal DESIGN, exact OBSERVATION_INTERFACES contract proposal, CONFORMANCE capability account and numbered integration/runtime vectors, standalone source-schema/static-lint prototype, ten worked YAML recipes, its tests and the dependency versions actually exercised.

This branch is externally produced reference material for the owning issue. Its creation, testing, publication, linkage or request by an authorized repository actor does not itself establish or extend an accepted design, contract, release requirement or authorization to integrate these changes. Authority remains with the owning issue, subsequent maintainer decisions and the repository's normal integration rail. Reconcile this material against then-current authoritative repository state and current issue decisions; do not apply it mechanically. No production implementation file is changed by this reference.

The exact published GitHub commit SHA recorded in #953 is the handoff identity. The branch name is navigation. Publication uses GitHub Git object/tree APIs over the exact audited base; no producer-local commit SHA is claimed as the published identity. Preserve published reference history; substantive revisions require a new exact reference commit and updated issue record.

## Validation actually performed

- Read root AGENTS.md and README plus relevant recipe/planner/protocol, configuration, authority and test sources at the audited base; read #903 and relevant configuration scope in #492.
- Obtained a full read-only clone at the pinned base. Verified archive SHA-256 `e9539c64915d9a0d5482b5f8d97aa924c66aa1564504d766fdb556f374b6b3f9`; verified checkout commit and tree against the values above.
- Consulted primary CWL, OMG BPMN, Open Workflow and W3C PROV sources. The design borrows terminology; it does not claim standards conformance.
- Checked the generated Draft 2020-12 source schema and ten example documents against mock resource summaries.
- Ran `pytest -q test_language.py`: 57 tests passed in the producer environment (Python 3.13.5; jsonschema 4.26.0; ruamel.yaml 0.18.17; pytest 9.0.2).
- Ran Python byte-compilation for the reference Python files. No audited repository implementation code was executed.
- Publication/tree identity verification and GitHub check availability are recorded separately in the owning issue's publication comment, after publication.

## Not validated / known limitations

- This is a design and authoring prototype, not the production replacement. No complete compiler, canonical identity encoder, exact contract/interface resolver, accepted-evidence runtime or workflow engine is implemented here.
- Fixture resources are mock summaries, not verified production contracts, descriptors, interface digests or conformance evidence. Runtime/adversarial vectors in CONFORMANCE.md are requirements and have NOT been executed.
- Repository lint/unit/distribution/build, installation/Compose, database/restart/scale and provider qualification were NOT run. Passing reference tests is not a substitute. Provider qualification remains separately authorized.
- The publication interface does not expose GitHub's native issue Development-branch relationship mutation. A Markdown link and issue-number branch name do not establish it. This limitation must also be recorded in #953; a capable publisher may add the native relationship later without altering the reference commit or provenance.
- The integration agent must reconcile later main/issue changes, produce executable contract authorities, re-express existing witnesses and regenerate the current pre-v1 baseline. No compatibility adapter or migration is requested.
