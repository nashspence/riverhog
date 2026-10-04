# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: https://github.com/nashspence/riverhog/issues/953
Convention: https://github.com/nashspence/riverhog/issues/903
Reference branch: `reference/external/953-stove0-recipe-language`
Audited authoritative base: `main` @ `802ed38225b4ecd9aacd1b7dc3de51a1b1fd1905`
Audited base tree: `079c49f5ea400d891d2ee5e6453a38afebfb51b9`
Previous published reference / parent of this revision: `16f7d56f54d7af377c792c9522fdd655699aa739`
Previous reference tree: `5a62e2b4b13ad121af59ac31f9971c9b173d6371`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort level: not exposed
Requested by: Riverhog maintainer, in this conversation
Published by: the same producer through the connected GitHub publishing interface
Date: 2026-10-04 UTC

## Purpose and revision

A formal ground-up Stove0 recipe replacement design and integration-agent handoff for #953. The maintainer requires a pre-v1 hard cut with no backward compatibility or migration. This revision adds a mandatory compiler-derived RecipeContract to the proposal. The latest conversation is explicitly **intent calibration rather than directly authoritative**; its examples were reviewed, not transcribed as accepted schema. The resulting supplement is a recommendation for issue-local reconciliation and acceptance.

RECIPE_CONTRACT.md refines identity, conditional outcomes/evaluation, nested no-output, transitive exposure and retirement: an equal contract does not authorize substitution; a potential effect is not a guarantee or permission; a source-loss decision code is not deletion evidence; child root-retirement policy is not inherited authority. Recipe ID/revision/full SHA stay outside the boundary hash.

The original nine-file handoff is preserved in the parent commit. This revision adds RECIPE_CONTRACT.md, contract_projection.py and test_contract_projection.py, and updates this handoff and the reference README. Existing DESIGN/OBSERVATION_INTERFACES/CONFORMANCE and authoring examples/tests remain implementation input. The supplement explicitly extends the compiled envelope and compiler phases and adds issue-local acceptance vectors. All changes remain under `.reference/`; no production implementation is changed.

The new exact published SHA recorded in #953 supersedes the earlier SHA as the current handoff. The earlier commit remains its ancestor and available for historical comparison. The branch name is navigation, not handoff identity. Publication uses GitHub tree/commit APIs followed by a non-force fast-forward; no producer-local commit SHA is claimed as published. Full-tree and parent verification after publication are recorded in the issue.

## Authority

This is externally produced reference material. Its creation, testing, publication, linkage or request by an authorized repository actor does not itself establish or extend an accepted design, external contract, release requirement or authorization to integrate. Producer and requester authority are distinct. The owning issue, subsequent maintainer decisions and the normal integration rail control. Preserve the issue's original body as historical scope; record this later refinement and exact reference SHA in a comment. Reconcile against current authoritative main and issue decisions, not the conversational sketch or branch alone.

## Validation actually performed for this revision

- Re-read #953 and its existing publication comment, #903, root AGENTS.md/README, reference design and relevant invocation/output/retirement definitions. Confirmed main and the reference branch at the exact SHAs above before editing.
- Obtained the full read-only reference clone. Verified ZIP SHA-256 `35601595b578946f817c14dcec5c3332288965a7df5706c623e5d295a42cc63b` and manifest SHA-256 `dfac9a790ee1e00c7f9604b5ba26f2f97444b9043ccb7ffb2b29cbf63094e27c`, then checked the checkout's commit/tree.
- Consulted primary CWL Workflow/subworkflow, RFC 8785 and JSON Schema sources. This is terminology/semantics grounding, not standards-conformance certification.
- Generated and checked the reference payload schema; exercised the ten existing authoring examples and their mock, test-adapted boundary slices.
- Ran `pytest -q test_contract_projection.py test_language.py`: **127 passed**, including **70 new** projection tests and **57 existing** authoring tests.
- Ran `python -m py_compile contract_projection.py test_contract_projection.py language.py test_language.py`: passed.
- Ran `python language.py check examples.yaml`: all ten passed. Generated the unsealed payload schema with `python contract_projection.py schema`.
- Environment: Python 3.13.5; jsonschema 4.26.0; ruamel.yaml 0.18.17; pytest 9.0.2. No audited production implementation code was executed.
- Publication verification and Actions/check availability are recorded in #953 after publication, not asserted here in advance.

## Not validated / known limitations

- New code is an unsealed structural boundary projection over mock normalized slices/dependencies, NOT the production source compiler, exact resolver, authenticated contract validator, canonical identity codec, applicability prover or workflow engine. Structural equality tests are not JCS hash conformance.
- The test-only source-to-slice adapter is not another proposed supported representation. Existing mock export/retirement flags are not sufficient production contracts or authority. Complete parent/child dependency derivation, schema entailment, output validation, dynamic plan obligations and retirement proofs remain integration work.
- Runtime/adversarial requirements in CONFORMANCE.md and RC-01 through RC-11 are NOT claimed as executed runtime tests. Repository lint/unit/distribution/build, installation/Compose, real state/restart/scale/retirement and provider qualification were NOT run. Provider qualification remains separately authorized.
- The available publishing tools do not expose GitHub's native issue Development-branch relationship mutation. The relationship was absent from the previous handoff and is not established by a Markdown link or issue-number branch name. Record the limitation in #953; a capable publisher may add it without altering the reference SHA or provenance.
- The integration agent must reconcile later main/issue changes, implement one authority, re-express behavioral witnesses, regenerate current pre-v1 contracts/configuration/client references, and validate the actual integration SHA through the normal rail. No compatibility shim or migration is requested.
