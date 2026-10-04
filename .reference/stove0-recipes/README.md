# Stove0 recipe replacement reference

Owning issue: [#953](https://github.com/nashspence/riverhog/issues/953). Convention: [#903](https://github.com/nashspence/riverhog/issues/903). Read `../HANDOFF.md` first. This is external implementation input, not an integration branch or independently accepted contract.

## Reading order and current supplement

Start with DESIGN.md for the source/compiled language and lifecycle rules. Then read **RECIPE_CONTRACT.md**, which adds the mandatory compiler-derived public invocation/result boundary to every compiled recipe. It supplements the conceptual envelope in DESIGN.md section 4 and the compiler phases in section 11, and qualifies any shorthand about collection capability, required evaluation context or retirement permission. The preceding conversation was intent calibration, not an authoritative field-by-field specification.

OBSERVATION_INTERFACES.md specifies exact observer ports/views/evidence adapters. CONFORMANCE.md maps existing capabilities and runtime witnesses; the RC-01 through RC-11 requirements in RECIPE_CONTRACT.md extend that integration account. Nothing in this supplement relaxes exact identity, evidence, selection, settlement, source-loss or retirement authority.

Two semantic representations remain: authored source and compiled program. The contract is a verified compiler projection, not a third authoring language, independently supplied manifest or compatibility/substitution permission. Keep recipe IDs/revisions and the full implementation SHA outside the separately hashed boundary. Contracts describe normal completion, collection exports and local no-output alternatives separately, with conservative transitive context/effect exposure.

## Executable reference material

`language.py` generates the source schema and lints selected static invariants. `examples.yaml` contains ten worked recipes. Its resource summaries, including the old prototype's `exports_collection`/retirement flags, are mocks only, not production authorities; the replacement must derive child capability from verified contracts and retain real settlement checks.

`contract_projection.py` generates a closed **unsealed** contract-payload schema and projects normalized boundary slices with mock dependencies. `test_contract_projection.py` includes a test-only adapter for all ten existing examples. It is not a production compiler, signature sealer, dependency resolver, applicability prover or authority validator. `test_language.py` continues to exercise source authoring.

```bash
python -m pip install -r requirements.txt
python language.py schema > /tmp/stove0-recipe-source.schema.json
python language.py check examples.yaml
python contract_projection.py schema > /tmp/stove0-recipe-contract-payload.schema.json
pytest -q test_language.py test_contract_projection.py
python -m py_compile language.py test_language.py contract_projection.py test_contract_projection.py
```

Current producer checks: all ten source examples passed; their ten test-adapted boundary slices passed; **127 tests passed (57 existing authoring tests and 70 new projection tests)**; byte-compilation passed. Python 3.13.5 and dependency versions are recorded in requirements.txt. No audited production implementation code was executed. JCS identity vectors and production compiler/evidence/lifecycle/restart/scale/retirement qualification were not run; requirements are not test results.

This revision extends historical reference commit `16f7d56f54d7af377c792c9522fdd655699aa739`. The new exact SHA recorded in #953 is the current handoff identity; preserve the ancestor. Reconcile against the then-current issue and main, implement through the normal rail, and regenerate public references from executable authorities. No migration, old reader, alias layer, duplicate signature authority or dual runtime is requested. Do not land these hand-maintained reference inventories on main as a competing source of truth.
