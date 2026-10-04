# Stove0 recipe replacement reference

Owning issue: [#953](https://github.com/nashspence/riverhog/issues/953). Convention: [#903](https://github.com/nashspence/riverhog/issues/903). Read `../HANDOFF.md` first. This branch is implementation input, not an integration branch or accepted alternate contract.

Start with DESIGN.md for the complete proposed language, semantics, canonical compilation boundary and lifecycle rules. OBSERVATION_INTERFACES.md specifies the companion exact port/view/evidence contract that removes representation plumbing from recipes. CONFORMANCE.md maps current capabilities to the replacement, gives numbered runtime/adversarial requirements, and identifies integration owners and existing behavioral witnesses.

`language.py` generates the closed authoring schema and lints selected static invariants. `examples.yaml` contains ten separate worked recipes: simple transform, staged media/sidecars, repeated observer use, overlapping fork with exact-subset join, nested exports, decision-only approved loss, nested no-output, external delivery, Review0 bindings/output policy, and explicit no-output before an operation. The resources are **mock interface summaries**, not deployable exact contracts or fake qualified hashes. `test_language.py` checks authoring behavior only.

```bash
python -m pip install -r requirements.txt
python language.py schema > /tmp/stove0-recipe-source.schema.json
python language.py check examples.yaml
pytest -q test_language.py
python -m py_compile language.py test_language.py
```

The producer ran the checks with Python 3.13.5 and the versions in requirements.txt: ten examples passed and 57 tests passed. No repository implementation code was executed. These files do not include a production compiler, JCS identity implementation, real interface catalog, evidence validator or workflow engine. See the explicit unrun integration vectors rather than assuming grammar validation proves runtime safety.

The design is a pre-v1 replacement. There is no migration, old-schema reader, alias layer or dual runtime to retain. Reconcile against the then-current owning issue and authoritative main. Implement accepted work through the normal integration rail, regenerate public references from executable authorities, and do not land these hand-maintained design/reference inventories on main as a competing contract authority.
