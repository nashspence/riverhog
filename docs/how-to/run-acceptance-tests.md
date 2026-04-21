# Run Acceptance Tests

The executable acceptance contract lives in the Gherkin feature files under
`tests/acceptance/features`.

## Preferred commands

Run the acceptance contract directly with pytest:

```bash
pytest tests/acceptance
```

Run the same acceptance contract inside the deterministic test container:

```bash
./test acceptance
```

## What lives where

- `tests/acceptance/features/` contains the normative external scenarios.
- `tests/acceptance/test_bdd_acceptance.py` loads those features through `pytest-bdd`.
- `tests/acceptance/conftest.py` provides the shared step definitions.
- `tests/integration/` keeps deeper regression checks that are useful but are not currently part of the published acceptance contract.

If a regression test in `tests/integration/` becomes part of the public product contract, add or update the matching `.feature` scenario first and then move the executable coverage into the acceptance layer.
