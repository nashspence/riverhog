# Run Acceptance Tests

The executable acceptance contract lives in the Gherkin feature files under
`tests/acceptance/features`.

## Preferred commands

Run the acceptance contract directly with pytest:

```bash
pytest tests/acceptance
```

The ISO scenarios invoke `xorriso`. If you want the full suite directly on the host, make sure `xorriso` is
installed first.

Run the same acceptance contract inside the deterministic test container:

```bash
./test acceptance
```

## What lives where

- `tests/acceptance/features/` contains the normative external scenarios.
- `tests/acceptance/test_bdd_acceptance.py` loads those features through `pytest-bdd`.
- `tests/acceptance/conftest.py` provides the shared step definitions.
- `contracts/disc/` holds the machine-readable ISO layout and YAML schema contracts that the acceptance scenarios verify directly.
- `tests/integration/` is reserved for non-contract regressions when the suite needs them.
