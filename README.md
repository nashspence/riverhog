# riverhog

## Linting

Run the canonical lint flow with `./test lint`.

That lane runs `ruff check .` and then checks strict `mypy` output against the
checked-in regression baseline.

## Testing

Run the full suite with `./test`.
The canonical `./test` flow runs the lint lane first, then the unit, spec, and
prod-backed acceptance lanes.

Run the production-backed harness against the executable acceptance contract with `./test prod`.
Profile the production-backed harness with `./test prod-profile`.
Run the fixture-backed spec harness lane with `./test spec`.
Run the unit lane with `./test unit`.
Run the non-production lanes together with `./test fast`.
The `.feature` files under `tests/acceptance/features` are the source of truth for those scenarios.
