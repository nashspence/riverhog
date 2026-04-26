# riverhog

## Testing

Run the full suite with `./test`.

Run the executable acceptance contract with `./test acceptance`.
Profile the production-backed acceptance lane with `./test acceptance-profile`.
Run the fixture-backed spec harness lane with `./test spec`.
Run the unit lane with `./test unit`.
Run the non-production lanes together with `./test fast`.
The `.feature` files under `tests/acceptance/features` are the source of truth for those scenarios.
