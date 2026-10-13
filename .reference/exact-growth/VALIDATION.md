# Exact-growth refinement: validation record

Date: 2026-10-13. Reference only, under #969/#903.
Preserved parent: `ce0d34fa8da9e423adcf681c30f518d5949042f8`.
Read-only main audit: `9a485bc174b7b8d703ac05159e3df9be4f36be98`.
Original reference source remains `e43df5b5acce5d730c28b48b084e87445fbaac3b`.
No main/release source or dependency pin was modified.

## Final delivered diagnostics

```sh
python .reference/exact-growth/run.py --output .reference/exact-growth/results.json
python -m py_compile .reference/exact-growth/basis.py \
  .reference/exact-growth/test_contract.py .reference/exact-growth/run.py
```

All **20 diagnostics passed**, with no failures, errors or skips. The final
unittest run recorded 0.277296 seconds. Compilation passed.
`results.json` pins the exact tested scripts, source slice and analytical document.
This runtime is an observation, not a target or a growth proof.

The arithmetic suite checks independent byte/logical/index dimensions, exact
integer logarithms, attempt versus committed-change accounting, grant/copy
multiplicity, retained-space additivity, exact semantic selection counts, and
separate byte/record restart budgets. Its normalized coefficients are **not
numerical public bounds**. It neither replaces the integrated growth-declaration
model nor automatically classifies code as conforming.

The primitive suite executes the existing SHA-pinned native tag AST slice,
compares every streamed prefix digest against an independent recomputation, and
checks the generated annotation example's shape. A one-byte edit beside an
existing 65,536-byte tag produces 65,712 node-put octets on insert and 65,583 on
removal; only 129 new unique node octets were allocated in that insertion example.
This distinguishes computational byte visits from newly retained physical bytes.
The old tag root remains exact. For 128 64-byte chunks, streamed prefix hashing
processes 8,192 input octets versus 528,384 for repeated prefixes; every anchor
matches. The output-duplication test uses a counting sink, not filesystem output.

These observations illustrate why operand length alone is insufficient, why
stored-byte deduplication is not zero computational cost, and why independent
selection/output dimensions are needed. They are not full tag-module, backend,
archive or semantic-validator qualification.

## Additional checks invoked in this update

| Command | Result | Wall seconds |
|---|---|---|
| `python .reference/test_annotations.py` | 26 model tests passed | 0.818 |
| `python .reference/verify_schema.py` | 9 core-schema frames / 8 profile values passed | 1.527 |
| `python .reference/verify_schema.py --full-core` | blocked at import: missing rfc8785 (exit 1) | 1.655 |
| `make lint` | could not start: mise unavailable (exit 2) | 0.004 |
| `make unit` | could not start: mise unavailable (exit 2) | 0.003 |
| `make dist-smoke` | could not start: mise unavailable (exit 2) | 0.003 |
| `make build` | could not start: mise unavailable (exit 2) | 0.003 |
| `make linux-qualification` | could not start: mise unavailable (exit 2) | 0.002 |

The schema probe checked installed versions of jsonschema, referencing, rpds-py,
attrs and jsonschema-specifications against the repository lock before passing.
Schema success is not runtime/corpus success. The full-core attempt still fails
at the real canonical serializer import; no stub or replacement was introduced.
The Make attempts did not execute their required work. Python is 3.13.5, not the
repository's pinned complete environment. Existing 32 growth diagnostics were
not rerun in full by this update; their unchanged historical record remains at
the preceding reference. The new native tests reuse its pinned source loader.

## Proof not performed

Complete pinned lint/type/product tests, actual service annotation implementation,
PostgreSQL/physical-index profile proof, cryptographic storage, real authorization,
copy publication/repair, power-loss-safe restart, actual source membership proofs,
installed recovery, downstream source retirement and full cost-delegate closure
remain integration obligations. No finite sample proves an asymptotic class.
The exact classes are requirements to demonstrate, not claims these checks have
qualified Riverhog. No new crypto primitive or journal identity is implemented.

Publication will verify all changed blob identities and the full tree against
local Git, the exact sole parent and branch tip, and the exact-SHA Actions/status
results. An absent check is not green CI. Changes remain under `.reference/` and
the update advances the existing branch without force. Native Development linkage
remains unavailable through the exposed publisher actions and is recorded under
#903. The actual published reference SHA and evidence are recorded in #969.
