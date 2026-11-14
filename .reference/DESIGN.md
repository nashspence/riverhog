# Numeric evaluation: reuse declaration metadata, retain live observations

## Required work and avoidable reconstruction

Each numeric `_evaluate` call checks the supplied dimension names and every supplied value, resolves the requested metric, rejects asymptotic bounds as numeric budgets, and evaluates the bound expression. Recursive delegate evaluation supplies a newly computed argument mapping and performs these checks independently. `_validate_sample` evaluates every declared bound; execution validation also receives every actual named delegate and checks composition.

The integration agent's reader-only profile identifies repeated construction of the declaration's expected dimension-name set and metric-to-bound dictionary inside `_evaluate`. Those collections contain metadata already owned by the fresh parsed context. Reconstructing them for each numeric call contributes no additional validation of the caller's observations.

The live observations have a different lifetime. The public APIs accept `Mapping` objects. A metric lookup can change a dimension between successive bounds, and a source dimension iterator can mutate source declaration data before the original right-hand metadata lookup. Checking a mutable mapping once and carrying that acceptance across bounds would change existing public behavior. This proposal leaves every live dimension key/value check in place, in its original order. It does not claim that every remaining scan is inherently unavoidable under every possible future ownership model.

## Narrow implementation

Two optional references belong to `_Context`:

- `numeric_dimension_names` is a fresh `frozenset` only when every parsed declaration dimension name has exact type `str`.
- `numeric_bounds_by_metric` aliases that context's existing `bounds_by_metric` dictionary only when every metric name has exact type `str`.

The eligibility checks inspect the declaration's dimension and metric names once when its parsed context is built. They do not walk expression trees, alter parsing, classify whole projections, or validate a sample early. Non-exact string metadata retains the existing reconstruction path, preserving callback-bearing hash/equality behavior. Source contexts leave both references as `None`.

`_evaluate` keeps its original order: construct the supplied dimension-name set, select the expected declaration names, compare names, inspect actual values, resolve the bound, check its relation, and evaluate its expression. The expected names stay in the set family, preserving the comparison direction for custom observation keys. A `None` reference executes the original set/dictionary comprehension at its original point. The bound index is selected after actual dimension validation, so source mutation during that validation remains observable.

No checked unit, accepted sample, numeric result, expression result, public mapping, or source binding is cached. There is no global context. Every independent public reader call still constructs and validates its own parsed contexts.

## Work preserved

Expression parsing and typed construction, unit derivation, relation strength, ownership, graph cycles/depth, unused metadata, declaration digests, per-root resource budgets, actual values, numeric domains, bound order, and actual delegate composition retain their existing algorithms. A zero delegate multiplicity still evaluates the child expression, so a zero count cannot hide an invalid logarithm or other numeric failure.

The proposed structural changes are confined to `_Context`, parsed context construction, and `_evaluate`. Public signatures/exports, `_source_context`, `_validate_sample`, `_evaluate_expression`, public reader wrappers, canonical preparation, and the existing limits remain unchanged. Late invalid roots, descendants, unused dimensions/metrics, shared-child argument changes, and mutations between independent calls remain rejecting cases.

## Memory and lifetime

Each eligible parsed context retains one additional frozen set of dimension names. Its bound reference aliases an existing dictionary and copies no bound entries. Both parsed and source contexts gain two optional reference slots; source contexts retain no new indexes. The added retained entries are proportional to the dimensions of each live parsed declaration. Normal local/closure references and garbage collection determine the context lifetime; there is no cross-call registry.

This exchanges repeated transient declaration collections for bounded metadata associated with each independently parsed context. Construction counts and cumulative entries are work accounting, not allocator measurements, bytes allocated, peak memory, or RSS. A live-observation scan remains separate from a declaration-metadata construction.

Eligible names are indexed even when a parsed context receives no numeric evaluation. Pure projection validation therefore gains some metadata construction work. That work is included in the complete retained execution-reader comparison, but no separate projection-only or source-bound timing experiment is claimed. Existing admissions and limits remain unchanged; there is no new aggregate metadata cap.

## Evidence boundary

The experiment uses the exact accepted public reader as baseline and the proposed reader as separate modules on one clean proposal checkout. The retained workload has 46 ordered groups from the prior helper-replay receipt and samples from the prior actual three-turn capture: 122 independent roots and 1,078 total sample nodes across roots and descendants.

Each reader receives one separately labeled diagnostic pass and one unmodified elapsed observation of the complete public replay. Fresh parsing, numeric checks, canonical digests, and composition stay inside those public calls. Input preparation, behavioral equivalence, diagnostic copying, and report writing remain outside the unmodified timer. Instrumented timings do not replace unmodified observations or support additive CPU attribution.

Adversarial comparisons cover both serialized JSON cases and fresh public source/Mapping callback fixtures. Original inputs, order, exact outcomes, source identities, exits, and cleanup are retained. No journal census, publication fixture, additional actual caller turn, full unit-suite rerun, or large workload belongs to this reference.
