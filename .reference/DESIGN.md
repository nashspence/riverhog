# Fresh lookup maps for parsed declaration contexts

## The work being reduced

Unit derivation recursively checks expression trees. Each visit currently rebuilds the current declaration's dimension-name-to-unit map. Each delegate visit also rebuilds the child dimension map and child metric-to-bound map. Those constructions supply lookup data; they do not establish unit or relation acceptance.

The retained caller/unit projection has 100 declarations, 2,867 dimension entries, and 3,383 bound entries. The integration agent's accounting identifies repeated map construction as a concrete allocation-work candidate. This reference verifies that input against the exact source and compares complete public reader calls, retaining the original checks.

## Narrow implementation

`_Context` gains two optional private-context fields: `dimension_units` and `bounds_by_metric`. Both default to `None`. `_projection_contexts` populates them only after the existing fresh typed `Declaration` constructor has accepted that row. Each map contains the same entries as the previous comprehension; the metric map points to that context's existing frozen `Bound` objects.

At the three old lookup sites, `_expression_unit` selects the context's map when present and otherwise executes the old comprehension. The fallback checks `is None`, rather than treating an empty mapping as absent. No other lookup site or validation algorithm changes.

The source constructor `_source_context` still uses its original two-argument `_Context` construction. Source-bound declarations remain caller-visible objects, so `declare_growth` and `evaluate_bound` retain their reconstruction behavior, including fresh behavior after a forced source mutation between independent calls. The optimization is restricted to contexts that own newly parsed declaration objects.

## Checks that retain their existing work

The parser still reads every declaration, bound, and expression afresh, including unused metadata. Existing grammar, duplicate-name, ownership, format, resource-budget, graph-cycle, and graph-depth checks are unchanged. Every independent public projection/sample-reader invocation constructs its own contexts and lookup maps.

Unit traversal still visits every expression. Every delegate edge still checks its named owner, call-count unit, stack cycle, complete argument keys, each argument's units under the equal relation, requested metric, relation strength, recursive child expression, and declared result unit. A successful check at one edge supplies no acceptance for another edge.

The following remain byte-for-byte unchanged: `_source_context`, `declare_growth`, `evaluate_bound`, `_read_expression`, `validate_projection`, `validate_sample`, `_validate_sample`, `validate_execution_samples`, `_evaluate`, and `_evaluate_expression`. Only the private context definition, its parsed construction, and three map-selection sites change. The public wrapper functions continue to invoke fresh parsing normally.

Actual dimension/metric keys and values, declaration digests, delegate invocation counts, actual argument mappings, per-root sample budgets, and every nested sample remain checked through their original paths. Expression-specific argument maps are rebuilt as before. Neither units nor numeric acceptance results are memoized.

## Lifetime and retained-space tradeoff

The lookup maps belong to one parsed context. They are not placed on a source `Binding`, a source `Declaration`, a public API, or a global registry, and cannot be reused by a subsequent independent reader call. Delegate linking retains its existing graph and does not supply map contents.

The change exchanges repeated short-lived lookup dictionaries for two maps retained alongside each parsed declaration during that reader's context lifetime. Additional retained entries are proportional to the parsed dimension and bound entries. Existing metadata admissions remain unchanged; this is not a new total cap or a claim that resident memory decreases. The profile reports map constructions and entry counts separately from wall/CPU observations. It is not an allocator, byte-allocation, or RSS profile.

## Evidence boundary

The baseline metadata audit executes one public projection construction on exact clean source, compares every canonical byte with the retained projection, and verifies the retained sample declaration digests. It invokes no product operation or physical fixture.

The paired comparison uses the exact frozen baseline reader and proposed reader on one clean proposal checkout. Its two workloads are a complete 100-row projection validation and the 46 ordered receiving groups retained from the prior actual three-turn capture, comprising 122 independent roots and 1,078 nested nodes. Each call starts with a fresh projection parse. Allocation/visit instrumentation and unmodified elapsed observations are separate passes. The proposed change must preserve all expression visits, inventories, sample bytes, validation outcomes, and independent obligations.

The adversarial comparison checks exact outcomes against the frozen baseline reader, while maintained tests enforce the relevant checks directly. No new actual caller turn, full census, physical publication campaign, large trial, or product-throughput claim is part of this reference. The separate compose completion-window correction and PostgreSQL qualification remain with the integration agent.
