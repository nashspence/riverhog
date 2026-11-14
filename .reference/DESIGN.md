# Declaration compilation and recorder ownership

## The observed cost boundary

The issue-authorized caller reaches 100 declarations and 142 delegate edges. The baseline traversed `_expression_unit` 68,785 times for each `project_bindings` and `validate_projection` call. A DAG can reach the same intrinsic child body repeatedly along different incoming paths. Repeating that structural work does not check another execution sample.

The maintained sample helper also calls `project_bindings` and then `validate_execution_samples` for each emission. Both compile the projection. The existing batch reader already compiles once for a list of roots and then checks every supplied root. These are distinct opportunities: avoid repeated body walks within a compilation, and amortize compilation across an explicitly owned group of samples.

The earlier experimental expression-key memo is not included. This proposal does not hash recursive expression objects or retain a compiled plan across public calls.

## One compilation owns checked bodies

`_projection_contexts` still parses the supplied projection afresh, validates its metadata budgets, creates fresh contexts, checks the complete delegate graph for cycles and maximum depth, and rejects duplicate public subjects before constructing the new set.

The set key is `(declaration_id, metric)`. A key is added only after that bound's own expression has returned its declared unit. That intrinsic result is stable within the fresh, validated context graph. Each incoming delegate edge still checks its multiplicity unit, exact child dimension mapping and argument units, selected metric, and relation strength before consulting the set. Argument expressions retain their existing `equal` requirement.

The top-level declaration loop skips only bodies that were already successfully checked through a delegate. Source attachment and `evaluate_bound` continue to use the original uncached behavior. A later public reader invocation parses and checks everything again, including a projection mutated since an earlier successful call.

This makes intrinsic body visits proportional to the source bodies and their expression occurrences instead of repeatedly expanding shared descendants. Parsing, graph traversal, each edge's obligations, and actual sample validation retain their costs. The change makes no claim that all reader work becomes constant or that numerical evaluation is cached.

## An explicit recorder scope owns pending work

`tests.support.growth_batch.SampleBatch` is test support, not a new public package API. A caller supplies a fixed tuple of source operations and a positive pending-root limit. The real test uses `(callers.ROOT, units.ROOT)` and `limit=4` for exactly three caller turns.

At entry the helper validates the source closure once and retains its immutable canonical projection bytes. It pins the exact operation, binding, declaration object, delegate tuple, and canonical declaration-row digest for every reachable source binding. Row digests avoid retaining a second copy of the large projection. Replacing even a semantically identical binding or substituting another callable with the same declaration ID is rejected.

Each completed caller or unit sample is admitted before it returns to its parent. Admission copies the observed dimension and selected metric mappings, preserves the nested observations, and seals the canonical sample bytes. This seal detects changes before receiving; it is never a successful-validation receipt. Both inputs must be mappings, so malformed iterable pairs are not silently converted into evidence.

On reaching the root limit, on explicit flush, or on context exit, the helper checks source identity, checks each pending sample seal, reconstructs a private projection mapping from the immutable bytes, and passes each operation's full list of roots to the existing `validate_execution_samples`. No root is omitted from its group. The public reader performs fresh projection validation and all primitive/nested sample checks. A second source check precedes successful batch accounting.

There is one compilation per operation group per flush, plus the initial source projection. There is no compiled-plan cache across flushes and no acceptance cache. Grouping different root declarations does not deduplicate repeated nested observations: a unit root is independently checked and is checked again inside its successful caller.

## Exceptions and mutation

The unit root has an independent pending obligation even if its outer caller raises before producing a root. Context exit flushes pending samples during success and exception unwinding. A valid pending batch preserves the original exception; invalid evidence raises the validation failure with the original exception retained by Python's exception chaining.

An admission or validation failure poisons the scope. Catching that exception inside the context cannot turn exit into success. The context is one-use and rejects work before entry or after close. Exit clears pending samples, source snapshots, root bindings, digests, and projection bytes.

The source checks detect bindings or metadata changed at a receiving boundary, including a final empty flush. They are not a concurrent mutation protocol or a hostile-code security boundary. Completed samples changed before their pending receipt are rejected. Already received samples carry no trusted marker; a later serialized consumer must run the normal standalone reader, which rejects later contradictory edits.

## Preserved checking and boundedness

| Boundary | Preserved owner / behavior |
| --- | --- |
| Malformed declaration shape and expression metadata | Fresh `_projection_contexts` parsing and existing 4,096-declaration / 131,072-expression-node / depth-64 limits |
| Whole graph cycles, depth, and duplicate subjects | Existing full graph checks before any body reuse |
| Distinct operations claiming one declaration ID | Existing `project_bindings` source ownership checks |
| Edge units, metric selection, relations, dimension mapping | Checked for every referring expression before body reuse |
| Sample digest and complete dimensions / metrics | Existing public execution reader, for every admitted root |
| Actual delegates, invocation count, mapped child dimensions | Existing recursive sample and composition checks, for every nested node |
| Per-root sample admission | Existing 65,536-node budget; a fresh budget for each root |
| Buffered observations | Explicit maximum pending root count; `4` in the actual caller test |
| Source and projection lifetime | One context; immutable bytes across flushes, fresh compilation within each public reader call |
| Independent serialized evidence | Existing source/digest/outcome binding and standalone reader; no trusted-cache escape hatch |

The queue bound is a root-count bound, not an RSS guarantee. A root can contain a large nested sample, and each source snapshot can use the reader's existing metadata admission. Canonicalization and source verification have real cost. This reference does not introduce an additional data-size policy or weaken a maintained reader budget.

## Narrow integration surface

Only final caller and unit emissions accept the optional batch. Stamp, framing, preparation, Native open/advance, asynchronous, and other nested helpers retain immediate validation. Default recorder construction keeps its immediate behavior. This deliberately preserves their existing error placement and completed-child obligations.

An automatic unit flush can occur inside the outer caller's `child()` scope, where the previous immediate unit check ran. It is not outside every enclosing observer. The final context-exit flush occurs after those recorder scopes. The real test verifies unchanged observed sample bytes through the existing immediate helper.

The reference's measurements isolate declaration compilation and replay validation. They do not attribute the earlier 470.50-second interrupted census to this function, qualify that run, or establish an end-to-end product or full-census speedup. The integration agent owns adoption and broader qualification.
