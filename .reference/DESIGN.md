# Bounded recorder ownership of sample validation

## The observed cost boundary

The issue-authorized caller reaches 100 declarations and 142 delegate edges. The baseline traversed `_expression_unit` 68,785 times for each `project_bindings` and `validate_projection` call. The current delivered source leaves those compilation semantics unchanged.

The maintained sample helper calls `project_bindings` and then `validate_execution_samples` for each emission. Both compile the projection. The existing batch reader already compiles once for a list of roots and then checks every supplied root. The selected approach amortizes that compilation across an explicitly owned group of samples.

No expression memo or compiled-plan cache is included. An initial compilation-local body-set experiment is preserved at `7c78eb33fd310653468c1a2fa9dfac41417c5adf`; its counts fell but sample-validation timings were slower. [RESULTS.md](RESULTS.md) records the mixed observation and the decision to exclude it. The final growth-contract package is identical to the audited base.

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
| Whole graph cycles, depth, and duplicate subjects | Unchanged full graph checks in every reader call |
| Distinct operations claiming one declaration ID | Existing `project_bindings` source ownership checks |
| Edge units, metric selection, relations, dimension mapping | Unchanged checking of every referring expression |
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
