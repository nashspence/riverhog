# Extending the observer's bounded lifetime

## Four distinct costs

| Cost | Existing work | Selected treatment |
| --- | --- | --- |
| Declaration compilation | `project_bindings` projects and validates metadata; each public sample-reader call also parses/checks the projection | Admit one source union per explicit owner and group samples through the existing reader; retain fresh compilation per receiving group |
| Source/digest verification | Binding identity, declaration projection/canonicalization, declaration digests, sample seals, source rechecks | Reuse immutable admitted source bytes/digests; keep identity checks and full source-content checks at receiving boundaries |
| Sample-bound evaluation | `_validate_sample` checks digests, dimensions, metrics, bounds, nested delegates, counts, and mappings | Preserve every existing independent root occurrence and every nested check; never cache numeric acceptance |
| Primitive instrumentation | Actual SQL/row/byte/hash/JSON/resource/kernel observations, plus observer setup and teardown | Keep instrumentation and measured operations unchanged; keep observer sealing/validation outside active primitive accounting |

These costs overlap in callers but have different owners. Native `complete()` is observation assembly: it is not a numeric validator. A replay of retained samples measures evidence handling, not fresh product operations. Any unassigned CPU in an actual capture remains unattributed.

## One fixed source snapshot

`SampleBatch` still owns a fixed tuple of root operations and an explicit pending-root limit. Initial `project_bindings` validates the entire reachable union using the unchanged public reader and its existing ownership, metadata, cycle, depth, unit, and relation rules. The batch retains immutable canonical projection bytes, exact operation/binding/declaration/delegate-tuple identities, and each row's canonical digest.

The new `source(operation)` lookup returns the captured binding and digest for a member of that admitted closure. It checks object identities without hashing, serializing, compiling, or receiving a sample. Full declaration contents are checked by `_current` before and after receiving, including empty context exit. Thus a frozen declaration forcibly mutated in place still prevents successful receipt even when it is not selected by a particular leaf group.

Metadata lookup does not grant independent receiving authority. The default accepts only explicitly supplied roots, preserving the adopted caller/unit scope. `include_descendants=True` explicitly admits all owners in that same captured closure for the extended Core recorder. Foreign callables, including another callable with an identical declaration ID, remain rejected.

## Exact completed-record receipt

`receive(operation, sample)` accepts the existing completed record without rebuilding it, filtering fields, or deriving primitive values from bounds. It captures an identity seal, enters the bounded pending buffer, and flushes at the configured root limit. `sample()` remains the builder used by existing Core helpers and delegates admission to `receive()`.

A flush checks all admitted source rows, checks every pending seal, and groups all pending roots by operation. It decodes the immutable union once, selects each operation's complete reachable declaration closure, and gives each public reader a fresh deep copy including all top-level fields. Selection follows every declared delegate edge, including edges unused by bound expressions. One reader's mutable mapping cannot affect another group or flush.

This avoids compiling the entire 100-declaration caller union for each small leaf. It does not retain parsed contexts or copies of every owner's projection across flushes. The public reader still validates the group's metadata and every supplied root/nested sample afresh. Initial union validation and full source-content rechecks remain responsible for metadata elsewhere in the admitted union.

The pending limit bounds roots, not bytes or RSS. Existing per-root sample and source metadata admissions still apply. Scope exit releases source maps, digests, immutable bytes, and pending records. The helper is a sequential test-observer lifetime, not a concurrent source-mutation protocol or hostile-code security boundary.

## Native construction and outer helpers

`observe_replay(..., batch=...)` obtains binding/digest metadata from the explicit outer scope. It still walks the declared operations and installs/removes its existing primitive wrappers. Inner `complete()` remains dictionary/arithmetic assembly. It performs no sample sealing, hashing, serialization, queue admission, or validation while primitive observers are active.

This separation matters because Native observers wrap global hash and JSON functions, and lifecycle recording transitions themselves account for resources. Adding a hash or toggling recording inside child completion would alter primitive observations.

The optional scope is forwarded through `session_observer` and `advance_observer`. `checkpoint_samples` and `advance_samples` own one four-root batch around their finite supplied/generated case loop. Each completed root enters that batch at the old public-validation site, after primitive observation ends and before later assertions or progress writes. A later iterator/assertion failure therefore still receives earlier completed roots during unwinding.

Native child assembly was not independently validated before this change. Its existing nested records remain within the independently received outer roots; no new independent child root is fabricated, and none of the existing roots is omitted. The explicit `failed_operations` policy and existing failure completion remain unchanged. Other observer callers retain immediate construction when no scope is supplied.

## Core inline construction and receiving

The opt-in descendant scope flows from the caller through the unit recorder, stamp/framing/preparation emitters, and the Native-owner opening/advance recorder hierarchy. Constructors use admitted metadata instead of preparing overlapping projections and digests per operation. The default caller/unit-only routing remains available with the original explicit root admission.

Every Core child that was independently validated still has one independent receipt before being attached to its parent. Native opening, session work, close, and final-root sites likewise receive once at their existing validation boundary. Parent roots still recursively check those child observations again. Parent/child overlap is not used to discard evidence.

New Core sealing and automatic flushes run under the existing `paused()` chain, which suppresses Core, lease, and open Native database observers and restores their state. An admission/validation failure poisons the batch; catching it cannot turn context exit into success. Pending valid roots are received during parent failure. Invalid pending evidence retains the original exception through normal chaining.

A delayed failure may belong to an earlier root rather than the sample triggering a flush. The Core batch path therefore records a batch-receiving scope and chains `EvidenceFailure` without assigning another sample's declaration, metric, or value to the triggering operation. Immediate-mode diagnostic behavior is unchanged.

## Evidence boundary

The baseline captures exactly three real service-owned turns of one 4,913-byte journal with zero unrelated rows. It supplies 122 independently received roots and 1,078 nested checks for matched literal replay. The final direct comparison must include source admission, metadata construction, wrapper setup/teardown, emission, sealing, flushing, and release. Counts and timing are separate passes, and active product instrumentation is not reproduced by literal replay.

This reference does not explain all of the interrupted 600-second diagnostic or the separate publication-helper cost. No full census, trajectory population, large campaign, new acceptance cache, public package API, product/declaration change, or concurrency-policy change is part of the proposal.
