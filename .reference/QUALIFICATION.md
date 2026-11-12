# Focused consumption and qualification

This is proposed evidence for #989's asynchronous scope gap, not acceptance of
#964's full retirement or larger-collection campaign. Reconcile the current
source and owning decisions before adopting the candidate. Do not merge the
reference branch as production integration.

## First useful slice

In a separate disposable checkout of the audited base with this reference
material, run the unchanged-source reproduction, then the explicit source
assembler, then the maintained-owner witness runner:

```sh
mise x python uv -- uv run --locked --all-packages --group dev python .reference/reproduce.py
mise x python uv -- uv run --locked --all-packages --group dev python .reference/prepare.py
mise x python uv -- uv run --locked --all-packages --group dev python .reference/run_witness.py
mise x python uv -- uv run --locked --all-packages --group dev python .reference/check_transcripts.py phase-samples.jsonl
```

`prepare.py` refuses edited audited inputs and is intentionally not idempotent over
its own source overlay. Start from a fresh worktree for each run. It is the single
candidate assembly entrypoint (using the exact-base `apply.py` helper): reference source modules and explicit test edits
produce the exact implementation exercised by the runner. The published product
tree stays unchanged; result artifacts contain the overlaid source hashes.

Use the actual 17-byte file, three-byte admission and two terminal slots. After
first submission, hold execution outside the scheduling root. Release/join that
exact worker, check positive original actor receiving and the still-charged
ordinary reservation, then make the next scheduling call. Its result must be
three, decomposed into completion, withdrawal and next submission. File bytes
must change only when the associated worker actually runs. A separate two-original
vector reaches four with an original close resubmission in the same turn.

## Preserved and additional vectors

| Vector | Required distinction/evidence |
| --- | --- |
| Empty, linked and zero-limit | No admitted retirement worker and no invented physical progress. Invalid controls reject before registry mutation. |
| Independent catalog population, wide controls, large epoch, cold/warm | Retain the existing catalog/unit/loop regimes and child samples. Do not replace unrelated-row work with a test model or omit the wide original scope. |
| Busy writer, busy reader, busy active member, three busy candidates | Real filesystem custody refuses work on original workers. Coordination stays bounded; three candidates are not renamed three successful retirements. |
| Independent file extents and octet grants | Actual truncated/unlinked extent agrees with the admitted worker quantum. No scheduled byte grant is counted as bytes already reclaimed. |
| Capacity and repeated blocker | Preserve the real fsync refusal and retained remaining bytes. Record the blocker through the actual conditional catalog path; repeated equal blockers must not advance its version. |
| Unclassified file, missing coordinator | Keep unknown bytes and actual custody errors. Do not infer cleanup completion or replace the original owner. |
| Known close refusal, repeated close refusal, unknown native close | Reuse the exact actor and ordinary grant. No repeated `_advance`, fresh descriptor or forced receiving. A terminal worker slot can finish while ordinary receiving remains pending. |
| Slow original exception-frame receiving | The actual worker waits at the injected receiving boundary, not the scheduling thread. Another admitted original advances and retires independently. |
| Parent service scheduling | A truthy child bookkeeping result counts one parent scheduling turn. Its sample ends before separately observed worker release/join. Preserve validation/readiness and busy/rejection regimes. |
| Shutdown | Stop new admissions, join actual workers, then perform explicit execution-side stopped receiving. This full retained traversal is not smuggled into the per-call coordination bound. |

The original resource/namespace/SQLite/worker conditions of #987/#988 remain in
force. Existing real native unknown/reuse tests are included through the maintained
journal/discovery registry cases; a new scheduling bool cannot replace them.

## Evidence composition and negative controls

Validate every root and its actual children using the maintained growth framework.
The scheduling root contains registry and catalog-selection children, not future
file work. The original worker root contains construction, actual unit work,
resource receiving and real blocker formatting/catalog work, including partial
and exceptional paths. Close-only roots contain no repeated unit work.

For each admitted attempt retain the original context, exact reservation/scope,
actual worker object and forward/close kind. Match those to a separately recorded
worker root and join, plus a pending or withdrawn ordinary disposition. A new
quantum is a new original even for the same job/scope; close retry is the same
original with a new physical attempt. Reject missing or duplicate causal links,
changed originals and primitive samples assigned to the wrong phase. Retain the
full sample stream/digest rather than maxima alone.

Counts must be observed from source primitives or actual child samples, never
populated with the upper bound. Independently measured child inputs must dominate
every heterogeneous call; validate each child before summing it. Failure evidence
must not disappear because an exception prevents a normal return.

Useful negative controls are the old `L` envelope against the three-event case,
restoring an inline `_advance` child, making a close retry reconstruct an actor,
and dropping a worker sample or original join from the transcript. Each must
fail its corresponding assertion or validator. Numeric ceilings must not be
raised to accommodate a bad observation.

## Integration and remaining obligations

The candidate's generated module/style and final declaration identifiers stay
with the owning integration. Add the accepted module and tests to the current
source/interface/closure/growth/distribution gates, and verify source discovery
rather than assuming a reference test selection supplies global completeness.
Preserve existing declared primitives and platform-qualified resource owners.
The branch-only workflow is not a new permanent product gate.

Required exact-final-source and tested-merge product qualification, supported
backend/platform integration, full ordinary/terminal budget scenarios, the
approved independent-arm 1,000-member proof and the preserved 128-file lifecycle
remain separate work. No timing target, release/provider permission, archive
identity change or runtime activation is proposed here.
