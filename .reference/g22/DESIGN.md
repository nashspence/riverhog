# G22: certificate-first exact-prefix validation

Recommended integration design for #964 under #903. Source audited:
`152d095038df0c0d0ed642cf265d47b11c65a754` (tree
`991e994457b487a7dca016b7fc0409485cef25c6`). This is a tested reference design,
not a claim that main or a released contract already conforms. G26 is untouched.

## 1. Decision

Use **two explicitly different execution profiles**:

1. For a supplied complete finite journal, make one disk-backed topology prepass
   per correction-delimited epoch, then replay every original frame in order through
   the normal semantic validator. A strict topological certificate makes each cycle
   check an indexed rank comparison instead of a new reachability traversal.
2. When a certificate is unavailable (including a cyclic speculative epoch), use
   the genuinely online level/fuel algorithm of Gueneau, Jourdan, Chargueraud and
   Pottier (ITP 2019), adapting Bender et al. No advance final graph size, random
   priorities, transitive-closure matrix, global order-array rewrite or recursive
   Python call stack is required.

Preserve current correction acceptance as an atomic effective-view update. Reset
its disposable validation aggregates and rebuild them from that effective view,
just as the existing implementation does. Charge this cost explicitly; do not
promise linear total work in the final graph for arbitrary many corrections.

This recommendation deliberately distinguishes a **complete transcript API** from
an API returning a separately accepted answer after every arriving edge. Using
look-ahead inside the former does not create staged public acceptance in the latter.

## 2. Why the actual maintained interface permits this

Audit these exact owners, not inferred acceptance from function names:

- `packages/riverhog-provenance/src/riverhog_provenance/journal.py`:
  `validate_journal_chunks` returns one JournalSummary only after all input, every
  prefix, predecessor/checkpoint and optional exact-tail checks have succeeded.
  It does not yield intermediate accepted semantic results.
- `write_assertion_batches` already spools exact complete frames, then validates
  the complete transcript before returning it to its publisher. Materialized
  append helpers return the entire resulting journal, not a delta-only resource.
- `riverhog/src/riverhog_core/services/collection_uploads.py`:
  `append_collection_provenance_journal` acknowledges exact transport octets and
  offsets/retry identity. Sealing verifies full byte count/hash, enters validating,
  calls `_validate_next_upload_journal_entry`, and only then establishes sealed
  semantic state. A transport append is not semantic acceptance of every prefix.
- `journal_store.JournalStore.accept` currently checks EVERY frame's effective
  semantics. Retractions are exact assertion references; IDs/identity domains and
  retired assertions are not reusable; old canonical frame bytes remain recorded.

**No public boundary moves.** The new validator still rejects a transcript whose
first invalid prefix is followed by a later repair. It returns no accepted result
until all framing, current-provider fences, graph semantics and tail checks pass.
The prepass may consume a finite epoch before reporting its first semantic error;
there is no new early-rejection latency promise. KeyboardInterrupt/cancellation
and resource failures never produce acceptance. Resource/transport error precedence
is not converted into success or hidden evidence.

The prepass is topology evidence ONLY: it cannot supply future local objects,
recorder authority, profiles, references, temporal facts, or accepted assertion IDs.
All those remain checked against the actual current-prefix semantic store.

## 3. Epochs and immutable replay

An epoch is the maximal sequence ending just before the next `correction` frame.
A correction starts the next epoch, together with all following non-correction
frames. There is no arbitrary configurable semantic boundary or new total limit.

A private SQLite scratch database contains:

| Table | Keys and purpose |
| --- | --- |
| `frames` | position -> original canonical JSON octets, bounded one-frame read window |
| `active` | assertion ID -> graph kind/source/target; speculative active graph assertions |
| `arcs` | unique (kind,source,target), including graph carried into this epoch |
| `degrees` | (kind,vertex) -> remaining indegree |
| `ready` | indexed (kind,vertex) queue, delete on pop |
| `ranks` | (kind,vertex) -> strict topological ordinal |

For each epoch:

1. Retain exact original frame bytes. Do not normalize, reorder or reauthor them.
2. If its first frame is a correction, remove its named assertions from the
   speculative active-edge map before seeding this epoch's arcs. This is NOT
   permission to retract them: exact references, liveness and identity are still
   checked during authoritative replay. Duplicate IDs or invalid retractions can
   at most spoil a speculative certificate; they cannot bypass replay rejection.
3. Add each encountered derivation/specialization edge to its separate graph.
   Keep assertion multiplicity in `active`, but graph reachability uses unique
   edges. Retracting one of two assertions for the same edge must retain the other.
4. Calculate indegrees and run disk-backed Kahn elimination. Each vertex enters
   the ready queue once and every arc is visited once. Outgoing selection is
   indexed by (kind,source,target); ready pops do not scan already completed work.
   SQL grouping, key comparison and B-tree costs are separately accounted.
5. Certify a kind only when all its vertices have ranks. Never publish these ranks
   as archive authority, a public field, or a persisted acceptance checkpoint.
6. Replay frames in their original order. Run all original schema, time, sequence,
   predecessor, checkpoint, recorder, reference, profile and lifecycle checks.
   On an actual edge addition, reject self-edges; for a certified kind require
   both exact endpoints and rank(source) < rank(target). Record the actual edge
   only in the semantic store at its original prefix.
7. If the speculative graph is cyclic, replay with the online algorithm instead
   of raising a later cycle before an earlier identity/reference failure. It must
   find the invalid prefix. A later correction cannot forgive that prefix.
8. Finish the original expected-anchor/exact-tail checks, detach the scratch
   certificate, freeze the original validated store, and return the same Summary.

Parser or source errors encountered while reading are retained until preceding
readable frames have been replayed, then re-raised; no late provider error is
swallowed. Resource errors during private SQL work still abort and clean up.
The context manager owns and releases the stream, connection and directory on
success and all exception paths. Tests exercise both semantic failure and a late
provider revocation.

## 4. Correctness argument

Within an epoch there are no retractions. For a valid transcript, each effective
prefix edge set is a subset of the epoch's final edge set. A strict topological
rank for the union therefore proves acyclicity of every prefix subgraph. Conversely,
a cyclic effective prefix makes that union cyclic; it is not certified and the
online checker rejects the actual failing prefix. Splitting BEFORE a correction
is essential: an edge legally reversed by an atomic correction must not be mixed
with its retracted predecessor in one union.

The graph certificate is only a sound accelerator for the cycle subcheck. Current-
prefix object/reference/type checks remain independent, so an edge referring to an
object declared in a later frame is still rejected even if that object's ID already
has a speculative rank. Identical topology with distinct assertion IDs retains both
assertions and exact retraction semantics. Derivation and specialization do not
share a graph or rank namespace.

Required invariants of accepted output are unchanged: canonical octets and ordered
frame chain, journal/entry/assertion identities, prefix and root digests, checkpoint
coverage, exact external anchors, full history/tombstones, every-prefix validity,
recorder/profile rules and materialized graph/findings. No cache or certificate
can substitute for the caller's current access/fence checks.

## 5. Genuine online fallback: exact integration specification

Implement the 2019 level-fuel variant, not an ad hoc rank repair. The supplied
`_online_dag.py` is an independently tested Python/SQLite port; it is NOT covered
by the authors' OCaml/Coq proof. Research and primary sources are in RESEARCH.md.

Use existing `edges(kind,source,target)` plus private tables:
`dag_levels(kind,vertex,level)`, `dag_incoming(kind,target,source)`,
`dag_seen(vertex)` and `dag_queue(vertex)`. The incoming table contains EXACTLY
same-level incoming edges. Levels never decrease during one insertion-only run;
every accepted edge satisfies L(source) <= L(target). The graph stays acyclic.

For new edge v->w:

- Reject v==w. Ignore a duplicate for graph topology only; normal assertion
  identity/evidence validation still runs at its owner.
- Add unknown vertices at level 1. If L(v)<L(w), insert directly.
- Otherwise search backwards from v over horizontal incoming edges, with fuel
  L(v). Mark each reached vertex; reaching w rejects the insertion. Count examined
  edges even when their predecessor is already marked. No deleted/visited-prefix
  table scan is used to find the next frontier vertex.
- Completed backward search with L(w)==L(v) permits insertion. If completed at a
  different level, promote w to L(v). Exhausted search promotes w to L(v)+1.
- Clear w's horizontal incoming set and propagate forward. A backward-marked node
  rejects the insertion. Promote successors below the new level, clearing stale
  incoming sets and enqueueing each promotion once. Record horizontal incoming
  edges at equal levels, but do not traverse through greater/equal-level nodes.
- Insert the edge and its horizontal incoming record if appropriate. Clear search
  scratch. Deterministic queue tie-breaking is not semantic graph order.

Use a SQLite savepoint around one attempted insertion. Rejection/exception rolls
back edge, levels and incoming state. This permits testing later requests against
the exact pre-attempt graph. It does NOT make repeated rejected attempts free:
retain their measured cost separately. Do not share a mutable scratch connection
between callers. Use decimal levels or an explicitly checked physical representation;
no new semantic vertex/edge/depth ceiling or fixed known-final-size parameter.

Corrections retire assertions before rebuilding active semantic aggregates. Reset
all online level/incoming/frontier state before replaying the effective graph. The
monotone insertion bound must NOT be applied across deletions without a reset or a
separately justified fully dynamic algorithm. The certificate path handles valid
finite correction epochs without feeding every edge through this fallback.

## 6. Remove two remaining semantic bookkeeping multipliers

A graph algorithm alone would leave other G22 prefix costs quadratic.

**Touched rows:** replace the partial touched index with a full index matching
`UPDATE assertions SET touched=0 WHERE touched=1`. The old partial index excludes
inactive rows but the update does not; do not narrow the update's semantics just
to enable the index. Verify the actual query plan on supported SQLite builds.

**Lifecycle bounds:** retain per-state (minimum dated usage OR invalidation,
maximum dated usage). Newly accepted timed events update these extrema once.
Generation may not be later than that minimum; invalidation may not be earlier
than that maximum. These are exactly the previous pairwise inequalities, including
late-arriving generation/invalidation assertions. Preserve same-state generation/
invalidation uniqueness and within-activity checks at their existing owner.
Corrections clear/rebuild extrema from the effective rows, so withdrawing an
extreme usage does not leave a stale bound. Untimed events introduce no fake time.

Do NOT remove graph-reference, fixity-conflict, known-location or current-recorder
validation to meet a budget. G02/G01 schema/canonical costs remain separately owned.

## 7. Precise candidate growth profiles

These are RECOMMENDED declarations to bind and qualify through #963/#964, not
already-approved Closure schema or a speed guarantee. Constants are internal witness
budgets. H=exact encoded input bytes; A=all assertion records processed; V/E=distinct
graph endpoints/arcs. Ordered index and canonical/schema work are explicit addends.

### G22-T0: valid complete append-only transcript

No correction frames. Graph/counter work O(H+A+V+E); each graph arc staged once,
visited once for ranking, and compared once per actual edge assertion. SQL index/
grouping work O((A+V+E) log(2+A+V+E)), with key lengths charged. Add the existing
canonical/schema/profile costs for the constant number of passes over each frame.

RAM is bounded by the existing maximum frame/window plus configured SQLite cache,
not total H or graph size. Private disk is O(H+A+V+E), alongside the existing
validated-store disk. No caller-visible graph is materialized merely to validate.
This is the primary bulk-publication fast path, not a claim that all G22 calls have
that bound.

### G22-T1: valid transcript with C corrections

For each actual correction-delimited epoch i let V_i/E_i be its graph union,
and A_c the active assertion view actually rebuilt at correction c. Logical work:

`O(H + A + sum_i(V_i+E_i) + sum_c A_c)`

plus indexed/canonical/schema work over those visits. In the worst case this can
be O(H + C*(A+V+E)); **it is not linear in final graph size when C independently
grows**. Ordinary append-only production must not be routed through artificial
corrections/epochs to excuse repeated work. This conservative correction profile
preserves all semantics and existing capabilities without claiming an unavailable
fully dynamic bound. Optimizing narrower correction classes later is permitted,
but cannot be called already established by this patch.

### G22-O: actual online insertions / invalid-epoch diagnostic replay

For one insertion-only run with n vertices and m distinct successful edges, the
chosen algorithm's abstract amortized work is O(n+m*min(sqrt(m),n^(2/3))). SQL access
adds its stated log/index cost and clearing work charged to prior insertions.
An attempted terminal cycle can cost O(n+m); rejected-and-continued requests require
the sum of their actual attempted work, not a bound in successful m alone. Start
new accounting after a correction reset. Do not generalize a paper's unit-cost
operations into constant-time SQLite queries or arbitrary deletion support.

The certificate path prevents this fallback from taxing a valid append-only full
transcript. Invalid input remains fail-closed; no linear valid-input declaration
becomes an unlimited denial-of-service or retry guarantee.

### G22-M: materialized append helpers

APIs taking and returning the entire prefix necessarily process/copy those bytes.
Across calls charge `sum supplied prefix bytes + sum returned prefix bytes`, plus
the applicable validation profile—not merely final output size. For bulk output,
use the existing batch/spool interface once. Do not issue N full-prefix append
calls and declare the resulting lifecycle linear by analyzing one call alone.

### Composition limits

This work does not fix G23 corpus-wide elimination, G24 history traversal, G26
indexing, arbitrary schema costs, or every other family's counter. A full Riverhog
lifecycle must account for them independently. No ten-minute result or blanket v1
linearity follows from these G22 results. #960's existing disposition is unchanged.

## 8. Integration map and exact prototype application

The executable payloads are kept under this reference directory, not on main.
Use a separate worktree on exact 152d0950 to reproduce. Against post-#963 source,
inspect drift and reconcile explicitly; the reference branch's runtime base is older.

1. Copy `_journal_epochs.py` and `_online_dag.py` to
   `packages/riverhog-provenance/src/riverhog_provenance/`.
2. Copy `test_journal_epochs.py` and `test_online_dag.py` to
   `packages/riverhog-provenance/tests/`.
3. `git apply --check /reference/g22/integration.patch`, then apply it. It changes
   only `journal.py` and `journal_store.py` at the audited source. No public export,
   schema, archive byte, durable application state or acceptance boundary changes.
4. Run the focused tests and counters below; use the locked toolchain for actual
   integration. Format/type/style checking through that rail remains required.
5. Register the EXACT public-operation/profile applicability and witnesses with
   #963's evidence machinery and #964's owner declarations; do not copy this
   review-label taxonomy into main as a new authority/registry.
6. Validate sealed-upload acceptance/error and current-access fences through the
   real Riverhog HTTP/worker path; retained client tests supplement, not replace it.
7. Preserve archive-only recovery independence. The neutral provenance package may
   use its own private disk indexes, never import server or client implementations.

Do not make online levels, ranks or frontier records durable public authority.
Server temporary files inherit `scratch_workspace.process_workspace`'s leased
TMPDIR; preserve its capacity-error mapping and test killed-owner reclamation in
integration. Native library callers retain their existing temporary-directory
ownership responsibilities. Normal failure cleanup is covered by this prototype;
hard-kill, quota, Windows and final-image behavior are still integration obligations.

## 9. Executed evidence and required finish

At the audited source: original provenance suite 150 passed; patched provenance
suite 187 passed (37 new cases); selected canonical production/completion/claimed
provenance client suites 21 passed. Online cases include all 720 orders of the six
non-self directed edges on three vertices, and 12 seeded 100-attempt sequences,
checked against independent Kahn plus rollback/level/incoming invariants.

The maintained transcript prototype measured 16/32/64/128/256 reverse-chain edges
with exactly N ranking edge visits and N certificate checks. At 128 timed usages of
one state, original decoding visits were 8,512 versus 384 after the extrema change;
complete results match. The touched-reset plan changes from SCAN to indexed SEARCH.
See evidence.json and profile_g22.py. These are native SQLite/Python tests, not
Docker, PostgreSQL, wire-contract, full gate or asymptotic machine-checked proof.

Still required on the actual integration source: locked lint/type/style/full unit,
distribution/image builds, exhaustive portable qualification, independent exact-SHA
CI/CodeQL, real sealed-upload and independent recovery functional parity, bounded source-spool and
killed-owner/capacity tests, and Closure/Audit witness registration. Treat old source
results as historical; do not call .reference tests a completed integration rail.

G22's algorithm choice and file-level implementation are supplied by this design.
The source must still be reconciled and qualified. G26 remains the next independent
design, with no schema, query or state-identity choice made here.
