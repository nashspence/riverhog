# G22 retained algorithms and declared cost profiles

[DESIGN.md](DESIGN.md) owns the current bounded/durable execution protocol.
The prior detailed algorithm/correctness submission is retained at
[4621be70:g22/DESIGN.md](https://github.com/nashspence/riverhog/blob/4621be70fe2229ef7de169862ff1ec5274dec00f/.reference/g22/DESIGN.md).
Read its semantic arguments, NOT its superseded physical transaction/savepoint,
cleanup or apply-the-patch instructions. Historical _journal_epochs.py,
_online_dag.py, integration.patch and their native evidence are unchanged.

## Certificate-first replay

A complete finite transcript API returns one accepted summary after validation.
Split the transcript immediately BEFORE each correction. For each epoch, start
with the effective graph after that correction and include the epoch's later
insertions, separately for derivation and specialization. Retain assertion identity
and multiplicity even when multiple assertions define one topological edge.
Speculative removal of a retraction target is not authority to perform it: replay
still checks exact target identity, earlier entry and liveness.

A strict topological order of the epoch union proves acyclicity of every insertion-
only prefix subgraph. Use indexed rank comparisons only for that subcheck. Native
schema, predecessor, recorder, identity, profile, time, reference and lifecycle
checks still execute at each original prefix. Future objects/ranks cannot cure an
earlier missing reference. Never concatenate selected journals into one effective
graph or alter exact original octets/anchors.

If the speculative union is cyclic, use native-order online replay instead of
reporting a later cycle ahead of an earlier semantic error. A bad prefix followed
by correction remains invalid. A legal correction that removes an edge and adds
its reverse must not be tested against an artificial union containing both epochs.
Persist Kahn degree/ready/rank and high-degree adjacency progress as DESIGN specifies;
logical topological work is not one physical SQL operation.

## Online level/fuel fallback

Use the level/fuel variant of Gueneau, Jourdan, Chargueraud and Pottier (ITP 2019).
For v->w, self edges reject and topological duplicates do not create another arc
(documentary identity still matters). Unknown vertices begin at level 1. L(v)<L(w)
permits direct insertion. Otherwise search backward from v along horizontal incoming
edges with fuel L(v), rejecting if w is reached. Count examined edges even when the
predecessor was already marked. If backward search finishes at equal level, insert;
if it finishes at a different level, promote w to L(v). Exhaustion promotes w to
L(v)+1. Clear stale horizontal incoming records and propagate forward, rejecting
if a backward-marked node is reached; promote successors below the new level and
record equal-level incoming edges. Do not traverse through already sufficient levels.

Levels do not decrease within an insertion-only run. Preserve the horizontal-
incoming invariant and separate graph kinds. Correct rejection must restore prior
edge/level/incoming state before another online operation observes the checker.
DESIGN's paged undo/quarantine replaces the prototype's graph-sized savepoint.
Corrections reset online accounting and rebuild the active semantic view; no
insertion-only amortized bound is applied across deletions without justification.
The published Coq proof does not certify this independently adapted SQLite code.

## Other retained fixes

The touched-row index must match the actual reset predicate including inactive
rows; do not narrow semantics just to use the former partial index. Reset touched
rows in bounded indexed batches. Retain exact per-state earliest dated usage or
invalidation and latest dated usage, preserving the native pairwise generation/
invalidation inequalities. Corrections rebuild those extrema from the effective
view; withdrawn extremes cannot remain authoritative. Untimed events create no
invented timestamp. Keep native uniqueness and within-activity checks.

## Candidate profiles, not timing guarantees

H = exact input bytes; A = authored assertions; V/E = distinct graph vertices/arcs.
Index/key bytes and schema/canonical/profile work are separately charged.

T0, valid complete append-only journal: O(H+A+V+E) useful graph/byte visits, plus
O((A+V+E) log(2+A+V+E)) indexed work at fixed-width keys and actual key-byte costs.
The admitted entry window and private cache bound RAM; graph/frame work can live
on disk. No complete graph materialization is required to validate.

T1, real corrections: O(H+A+sum_i(V_i+E_i)+sum_c active_assertions_c) useful visits,
plus their index/schema costs. It can be quadratic in independently growing
correction count and active graph. Do not manufacture epochs/corrections or call
repeated internal work a new necessary snapshot to excuse amplification.

O, true online insertion-only run: the selected abstract algorithm has amortized
O(n + m*min(sqrt(m), n^(2/3))) aggregate work, plus actual SQL/index/clearing costs.
A terminal cycle attempt can visit O(n+m); repeated rejected-and-continued attempts
need their own accumulated cost. Restart after a correction begins new accounting.
This is not a strict-linear promise or a proven bound for the independently
implemented persistence adapter.

M, materialized append: charge the SUM of supplied and returned prefix bytes across
calls plus applicable validation. N full-prefix append calls cannot be advertised
as a linear lifecycle merely by analyzing the last result. Bulk input should use
the maintained batch/spool path where its contract fits.

Add durable checkpoint/undo/source traffic, retained state and reclamation under
DESIGN. Ordinary recovery reuses committed work; loss of durable state is a distinct
failure/rebuild case. G23 corpus, G24 history, G26 indexing and arbitrary schema
costs remain separate delegates. No composite passes with missing delegate proof.
