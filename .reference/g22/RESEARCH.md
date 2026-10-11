# G22 research and choice record

This is source-based engineering research, not a proof that dynamic graph problems
have a universal linear solution. Accessed 2026-10-11. The recommended certificate
prepass exploits Riverhog's COMPLETE-input call boundary; it is not advertised as
an advance in general online graph algorithms.

## Primary sources and applicability

1. Armael Gueneau, Jacques-Henri Jourdan, Arthur Chargueraud and Francois Pottier,
   *Formal Proof and Analysis of an Incremental Cycle Detection Algorithm*, ITP2019.
   [Paper](https://drops.dagstuhl.de/entities/document/10.4230/LIPIcs.ITP.2019.18)
   and [author overview](https://gallium.inria.fr/blog/incremental-cycle-detection/).
   Figures 2/3 specify the genuinely online level-fuel variant: backward fuel is
   the source level, with forward promotions. The authors prove the OCaml algorithm's
   abstract amortized bound O(n+m min(sqrt(m),n^(2/3))). The proof does not cover our
   SQLite implementation. Its graph is invalidated on a cycle; our rollback adapter
   therefore has separate correctness tests and separately charges rejected attempts.

2. Michael A. Bender, Jeremy T. Fineman, Seth Gilbert and Robert E. Tarjan,
   *A New Approach to Incremental Cycle Detection and Related Problems*.
   [Preprint](https://arxiv.org/abs/1112.0784).
   Foundational level-based algorithms; the 2019 variant is preferred here because
   it does not require guessing final vertex/edge totals or maintaining that guess
   through resizes. Do not accidentally mix the older fixed-threshold update rules
   with the newer proof's level-dependent fuel rule.

3. David J. Pearce and Paul H. J. Kelly, *A Dynamic Topological Sort Algorithm for
   Directed Acyclic Graphs*, Journal of Experimental Algorithmics.
   [Author copy](https://www.doc.ic.ac.uk/~phjk/Publications/DynamicTopoSortAlg-JEA-07.pdf).
   Serious practical alternative with insertion/deletion support and local order
   repairs. It does not supply an unconditional linear aggregate bound; array-wide
   reorder implementations can add another hidden scan. Not selected because the
   actual bulk-validation interface permits a simpler rank-certificate approach,
   with the stronger researched online fallback when necessary.

4. Li Chen, Rasmus Kyng, Yang P. Liu, Simon Meierhans and Maximilian Probst Gutenberg,
   *Almost-Linear Time Algorithms for Incremental Graphs: Cycle Detection, SCCs,
   s-t Shortest Path, and Minimum-Cost Flow*.
   [Preprint](https://arxiv.org/abs/2311.18295).
   Almost-linear incremental cycle detection exists in the literature; no claim of
   impossibility or optimality is made for the selected fallback. That construction
   involves a substantially larger dynamic-graph/min-cost-flow framework. Adopting
   it here would be a separate algorithm-engineering project rather than a focused
   low-dependency integration. It is not needed for the complete-transcript profile.

5. SQLite [query planning](https://www.sqlite.org/queryplanner.html) and
   [savepoints](https://www.sqlite.org/lang_savepoint.html).
   SQLite index/grouping costs remain explicit. A single SQL call can hide a scan;
   count visited data and inspect real plans. Savepoints roll back one rejected
   edge attempt without conflating that rollback with journal publication or
   permission to resume arbitrary rejected streams for free.

6. Riverhog exact source, not generic algorithm terminology:
   [journal.py](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/packages/riverhog-provenance/src/riverhog_provenance/journal.py),
   [journal_store.py](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py),
   [upload service](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/services/collection_uploads.py),
   [prefix and bounded-memory tests](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/packages/riverhog-provenance/tests/test_journal_extent.py),
   [leased scratch](https://github.com/nashspence/riverhog/blob/152d095038df0c0d0ed642cf265d47b11c65a754/riverhog/src/riverhog_core/scratch_workspace.py).
   These define the unchanged acceptance boundaries. Transport append, semantic
   validation, sealed history and archive-root publication are distinct stages.

## Alternatives not selected

- **One final effective-graph check:** insufficient. An invalid prefix may become
  valid after a later correction or object declaration. All prefix checks remain.
- **Recompute reachability on every edge:** simple but exactly the multiplier under
  audit; changing only its query plan does not remove aggregate repeated traversal.
- **Only dynamic levels on all input:** correct but unnecessarily charges its
  non-linear aggregate bound even when the complete transcript is already supplied.
- **Full transitive closure:** potentially quadratic space and update amplification;
  contradicts the desired sparse/shared scaling and is not necessary for cycle checks.
- **Atomic staged public batches:** could change the problem, but also changes when
  callers obtain accepted evidence. Not needed and not authorized by this design.
- **Permanent warm graph cache:** cannot establish cold/bypass or revoked-reader
  behavior, and does not repair a bad underlying algorithm.

## Scope of the supplied evidence

The online graph implementation was translated and tested independently, not copied
from or certified by the authors' Coq extraction. The epoch certificate correctness
argument is specific to monotone edge sets between corrections plus unchanged prefix
replay. Tests corroborate it; finite samples are not a universal proof.

No new library or runtime dependency is proposed. Native tests used the exact upstream
rfc8785.py 0.1.4 implementation externally because the locked workspace could not be
installed here. It is not bundled or substituted in the patch. No full-runtime speed
projection, accepted 600-second result, or G26 result follows from this research.
