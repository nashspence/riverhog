# Integration map: remove repeated work, not proof

Owning issue: #964. Measurement authority: #963, following #961.
Audited source: `8425e672c60285a549ca4d5aaba15ecbd99e5bf3`.
Prior reference: `703ff38fa6c0f50d9954b78503e16c9958b2f46a`.
This file is non-authoritative #903 input. Read current issue comments first.

## Scope and disposition

The maintainer's #960 comment 6102101195 explicitly approves closeout with the
known unmet 600-second objective after complete applicable functional validation,
exact-SHA evidence and cleanup. Do not keep adding performance-only commits to
#960. Its target and maintained profile remain: a miss is reported, not redefined
as success. Carry the final observed shortfall into this issue. Functional gaps
and missing evidence are not waived; do not close unrelated owner issues here.

#963 and #964 can be one coordinated effort with separately reviewable slices.
Preserve #961's prerequisite. Land the shared measurement foundation before the
#964 production/target integrations depending on it; reconcile this audit on the
resulting exact baseline. Do not create another work/target registry. Workload,
hardware/condition, new numeric-target, proof-cadence and representation decisions
still pass #964's maintainer checkpoint. Reference-only probes can run now.

The closure assessment is deliberately limited: the completed historical
`5a84732e` run was 20,469.23 seconds, and a later `a71c5335` checkpoint measured
336 seconds of input publication and 1,620 seconds of planning. Current source
has more fixes, but no completed sub-600-second witness was available. Those
observations plus repeated source-level work make more isolated #960 closeout
tuning a poor bet; they do not prove the target impossible or that every finding
below dominates the 128-WAV scenario.

## Growth objective

For sparse, output-linear workloads, require useful work to grow linearly with
required input bytes, unique records, actual declared edges and requested output.
Count visits, comparisons, hash/read bytes, database work and stored state, not
just elapsed time. A batch, disk-backed queue, more workers or a warm cache does
not make a quadratic algorithm linear. An internally expanded member-by-history
table is not automatically a necessary result against which to normalize cost.

State the cost model precisely. B-tree/ordering costs and dynamically changing
snapshot constraints must be measured and explicitly justified. No universal
worst-case O(file_count) claim is made for arbitrary dense output or online graph
updates. Do not weaken every-snapshot cycle, correction, authorization or custody
semantics to meet an asymptotic label. Unresolved complexity is an open design
question, not permission to quietly accept a quadratic implementation.

Retain the 1k/10k/100k research envelope and one large exact collection. Sparse
controls must be distinct from genuine dense relationship workloads. The current
single-sidecar fixture cannot expose balanced pairing cost; recovery is a separate
required surface, not interchangeable with successful server reads.

## Evidence index

Raw witnesses are in `superlinear/results.json`; source Git/SHA-256 identities
are embedded. N has the meaning in each row, NOT always WAV files. SQLite VM
counts are rounded down to 100 instructions and are engine/plan-dependent.

| ID | Witness / N | At 128 | At 256 | At 512 |
| --- | --- | ---: | ---: | ---: |
| S1 | Reverse-inserted chain, incremental cycle check (VM instructions) | 3,672,700 | 28,661,100 | 226,470,600 |
| S2 | Corpus chain elimination (VM instructions) | 90,300 | 344,700 | 1,345,000 |
| S3 | Growing journal touched reset (VM instructions) | 38,200 | 125,700 | 448,100 |
| S4 | Pending history queue (VM instructions) | 29,600 | 108,600 | 413,800 |
| S5 | N usages of one state (related rows decoded) | 16,384 | 65,536 | 262,144 |
| S6 | N primaries plus N sidecars, N real matches (comparisons) | 16,384 | 65,536 | 262,144 |
| S7 | Whole-scope port of N subjects (subject visits) | 8,256 | 32,896 | 131,328 |
| S8 | N one-segment journals (descriptor visits) | 33,024 | 131,584 | 525,312 |

Check raw counters when reproducing; do not install SQLite instruction constants
as portable product performance thresholds. The tests of this historical audit
expect the old bad counts; they prove diagnosis, not optimized product acceptance.

### S3: touched reset — first narrow correction

Owner: `packages/riverhog-provenance/src/riverhog_provenance/journal_store.py`,
`JournalStore.accept()` and scratch-schema construction.

An unconditional `UPDATE ... WHERE touched=1` cannot use the existing partial
index whose predicate requires `active=1`; a growing assertion table is rescanned
per frame. The diagnostic full touched index preserves row/flag outcomes and
changes the series to 16,600 / 33,200 / 66,500 VM instructions.

Implement an index or explicit bounded touched set consistent with the existing
semantics. Do not add `active=1` to the update as a casual shortcut. Test inactive
and retracted assertions, correction/replacement, empty frames, replay, and exact
journal/graph identity. Verify plans under the repository's actual SQLite version.
No archive encoding or public contract should change for an access-path fix.

### S4: pending histories/snapshots — same first slice

Owner: `riverhog_provenance/history.py`, `MemberHistoryClosure._drain()`.

`WHERE done=0 LIMIT 1` repeatedly scans completed rows. Add exact pending indexes
or a dedicated work queue while retaining the separate complete deduplication
state. Experimental partial indexes reduced the 512-history diagnostic to 25,500
instructions with all 512 visits retained exactly once.

Test histories AND snapshots, newly enqueued dependencies while draining, cycles,
duplicate imports, empty queues, cancellation, failed reads/retry and cleanup.
Audit the analogous states/views queues in `canonical_discovery_relevance.py`,
but mark them as related source-audit candidates until separately measured. Do
not label those unmeasured instances an additional confirmed probe.

### S7: observation subject lookup — same first slice

Owner: `some-implementations/stove0/application/server/src/stove0_core/accepted_observations.py`,
`AcceptedObservationStore.register_request()`.

Replace repeated searches of `request.subjects` with one indexed view of the
validated exact request set. Expected work is one pass over subjects plus one
lookup per actual port membership, not N(N+1)/2 subject visits.

Preserve duplicate/missing/extra rejection, full subject equality, exact selection
membership, port identity, retrieval policy and read actions. Test multi-port
scopes, shared subjects in lawful ports, mismatched identities, empty/invalid
scopes and large whole-scope requests through the real acceptance path. Fixed
16-subject batches do not qualify whole-scope behavior or authorize total limits.

### D0: existing output-decision lookup — already in the original handoff

Owner: `a_stove0_media_archive_lib/publication.py`,
`MediaPublicationPlan.decision_for()`; invoked by the Opus target.

Index the validated exact immutable decision set; retain ordering in the committed
representation and reject unknown or conflicting IDs. Test isolated caller views,
missing/duplicate decisions and real target output identity. The original probe
records 33,153 comparisons for 257 decisions and 2,003,001 for 2,001 decisions.
Large inline plans/status serialization are separate representation questions;
a lookup dictionary is not proof that total memory/control payload is bounded.

### S5: repeated lifecycle checks — second slice

Owner: `riverhog_provenance/journal_store.py`, `_StoredValidator.finish()`.

Each touched usage fetches all usages/invalidation for its state. N untimed usages
produce N^2 decoded related rows despite only N events. Separate newly added usage
validation from state lifecycle-bound changes; handle each affected state's
required recheck once per accepted batch/snapshot where semantically valid.

Test timed/untimed usages; generation arriving before and after usage; late
invalidation; contradictory times; corrections/retractions; multiple states; and
no cross-state attribution. Assert identical accept/reject outcomes and graph
findings. A warm complete-journal cache is not a solution for fresh or oversized
snapshots. Avoid claiming globally linear online updates without analyzing the
actual sequence of required accepted snapshots.

### S6: balanced filename matching — second slice

Owner: `some-implementations/stove0/observers/filename-prefix-sidecars/src/a_stove0_filename_prefix_sidecar_observer/pairing.py`,
`compare_filenames()`.

Index valid primary names by exact comparability authority, syntax, root, parent,
full leaf and stem. Preserve existing full-leaf precedence, original byte/code
unit semantics and all legitimate matches. Generate candidates only for matching
keys, then preserve deterministic required output order.

Test POSIX and Windows naming/encoding cases, explicit naming views versus same
context, relative/root differences, mixed authorities, ambiguous locators, duplicate
stems, suffix ambiguity, unusable evidence and dense genuine matches. Compare
complete candidate/status/evidence outputs with the old implementation on bounded
exhaustive fixtures. Do not choose just one match or strip provenance to save work.
The sparse ladder requires O(P+S+matches) candidate generation, with ordering cost
reported separately; real dense matches remain a different output-sized workload.

### S8: independent recovery — second slice

Owner: `some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py`,
`iter_journal_range()`, and its loop in `recovery.py`.

Build a root-qualified local descriptor index once or stream all selected journals
in one authenticated pass. Avoid full descriptor/root scans for every journal.
Keep touched payload byte/digest checks, range bounds, complete counts, root and
terminator verification, independent recovery, and bounded scratch/restart work.

Test exact standalone final-image recovery, many journals/segments, partial ranges,
missing/duplicate/reordered/truncated descriptors, corrupt payloads, wrong root,
interruption/retry and cache eviction. The historical probe supplies parsed fake
descriptors and a restricted commitment helper: it is NOT archive conformance.
Do not import server/client/database implementation into the independent recovery
tool to reuse a convenient cache. Shared behavior must have a neutral owner.

### S1/S2: graph validation — coordinated design slice

Owners: `_Edges.append()` in `journal_store.py` and
`CanonicalCorpusValidator.validate()` in `corpus.py`.

S1's reverse chain causes repeated reachability. On SQLite 3.46.1's fresh scratch
schema, a kind-only recursive access path multiplies that into cubic VM work.
The experimental join-order control yields 218,500 / 863,200 / 3,430,300: a useful
constant/query-plan correction, but still QUADRATIC. Do not present it as done.

For S2's complete corpus, use persisted indegrees and a ready queue with indexed
outgoing edges rather than recomputing/deleting over the entire remaining set.
Consume each edge a bounded number of times. Both chains and disjoint pairs need
controls, plus forks and each supported graph kind.

For S1's incremental snapshots, establish which constraints permit affected-frontier
or topological maintenance without deferring required cycle rejection. Corrections,
retractions and adversarial insertion order may require additional work. Present
that design/cost argument explicitly; this handoff supplies no universal O(V+E)
algorithm for arbitrary dynamic updates. Add product tests for self loops, duplicate
edges, forward/reverse chains, branching/converging DAGs, cycles introduced late,
corrections that create/remove edges, and cross-journal semantics. Preserve exact
failure timing at every required snapshot; validating only the final graph is not
an equivalent proof if invalid intermediate acceptance becomes possible.

## Related architectural candidates, not already-solved issues

The current discovery model expands selected recorded-history assertions for each
member. Batching SQL writes reduces cost per row, not logical row count. Measure
shared history versus genuinely independent/dense cases, then decide whether a
compact member-to-snapshot or other representation preserves query scopes and
results. Keep member attribution distinct from recorded co-residence. Changes to
index commitments or durable schemas require explicit owner/maintainer decisions.

Also retain cold/evicted/bypassed-cache tests, exact streamed hash-byte counts,
full-plan/status copy volume, ciphertext object counts, and resume replay. Recent
#960 fixes may already eliminate some old findings. Do not rebuild an obsolete
cache or suppress current authorization reads to force an attractive count.

## Measurement and product validation

1. Reproduce bounded diagnostics; verify recorded source hashes against the exact
   baseline. Report SQLite/Python versions and query-plan differences.
2. Add product-owner tests that fail on the actual old mechanism and pass on the
   candidate, covering exact output/error/authority and interruption semantics.
   Source-extracted probes are diagnosis scaffolding, not substitutes.
3. Measure increasing sparse sizes using semantic operation counters and actual
   state size. Require a constant multiple of useful work where the proposed
   algorithm claims linearity; expose repeated hashing/serialization too. A
   constant batch or cap cannot hide N multiplied by the number of batches.
4. Exercise 1k/10k/100k workloads in a staged, explicitly resourced plan under
   #964's decision checkpoint. Do not extrapolate hypothetical wall times as passes.
   Keep whole collection identity and complete proof. Keep stress workloads
   separate from ordinary commit cadence unless explicitly approved.
5. Use #963 for targets/conditions, report-only comparisons, missing/invalid evidence
   failures, phase/aggregate timing and exact-source reporting. Preserve the 600s
   scale objective and record progress toward it alongside growth at other sizes.
6. Complete all applicable locked local gates, exhaustive portable qualification,
   real final-image behavior/recovery/PostgreSQL checks and independent exact-SHA
   CI/CodeQL. Review every unresolved target/condition and explicit exception.

## Reproducing this reference safely

Use a separate audited-base checkout: the historical probe intentionally requires
its HEAD to equal the audited SHA. A reference branch's newer HEAD is not that SHA.
Copy evidence to a new output directory to avoid overwriting retained observations.

```sh
AUDIT_ROOT=/absolute/path/to/riverhog-audited-base
REFERENCE=/absolute/path/to/this-reference-checkout
OUT=$(mktemp -d)
# Prepare AUDIT_ROOT at 8425e672c60285a549ca4d5aaba15ecbd99e5bf3 using git worktree.
cp -R "$REFERENCE/.reference/superlinear/." "$OUT/"
(
  cd "$OUT"
  export RIVERHOG_AUDIT_ROOT="$AUDIT_ROOT"
  python probe_quadratic.py > probe.log
  python probe_query_plan.py > query_plan.log
  python -m unittest -v test_probes > test_results.log 2>&1
)
```

All three commands use standard-library Python; the original environment was
Python 3.13.5 and SQLite 3.46.1. Native production types, HTTP/claims, encryption
and PostgreSQL are not fully exercised by these isolated probes. Do not transplant
the intentionally simplified diagnostic dependencies into production code.

## Handoff completion report

Report each finding as reproduced, fixed, superseded by current source, design
pending, or not currently applicable, with evidence. Record old/new operation counts,
output/error parity, source SHA, resource/engine context, real qualification and
approved timing/growth comparisons. Do not silently count a query-plan improvement
as an asymptotic solution. Preserve issue ownership and the exact #960 exception;
there is no blanket permission to waive future misses or protected boundaries.
