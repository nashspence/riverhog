# Additional superlinear-work audit

Audited repository: nashspence/riverhog
Audited main SHA: `8425e672c60285a549ca4d5aaba15ecbd99e5bf3`
Producer: OpenAI ChatGPT / GPT-6 Astra Pro
Purpose: locate additional quadratic/worse algorithms beyond the previously
reported media decision lookup and shared-history membership expansion. This
package is diagnostic evidence, not a production patch or qualification result.
No repository source, issue, branch or acceptance criteria were changed.

## Reproduce

Use Python 3.12+ and an exact checkout of the audited SHA. No additional Python
dependencies are required. On the audit host Python was 3.13.5 and SQLite 3.46.1.

```sh
export RIVERHOG_AUDIT_ROOT=/path/to/riverhog
python probe_quadratic.py
python probe_query_plan.py
python -m unittest -v test_probes
```

The native tests compile the exact checked AST method bodies, classes or loop
statements. Repository-specific wire/schema validation and external services
are outside these isolated algorithm probes. Schemas are taken from the checked
constructor AST. Already indexed/selected records are supplied, and SQLite VM
instructions, comparisons, or descriptor traversals are counted. Source hashes
and output counts are retained. The recovery fixture supplies parsed descriptors
and a test commitment encoder; it does not claim archive wire validation or real
encryption. Its unchanged scan/range loops still check ordering, counts and
payload identities. The pairing fixture uses exact same-context ASCII locators with one valid explicit
shared naming-view identity (all subjects return usable);
the JSON helper is equivalent to JCS for that restricted domain only.

`test_probes.py` contains nine evidence-integrity/output checks, not nine full
product regression tests. Tests confirm source identity, expected outputs,
cycle rejection, exact counts and the two experimental index controls.
No Docker/final-image recovery, complete collection pipeline, locked repository
suite, CI or end-to-end wall-time projection was performed.

## Findings

1. **Journal touched-flag reset** — `packages/riverhog-provenance/src/riverhog_provenance/journal_store.py:564,627`.
   The index on touched is partial (`active=1`), but the UPDATE omits that
   predicate, so the planner scans all accumulated assertions for each frame.
   A journal with a constant number of new assertions/frame incurs a growing
   prefix scan, O(F*A), quadratic when F and A grow together. Adding a full
   touched index in the isolated schema preserves rows/flags and changes the
   probe's instruction growth to approximately 2x per size doubling. Review
   inactive/retracted assertions rather than casually dropping them from reset.

2. **Pending history/snapshot worklists** — `.../history.py:159-167,312-344`.
   Completed rows remain in unindexed `done` tables. Repeated LIMIT 1 scans
   revisit them to find the next incomplete row, O(N^2) with one visit/item.
   Test-only partial pending indexes retain all deduplication state and reduce
   measured VM instructions at N=512 from 413,800 to 25,500. The analogous
   states/views worklists in `canonical_discovery_relevance.py` deserve the
   same regression controls; they are source-audited but not separately counted
   by this package.

3. **Incremental cycle checking** — `.../journal_store.py:397-415`.
   A reverse-inserted sparse chain repeatedly searches an ever longer reachable
   tail. Even an efficient indexed query repeats O(N^2) work. On the audit
   SQLite build the unanalysed scratch table selects the edge relation first,
   indexing only `kind`; each recursive node scans all those edges, producing
   approximately cubic VM instruction growth. N=128/256/512 measured
   3,672,700 / 28,661,100 / 226,470,600 instructions. A diagnostic CROSS JOIN
   order accesses `(kind,source)` and reduces that to
   218,500 / 863,200 / 3,430,300 — still quadratic, NOT a linear solution.
   Preserve cycle rejection for every required snapshot and correction.
   A bounded topological index/affected-frontier design is needed, with explicit
   per-snapshot semantics; no unconditional claim of linear dynamic graph
   validation is made.

4. **Corpus-wide cycle elimination** — `.../corpus.py:187-203`.
   Recomputes zero-indegree sources over remaining edges and deletes sources
   without a source index. A sparse chain or independent pairs already produces
   quadratic work. Use a persisted indegree/ready queue and indexed outgoing
   edges so each edge is processed a bounded number of times, preserving full
   identity/reference/fork checks and cycle failure.

5. **Shared-state lifecycle validation** — `.../journal_store.py:459-489`.
   Each touched usage fetches every usage/invalidation of the same state.
   N untimed usages of one state cause N^2 fetched/decoded related rows even
   though there are only N events. Validate new usages once; when state bounds
   change, recheck each affected state once per snapshot/batch. Preserve temporal
   conflicts, corrections and exact state scope. Cache hits alone do not remove
   this work in fresh/large snapshots.

6. **Filename sidecar matching** —
   `some-implementations/stove0/observers/filename-prefix-sidecars/src/a_stove0_filename_prefix_sidecar_observer/pairing.py:275-308`.
   S qualifying sidecars scan P primaries. N matching unique pairs return N
   candidates after N^2 comparability checks. Index exact naming authority,
   syntax/root/parent and full-leaf/stem keys. Preserve same-context vs explicit
   naming-view semantics, ambiguity, original byte units and all genuine matches.
   Dense genuine matches may themselves be superlinear output; the probe is sparse.

7. **Whole-scope observation subject lookup** —
   `some-implementations/stove0/application/server/src/stove0_core/accepted_observations.py:422-435`.
   For each port subject, search the request tuple from its beginning. One port
   covering N unique subjects visits N(N+1)/2 subjects. Fixed small independent
   batches bound this per call, but do not cover lawful whole-scope requests.
   Index the validated subject set once and retain all exact selection/read checks.

8. **Independent recovery metadata traversal** —
   `some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/_provenance_reader.py:181-233`,
   called by `recovery.py:262-280`.
   Every journal range revalidates the complete descriptor sequence, then scans
   it again. With N single-segment journals, N reads traverse 2*N*(N+1)
   descriptors while returning exactly N payloads. At N=512 the probe has
   525,312 descriptor visits for 512 payload reads. Build an authenticated local
   descriptor index once or stream all journals in one root-qualified pass.
   Keep the recovery tool independent of server/database implementations and
   preserve immutable root/terminal and touched-payload checks.

## Interpreting linearity

N has a different explicit meaning in each probe: frames, pending histories,
assertions/edges, primary-sidecar pairs, request subjects, or journals. None of
these probes measures 128 complete WAV transformations. No claim is made that
all eight dominate the existing scale fixture.

A defensible useful-work objective is linear in required input bytes, unique
records, actual relationships and requested output, with logarithmic indexing
and necessary ordering costs stated rather than disguised. An explicit result
containing N^2 real relationships cannot be emitted in O(N) file-count time.
Parallelism and warm caching are not substitutes for removing avoidable
repeated scans. Resource-bounded disk queues are not inherently time-bounded.

Follow-up integration should honor the existing post-963/964 decision sequence;
this audit creates no new authority or waiver for issue 960.
