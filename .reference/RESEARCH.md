# Preliminary large-collection research — decision input, not acceptance

Pinned authoritative starting point: `main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3`
(tree `d18b813a489955b085094765f98282ea2e2276d0`). The research
precedes #963 and must be reconciled with the post-#963 baseline. See #964 for
scope and approval authority; this file does not establish public commitments.

## What the observations do and do not establish

The completed 128-WAV scale run reported under #960 at `5a84732e` was
20,469.23 seconds: publication 471, planning 1,516, execution/finalization
18,391, replay 22 (rounded phases need not sum exactly). This source is now
historical. A newer `a71c5335` parent run reported **336 seconds of input
publication and 1,620 seconds of planning**, without qualifying the complete
life cycle. None of these proves the current exact SHA meets the 600-second
profiling objective. The current objective is report-only as a command outcome,
though #960's accepted work must resolve its miss through its own authority.

**A ten-minute 128-file run alone is inadequate evidence for 100,000 files.**
Linear projection of that hypothetical result would require roughly 78 minutes
at 1,000 files, 13 hours at 10,000, and 5.4 days at 100,000. These are
illustrative calculations, *not runtime forecasts*, and omit fixed overhead,
concurrency, content duration, and nonlinear stages. A full 100,000-member
collection of long recordings is not comparable to 128 quarter-second WAVs.

## Source-confirmed growth hazards to investigate on the final baseline

1. **Repeated target output-decision lookup.** The accepted
   `MediaPublicationPlan.decision_for` does a linear scan over the complete
   immutable ordered output-decision tuple in
   `some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/publication.py:89–93`.
   The Opus target calls the method for media and XMP outputs in
   `some-implementations/stove0/targets/opus/target/src/a_stove0_opus_target/target.py`.
   In the existing one-route media shape, `N` WAV inputs yield `2N+1` output
   decisions. **One lookup per decision** entails `(2N+1)(2N+2)/2`
   comparisons: 33,153 for 128 inputs, 2,003,001 for 1,000,
   200,030,001 for 10,000 and 20,000,300,001 for 100,000.
   Actual code may call the lookup more than once per output. An indexed view
   over the already validated exact decision set is a candidate local fix,
   conditional on confirming unchanged identity, unknown-output rejection,
   completeness and deterministic output ordering. It is not an approved
   new contract or an established full-target bottleneck.

2. **Potential dense discovery membership expansion.** In
   `riverhog/src/riverhog_core/canonical_discovery_relevance.py:436–439`,
   `member_relevance()` traverses each selected `closure.snapshots()` and marks
   every assertion under `recorded-history`; `canonical_discovery_build.py`
   stages exact `(artifact, assertion, scope)` memberships. The #960 report
   recorded 608,231 membership links after only 82 members of a larger output
   collection. SQL batches reduce per-link queries but do not shrink the link
   count. If each additional member legitimately selects a growing shared
   documentary set, the expanded index may approach O(N * H), with H itself
   growing with N. **This is conditional**: shared journal co-residence alone
   must not establish member attribution; genuine dense relations may require
   extensive answers. Avoid deleting historical or `recorded-history` scope
   merely to improve an index. Evaluate shared immutable projections and
   exact member-to-snapshot linking as design alternatives; preserve output
   query/pagination, repair/rebuild and committed index identity or obtain an
   explicit protected-boundary decision for any new representation.

3. **Per-member closure construction.** The rebuild path in
   `riverhog/src/riverhog_core/canonical_discovery_rebuild.py`
   constructs a `MemberHistoryClosure` and `member_relevance` for each exact
   bound member. Recent immutable-frame, journal-reference and validation
   caching may reduce repeated parsing without eliminating the retained
   per-member scope work. Determine how many **unique** reference/journal
   validations and scratch bytes remain as the shared-history working set
   grows beyond caches. Do not assume that a cache hit at 128 extrapolates.

4. **Accepted plan and status representation.** A large transform's accepted
   media projection and publication decisions are represented inline and
   validated as complete tuples. Investigate peak plan bytes, canonical
   serialization/hash cost, repeated status copying, restart/claim payloads
   and practical HTTP/database budget at high counts. `#953` owns recipe
   semantics and `#948` owns execution design; a paged accepted plan is only
   a hypothesis and may require an explicit contract/identity decision.

5. **Cryptographic and publication work.** Each new independently encrypted
   object legitimately incurs distinct work and durable receipts. Measure
   *unique* cold key derivations, ciphertext objects, remote contacts, repeated
   validated prefixes and work per sealed journal. Preserve age strength,
   unique salts, live authorizations, ordering, restart fences and independent
   recovery. Concurrency may reduce wall time but cannot reduce necessary
   bytes and may exhaust RAM unless governed by shared resource budgets.

6. **Qualification fixture versus product limits.** The current
   `scripts/_processing_fixture.sh:13–15` restricts the *test* to at most
   1,000 audio files. This is **not** a product contract ceiling.
   A 10,000/100,000 proof requires an approved high-cardinality fixture or
   separate construction path without changing the maintained 128-file
   qualification, hiding limits or relaxing exact completeness.

## Reproducible bounded source check

`.reference/probe_scaling.py` parses the exact source, verifies the scan and
recorded-history traversal shapes with AST guards, and executes **only the
unchanged two-statement decision lookup** with counted comparisons. It also
constructs separate hypothetical incidence counts for fixed 32-per-member
shared assertions versus N-per-member shared assertions. It does not invoke
Riverhog services, read archive data, simulate authority, or measure 100,000
member wall time. `.reference/SOURCE_PROBE.json` retains native results;
`.reference/test_probe_scaling.py` enforces the known source shapes and
unknown-decision rejection. Measurements at 128/512/1,000/2,048 cases use
this environment's Python; comparison counts are exact, elapsed seconds are
not transport, database, or final-image timings.

The two incidence models are **illustrative upper/shape comparisons only**:
N * 32 vs. N * N selected links. Neither model infers the actual number of
links or their legitimate scope in a 100,000-member collection. A real
benchmark must measure member-history and recorded-history cardinality under
an exact scenario with query-result equivalence.

## Candidate workload taxonomy for research, NOT approved numerical targets

See `.reference/WORKLOADS.json` for a machine-readable *research input*,
not a maintained workload/target registry. One dimension must vary at a time:

- **Generic custody:** 0-byte/tiny/mixed files, equal-content distinct
  members, 128/512/1,000/10,000/100,000-member populations, one collection.
- **Sparse provenance:** independent short histories, a fixed small number of
  recorded relationships per member; full archive, index and recovery.
- **Shared history:** fixed and growing shared immutable journal/closure
  structures selected by many members, while keeping legally attributable
  scope distinct from shared physical co-residence.
- **True dense graph:** explicit lawful cross-member causal relations;
  measure against required output relationship cardinality E, not solely N.
- **Processing:** deterministic tiny WAVs (current 128-file anchor),
  heterogeneous media, real-duration recordings, sidecars, output expansion,
  two-producer/fork cases only where semantically required.
- **Read/recovery:** cold complete inventory, scoped single-artifact and paged
  discovery, archive-only rebuild, retained-root independent recovery,
  disconnect, cancellation and restart with minimal replay.

For each, distinguish file count, bytes, duration, history depth, unique
snapshots/assertions, output count, query result count and selected memberships.
Run a ladder of cheap, bounded component checks first; only then schedule
larger exact-image end-to-end proofs with adequate host/disk/time resources.
The 100,000-member case must ultimately have an approved, executed *complete*
proof—component models alone cannot close the issue.

## Measurement method after #963

Use #963's **one** executable authority for approved maintained workloads,
targets and execution conditions. Exploratory scripts do not register or
implicitly approve performance thresholds. Retain a complete source SHA,
clean-source status, image digests, workload identity, engine/tool versions,
CPU cores/cgroup memory, storage topology and rate conditions. Record real
completion, cancellation and failures; never count a timed-out run as success.
Use at least three comparable completed samples to establish or revise a
numerical target; retain all attempts, including failures.

Collect per-phase wall/CPU and high-water RSS/scratch; unique encrypted
objects/ciphertext bytes, scrypt work, storage contacts; SQL statement counts,
transaction durations and staged rows/bytes; selected histories, journal bytes
rehashed, immutable cache misses/evictions; index links and query-result
sizes; plan serialization/hash time, checkpoint bytes; API contacts and
latency; resume replayed units; background-job progress during the run.
External queue delay and image preparation are reported independently, but
must remain visible in complete-run cost. A latency target is not a safety
or correctness deadline. A valid miss remains report-only; missing or invalid
required evidence is a validation failure.

Characterize growth as a function of **actual unique work and result output**:
compare adjacent-size elapsed, CPU, peak resource, SQL operations and stored
index size normalized by N, bytes, unique selected history and legitimate E.
A quadratic relation count may be warranted by an explicitly dense query;
unnecessary multiplication of shared entries is not. Check expiry/restart,
small-job fairness, mutable grant/claim fences and independent recovery under
real backpressure. Do not remove existing proof from ordinary required gates
until the maintainer explicitly approves any revised cadence.

## Decision checkpoint and likely architectural tradeoffs

**Do not implement broadly yet.** First reconcile against the post-#963
mainline and the eventual #960 disposition. Present maintainer choices:

1. Supported workload families, cardinality envelope and acceptable behavior
   under finite resources (no hidden total limits).
2. Exact milestone completion and timeout conditions; whether discovery
   readiness is part of each workload's measured completion.
3. Approved hardware/resource profile and numerical latency/throughput/peak-
   storage targets for *each* workload, supported by comparable runs. Unknown
   values remain unresolved rather than guessed or changed to fit a run.
4. Accepted evidence cadence: inexpensive component size-growth tests on each
   SHA; bounded representative larger tests at integration; justified
   resource-intensive full runs under release or a dedicated qualification
   rail. No existing required proof is deferred automatically.
5. Owner-level architectural selection: indexed decision lookup vs. accepted
   plan representation; shared immutable discovery scope vs. expanded rows;
   caching vs. safe sharing; bounded crypto concurrency; reversible/rebuildable
   index schema changes and their protected-boundary implications.

Approval of this research method is not approval of any concrete design,
public contract, mutable-state replacement, or new target. #963 must not be
made responsible for these product decisions; #960 must not be indefinitely
expanded by them.

## Known limits

This is source analysis and a stdlib microprobe against a pinned checkout.
No 10,000/100,000-member end-to-end run, Docker/PostgreSQL simulation,
release/provider qualification, or measured source-equivalent comparison of
128 and 100,000 was executed. Historical #960 timings are bound to their
historical SHAs and not reused as performance evidence for this branch.
Model calculations are not a substitute for actual durable complete work.
