# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #964. Convention: #903.
Branch: reference/external/964-large-collection-scaling
Previous reference / direct parent: 5170378d0c24974d67f80377019fe99c54dd8257
Original reference runtime base: main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3
Original base tree: d18b813a489955b085094765f98282ea2e2276d0
G22 design/prototype source: main@152d095038df0c0d0ed642cf265d47b11c65a754
G22 source tree: 991e994457b487a7dca016b7fc0409485cef25c6
Prepared prototype tree: b4ac503c05f9c05ff0c39cb4072ba149dee28d2f
Policy approval: #964 comment 6111028968

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, in this conversation
Published by: connected GitHub integration; commit metadata is publication metadata.

## Integration agent: G22 now has a concrete design

Start with [g22/DESIGN.md](g22/DESIGN.md), then
[g22/RESEARCH.md](g22/RESEARCH.md) and [g22/evidence.json](g22/evidence.json).
This supplies the requested G22 algorithm, source map, candidate cost profiles,
prototype, negative tests and exact integration instructions. It replaces the
older handoff's statement that the G22 design has not yet been supplied.
**G26 remains deferred and unchanged.** No shared-history index representation,
query semantics, generation identity or database migration is selected here.

The key distinction is complete-transcript validation versus truly online answers.
For a complete supplied journal, build disk-backed topological certificates between
corrections, then validate EVERY original prefix in order. Future ranks never grant
future object/reference authority. A cyclic prefix cannot be repaired retroactively.
When no certificate is available, use the tested level/fuel online fallback instead
of repeated unrestricted reachability. Normal provider, prefix/tail, recorder,
profile, temporal, identity and custody checks remain at their owners.

The design explicitly distinguishes:
- append-only complete transcript: linear graph/record/byte passes, plus declared
  canonical/schema and SQLite indexing/grouping costs;
- transcripts with corrections: explicit effective-view repair and epoch costs,
  not universal linearity in the final graph when correction count grows;
- genuine online insertion: the researched subquadratic amortized logical bound,
  with SQL costs and failed attempts separately accounted;
- APIs returning full prefixes: cost proportional to all supplied/returned bytes,
  not a fictional delta-only bound across repeated calls.

No new public schema, acceptance boundary, archive bytes or durable application
state is proposed by the executable prototype. Its SQLite tables are private
scratch projections, not archive authority. It includes touched-index and temporal
extrema fixes so graph improvements are not hidden by those prefix multipliers.

## What to apply and validate

DESIGN.md gives the exact file map. Copy the two private helper modules and two
new test files into the owning provenance package, then check/apply
[g22/integration.patch](g22/integration.patch) to journal.py and journal_store.py.
The patch was checked against 152d0950. The reference branch intentionally retains
its older runtime tree; reproduce or integrate on a separate appropriate checkout.
Reconcile against the exact post-#963 source before landing through the normal rail.

Run the focused suites and [profile_g22.py](g22/profile_g22.py); establish declared
operation applicability through existing Closure/Audit discovery. Native tests are
not a replacement for locked lint/type/style checks, complete local gates, HTTP/
worker/DB/final-image compatibility, recovery, killed-process/capacity tests or
independent exact-SHA CI/CodeQL. Those remain required integration work. The supplied
code is a tested implementation reference, not already qualified mainline code.

No composite claim passes while a reachable delegate is unresolved or incompatible.
The current 64-family inventory remains a proposal; actual formulas and witnesses
must be bound at their owners. This G22 design makes the implementation choices
precise, but does not waive owner review of the final normative cost declarations.

## Research and validation actually performed

Reviewed exact journal-prefix tests, library acceptance, correction handling,
server transport append versus seal, immutable validation caches and leased scratch.
Researched primary online-cycle literature and alternatives; selected the 2019
level-fuel variant for fallback, not a claim that stronger results are impossible.
The authors' Coq proof does not certify this independent SQLite adaptation.

Original provenance suite: 150 passed on untouched 152d0950. Patched suite: 187
passed, including 37 new cases. Four relevant client selections: 21 passed.
All 720 orderings of the six non-self three-vertex directed edges and 12 seeded
100-attempt streams were checked against independent Kahn and rollback invariants.
Native work counters at 16/32/64/128/256 edges show exactly N certificate checks
and N ranking-edge visits. A same-state temporal control falls from 8,512 usage
record decodes to 384 at N=128, preserving results. Full raw counters and environment
qualifications are in evidence.json. Existing required warning policy was retained.

The test environment was constrained, not the locked workspace. It used native
Python 3.13.5 / SQLite 3.46.1 and externally fetched exact upstream rfc8785.py 0.1.4;
no substitute canonicalizer or extra runtime dependency is published. One combined
tool-limited invocation was interrupted without a final summary and is not counted
as a passed aggregate; the recorded frozen-source suites completed separately.

Not performed: full locked gates, full workspace suite, new dynamic Closure,
Docker/PostgreSQL/FTP/scale/final-image or independent recovery qualification,
CI/CodeQL, provider/release operations, machine-checked proof, or a completed
600-second performance witness. No runtime-regression-free claim follows merely
from this reference publication. Validate the actual integrated change.

## Preserved history, authority and publication

The v1 inventory, its canonical digest, G26 decisions, prior probes and source-pinned
raw results are unchanged. Read v1_inventory/DECISIONS.md for approved general policy;
its older G22 future-design brief is now answered by g22/DESIGN.md. Its G26 future
brief remains pending. Older findings must not be relabeled as current-SHA evidence.

#961 owns onboarding, #963 shared evidence, #964 growth, and #469 v1 accounting.
#960's explicit timing-shortfall disposition and maintained 600-second profile are
unchanged. No fixture reduction or hidden semantic limit is introduced.

Only .reference changes. The exact newly published commit recorded on #964 is the
handoff identity; retain every prior reference ancestor. Do not merge .reference
into main. Issue decisions and approved owner contracts remain authoritative; a
reference request, validation result or branch does not create release authority.

The publisher cannot establish native Development/dependency relationships. Markdown
links do not claim those native links. Their prior limitation remains; no main,
release or protected setting is changed by this publication.
