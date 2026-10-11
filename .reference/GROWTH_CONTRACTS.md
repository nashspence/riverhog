# V1 growth commitments: declaration, implementation and evidence plan

Authority: #964 comment 6104002488; coordination: #961 comment 6103986307,
#963 comment 6103991666, and release qualification #469 comment 6104044650.
This document is non-authoritative integration input under #903. It supersedes
older reference instructions that treat passing a large cardinality or approving
numerical seconds targets as the definition of scalability. It does not claim
current runtime conformance or approve a particular exception or representation.

## 1. Keep three independent obligations

Functional correctness, growth conformance, and observed elapsed time are distinct.
Required functional or growth failures block validation. Missing required evidence
is an error. A valid elapsed-time miss stays diagnostic, with its required review
and maintainer disposition. The #960 closeout exception remains in force for its
identified timing shortfall; it neither certifies a growth bound nor reopens #960.
Keep its current profile, interval, fixture and 600-second target unchanged.

#961 states the repository working rule. #963 schedules work and carries evidence.
#964 establishes operation-owned v1 declarations and makes the implementations meet
them. #469 accounts for those declarations and witnesses before the first v1 tag.
No reference document, fitted curve, green aggregate or generated report creates a
normative declaration. Approved owner declarations are the source for any released
machine/human contract view; do not add another master complexity registry.

## 2. Define an actual operation and cost model

For each in-scope operation identify:

- Owner and external operation/contract identity; precise start, completion and
  successful result. State whether the bound is per call, complete lifecycle,
  amortized sequence, static input or online mutation sequence.
- Independent dimensions: input/output bytes B, members N, encoded metadata H,
  necessary vertices/edges E, and genuinely requested result size R. Include key,
  path and record lengths where variable; do not assume arbitrary strings are
  constant-sized. State workload-family constraints, including sparse/dense cases.
- Cost measures: bytes visited/copied/hashed/encrypted, comparisons, graph visits,
  database rows/index work, request counts, checkpoint/replay work and necessary
  derived state. A single API/SQL call is not a unit-cost substitute for its scan.
- Bound type and assumptions: worst-case, amortized or expected; cold/warm/bypass;
  fixed concurrency/window and cryptographic configuration; operator capacity;
  finite failure/retry model. State underlying index/order costs honestly.
- Memory, scratch and retained-state bounds independently of total work. Disk-backed
  quadratic work is not acceptable merely because RAM is bounded. A whole-collection
  dictionary is not automatically a bounded-memory fix for a streaming interface.
- Concise algorithmic accounting, owning executable witnesses and narrowly reviewed
  exceptions. Internal test coefficients are reviewed regression budgets, not
  frozen seconds/calls-per-file promises unless separately declared externally.

Linear is the default for ordinary data streaming and collection traversal.
A typical accounting is W <= aB + bN + cH + dE + eR + f for the named counters,
where constants and dimensions correspond to the algorithm rather than a fitted
slow baseline. This is a schema for reasoning, not one preapproved universal bound.
Index/order maintenance and dynamic graph mutation may require another precisely
approved bound. Do not hide those costs or introduce a blanket near-linear exception.

Necessary input/result dimensions cannot include an avoidable member-by-shared-
history expansion manufactured internally. Conversely, explicitly requested dense
relationships cannot be omitted to pretend work is linear in file count alone.
Component/codec-owned transform complexity remains separate from generic archive,
transport, planning, status and preservation overhead.

## 3. Initial coverage to establish with owners

- Single large file: payload upload, retrieval and independent recovery scale with
  bytes; buffers and retry windows do not require the full file in memory/spool.
- Many members: whole ingest/append/finalize sequence, member identity, metadata,
  cold cryptographic-object work and finalization. Do not validate only one append.
- Provenance: static journal/corpus graph validation, correction/retraction, shared
  histories and exact selection. Specify online acceptance separately from static
  topological validation; per-edge traversal is not automatically amortized linear.
- Inventory/discovery/association: complete traversal and requested output,
  source attribution versus recorded history, stable ordering/pagination, indexed
  lookup and shared-history representation without unnecessary full expansion.
- Processing: exact accepted selections, execution-plan traversal, status reads,
  publication decisions, bounded progress and work distribution; codec costs stay
  with their owner. Scheduling concurrency does not change aggregate work class.
- Recovery/rebuild and interruption: independent tool behavior, final root and
  terminator verification, stage checkpoints, cold reads, cleanup and exact replay.

Approve exact scope, bounds, assumptions, exceptions and proof cadence before broad
integration. Do not label unestablished obligations as passed or invent numerical
speed targets to satisfy this approval checkpoint.

## 4. Evidence: reasoning plus executable witnesses

Finite samples corroborate the bound; they cannot establish asymptotic behavior on
all larger inputs. Use source-linked operation counts at increasing sizes and a
reviewed accounting explaining why each counted unit is visited a bounded number
of times. Curve fitting and a log-log slope are diagnostics, not pass criteria.

Witnesses run the maintained implementation, not a rewritten benchmark algorithm.
Check exact complete outputs and errors alongside budgets. Count work where it is
performed: scanned rows rather than just statements, decoded bytes rather than a
wrapper invocation. Keep counter overhead bounded and separately understood.

Use adversarial shapes: forward/reverse chains, disjoint edges, many usages of one
state, sparse and truly dense graphs, one sidecar per primary, ambiguous/no-match
names, equal-byte distinct identities, shared versus independent history, and
successive append prefixes. Test multiple sizes beyond fixed startup cost.

Cover first use, warm use, eviction, capacity bypass, page/segment/pack transition,
retraction, late authority revocation, failed/partial writes, restart and replay.
Configured resource budgets may explicitly delay/reject work, not truncate it.
Do not raise a test's coefficient to absorb a regression without owner review.

The existing source-extracted probes are historical counterexamples. Translate
accepted fixes into product-level positive/negative regression tests and include
the whole control/data path where its composition determines the bound. Preserve
the original raw evidence and record source drift rather than weakening guards.

## 5. Qualification by distinct regimes, not arbitrary huge runs

The 1k/10k/100k envelope guides design and investigation; it is not a mandatory
full-media size ladder or an implicit supported-total ceiling. Choose inexpensive
small/moderate fixtures that expose each algorithm and materially distinct path.

Cross boundaries with supported page/window/pack/cache settings where semantically
equivalent. Never add a test-only production fast path. Generated streams avoid
huge fixture storage, but a simulated terabyte counter is not real terabyte
crypto/storage evidence. Keep a compact real final-image end-to-end witness.

Retain selected larger tests only for actual DB planner, working-set, storage,
platform or integration behavior not established more directly. Record why each
large test exists, its resource budget and cadence at the owning acceptance surface.
Reducing a historically required proof requires reviewed equivalent coverage; this
reference does not remove #960's 128-file functional qualification.

## 6. Prioritize fixes without conflating layers

Use SUPERLINEAR_INTEGRATION.md for the earlier nine local algorithm candidates and
conditional shared-history expansion. It is diagnostic input at 8425e672, not a
proof that every finding is still present on the eventual integration source.

Narrow indexes/lookup corrections need exact-output/error/correction/retry parity.
For graph validation, remove repeated tail searches rather than merely choosing a
faster quadratic query plan. For shared history, examine representation and query
semantics together rather than optimizing millions of unnecessary membership rows.
Protect independent recovery; do not import server implementation authority.

## 7. Integration and acceptance

1. Land the amended #961 guide with integrity and scoped policy tests.
2. Establish #963's shared evidence handling. Keep declared, applicable growth
   witnesses and expected coverage distinct from timing-target applicability.
3. Reconcile source findings against the exact candidate, approve operation-level
   declarations/assumptions and any exceptions, then implement at each owner.
4. Verify deterministic work budgets, growth-regime boundaries, functional outcomes
   and lifecycle composition. Retain selected real integration/recovery witnesses.
5. Project only actual owner declarations into the existing release contract and
   witness machinery. The specific G26 derived-index identity hard cut is now
   approved (6112668478): **no schema migration or backward-compatibility
   support**. Preserve unrelated protected-state review and archive custody.
6. Run applicable locked local and independent exact-SHA GitHub qualification.
   Report functional and growth results separately from times. Missing growth
   proof or a declared violation blocks v1; a timing waiver cannot erase it.

This is a coordinated #963/#964 effort, not a requirement for #963 to solve every
algorithm first. The evidence foundation precedes dependent acceptance; #961 is
still the onboarding prerequisite. No runtime fix, fresh profile or universal
linear-growth guarantee is supplied by this reference revision.
