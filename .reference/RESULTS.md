# Findings and next action

The evidence supports a focused next slice: **account for one complete small
journal through the real stamped server-validation caller**, including every
service-to-Native transition. It does not yet identify a safe optimization or
establish whole-path growth. See [NEXT_SLICE.md](NEXT_SLICE.md) for the exact owner,
existing tests, missing caller census and finite deliverable. This follows the
new [integration checkpoint](https://github.com/nashspence/riverhog/issues/991#issuecomment-6854556220)
instead of commissioning another publication timing ladder.

## Source and evidence relationship

The audited implementation is `8ce49de94c32ae39562aab5ea4178b1dc8c75e77` on PR #979,
the [accepted #990 integration checkpoint](https://github.com/nashspence/riverhog/issues/964#issuecomment-6854178345)
within #964, where #991 owns this investigation. Product files in this reference
are unchanged from that source. Relative to the previous `2eb7e48...` reference,
the only changes outside `.reference/` are the PostgreSQL qualification test and
its script entry; runtime product bytes match. This equality does not equal
matching execution conditions. [Source audit](evidence/source-audit.json)

Historical observations are retained as [normalized evidence with source links
and limitations](evidence/prior-observations.json). The earlier 16-member
241.949-second publication and the 785.029-second observation differ in source,
worker progress and concurrent load. They are not a measured regression. The
previous failed 1,000-member arm did not reach the three reader passes, and the
stalled thread-entry profiler was an invalid diagnostic; neither becomes a
successful sample. The serial 1/2/4 results are issue-reported summaries here,
not newly executed or independently inspected raw sampler evidence.

## Findings that change the investigation

**Native transition multiplicity is concrete, but purpose and avoidability still
need attribution.** Both the integration owner's latest one-member checkpoint
and this reference's separate probe record **10,241 journal advances** and
**40,751 prefix checks** across publication and all three exhaustive reader
passes. This probe records 20,541 ledger loads and 10,063 fences. Each successful
load/fence invokes `_read` once at this source, yielding **30,604 derived
checkpoint reads**, consistent with the owner's directly reported count. These
are different scopes: not every Native advance belongs to a server validation
unit, and neither count is a proof that a check can be removed.

**The existing unit evidence leaves a specific caller-composition question.**
The service-unit and stamped-caller growth tests intentionally fast-forward the
Native fixture between selected measured service boundaries and assert three
measured owner calls. They establish useful local envelopes, but not the cost of
calling the complete service prerequisite/current-fence chain for every
transition of an actual journal. The new checkpoint reports 400 server validation
units. Attribute the full cadence before classifying repeated probes or fences
as necessary or avoidable. [Exact source and tests](NEXT_SLICE.md)

**Stored-object read counts include an explicit exhaustive-observer floor.**
The observer first reads each archive object, then whole-object checks read it
again through `iter_archive_object`; multipart checks consume their first read.
For the previous 16-member receipt, `2 * 110 + 16 = 236`, exactly the warm-pass
read count. This one-member probe independently has 20 whole objects and one
multipart object: `2 * 20 + 1 = 41`, again exactly the warm-pass count. This
explains the read floor, not publication time. In this small encrypted in-memory
fixture with scrypt factor 1, the three ciphertext-check subscopes consumed only
0.016 seconds of measured thread CPU, while the inclusive verification owner
scopes consumed 31.568 seconds. The remaining verification work deserves Native
owner attribution; these values do not characterize a real storage provider.
[Source mechanisms](COST_MAP.md)

**A warm byte cache does not imply reused semantic validation.** Each exhaustive
reader pass creates `2N+1` history-closure owners in addition to validating all
physical journals. Those owners have separate worklists and bounded caches;
current provider/read fences remain intentional. Server finalization also
validates each selected shared root per attached member. A fanout × selected
prefix term is a source-grounded hypothesis to measure and reconcile with the
declared logical-history obligation, not an already-proven growth defect.
[Lifetime and multiplicity map](COST_MAP.md)

**Transport, preparation and concurrency need their own accounting.** This
fixture uses POSIX prepared-primary storage, ordinary and admitted Native
SQLite, a catalog, and per-request TestClient machinery. It appends one member
at a time and manually drives worker coordinators. Configured capacity cannot
create independent ready work. A retained execution slot is also distinct from
an active worker or a reusable slot. [Concurrency and ownership map](CONCURRENCY.md)

## Latest integration checkpoint, preserved separately

The integration owner posted the following while this reference's one-member
probe was already in flight. It is current issue-reported evidence on the same
implementation source, with a different observer and execution conditions (12 versus nine reported
CPUs); physical-host equivalence was not established. Its raw receipt was
not supplied here. No further publication run was launched after reconciliation.

| Observation | Integration checkpoint |
| --- | ---: |
| Publication / complete diagnostic | 32.097 / 42.466 s |
| Process CPU / selected exclusive thread CPU | 35.799 / 31.917 s |
| Server validation units / catalog executions | 400 / 47,435 |
| Physical executor invocations / maximum active | 74 / 1 |
| Cumulative physical execution wall / CPU | 11.140 / 8.617 s |
| Readiness transition calls / actual readiness check | 202 / 1 |

The owner's largest selected exclusive CPU scopes were journal advance 9.536 s,
server validation unit 5.363 s, checkpoint read 3.127 s, finalization pipeline
3.035 s and prefix check 2.650 s. Those scopes retain unselected child work.
Their CPU must not be added again to process CPU, and cumulative worker wall
must not be added to elapsed critical-path time. The reported 12 available CPUs
and one active invocation do not show productive idle capacity: queue depth,
refusal reasons, lock waits and independent ready work were not measured.
[Owning-issue record](https://github.com/nashspence/riverhog/issues/991#issuecomment-6854556220)

## This reference's targeted diagnostic

One real 65,536-byte member, F1/C1/K2/Q1, ran serially on clean audited source
through unchanged `make profile`, with a 300-second finalization resource
condition. There were no sibling qualification jobs launched by this producer.
Host idleness and effective CPU quota were not established; the environment
reported nine CPUs/affinity entries and exposed no `cpu.max` file at the inspected
path. Instrumentation and imports are explicit conditions. Full exact output,
all fresh/warm/evicted reader checks, source/upload removal, original helper
receiving and profile-workspace cleanup passed.

| Coarse helper phase | Wall seconds | Process CPU seconds |
| --- | ---: | ---: |
| Publication setup before append | 4.258 | 4.149 |
| Append one input | 26.708 | 24.781 |
| Receive primary history | 1.119 | 1.114 |
| Supplemental/shared history preparation and staging | 15.416 | 13.743 |
| Finalization through fixture receiving | 46.299 | 44.423 |
| Fresh verification | 6.770 | 6.596 |
| Warm verification | 9.898 | 9.544 |
| Evicted verification | 15.694 | 15.428 |

The maintained publication boundary was **93.801 s** and complete profile
**136.777 s**. The observer window was **126.361 s**, with **119.885 s process
CPU**, **80.465 s covered exclusive thread CPU**, and **39.420 s unassigned
process CPU**. Its earlier startup and later outer receiving/serialization are
outside that window. Bootstrap including calibration took 7.774 s and partially
overlaps recorder startup; do not add it to the window as if disjoint.
[Summary](evidence/one-member-summary.json), [maintained profile](evidence/one-member-profile.json)

| Observed call scope | Count | Interpretation |
| --- | ---: | --- |
| Journal validation advance | 10,241 | Across publication and three reader passes |
| Ledger load / fence / publish / prefix | 20,541 / 10,063 / 10,063 / 40,751 | Nested owner calls, not additive leaf work |
| Catalog cursor execution attempts | 50,968 | Includes 1,172 raised cursor attempts; error families were not recorded |
| Admitted Native cursor execute / connection commit | 8,639 / 427 | Does not cover ordinary disposable SQLite |
| TestClient request | 639 | Inclusive request time includes delegated work and waits |
| Physical-execution thread lifetimes reaching hooks | 74 | Not a measurement of simultaneous activity or complete worker CPU |

The recorder observed 726 thread lifetimes reaching selected hooks, including
640 AnyIO worker lifetimes. These are neither all created threads nor concurrent
active counts. The source's per-request transport construction makes that churn
a specific fixture-cost lead; this probe did not measure portal construction or
prove its dominance. Current readiness work is not presumed repetitive merely
because the transition caller runs often; the integration checkpoint's one
actual census is retained above.

The recorded Native-advance exclusive CPU was 27.770 s; ledger prefix 8.274 s,
load 7.591 s, publish 4.194 s and fence 3.281 s; catalog cursor execution 3.898 s.
These are exclusive only of selected same-thread children. They can include
ordinary SQLite, Python processing and unselected descendants. The remaining
39.420 s is not allocated to SQL, locks, waste or any guessed operation.

### Measurement limits that affect interpretation

- The probe's **2.324 s of recorded bookkeeping CPU is partial observer cost**,
  overlaps selected parent scopes, and is not a throughput correction. Empty
  call calibration and raw observer data are retained. No matched uninstrumented
  reference was run, so the 93.801 s value is not a regression or speedup claim
  against the integration owner's 32.097 s.
- Catalog execute events omit fetch/materialization, pool/connection work and
  commit. Admitted Native cursor counters omit ordinary disposable journal,
  closure and worklist SQLite; broader owner spans can contain those calls.
  Statement count is not engine rows visited or VM work.
- The 1,172 raised catalog attempts were handled on a successful product path.
  Their SQL/error classification was intentionally not captured. They remain an
  attribution question; do not silently assume every one is the same contention.
- Same-thread exclusive wall is not an overlap/critical-path trace. CPU by
  measured scopes is not whole-thread CPU. No queue time, precise lock wait,
  active/idle worker timeline or independent-ready-work census was produced by
  this probe.
- Phase clocks are process-wide; spans crossing phases are charged to their
  start phase. The broad owner rows must not be used as a per-phase CPU sum.
  Preparation/staging and finalization/fixture receiving remain coarse groups.

The complete raw observer record is compressed losslessly in
[one-member-attribution.json.gz](evidence/one-member-attribution.json.gz), with
compressed and uncompressed hashes in [manifest.json](evidence/manifest.json).
No overflow or unfinished span was recorded. [Validation and reproduction](VALIDATION.md)
describe what was actually checked.

## Disposition

The reference advances source explanation and gives a reproducible, explicitly
limited independent count witness. It supplies no production optimization,
complete composed-growth proof, concurrency speedup, or large-size
qualification. Use [NEXT_SLICE.md](NEXT_SLICE.md) now; use the broader
[measurement method](METHOD.md) and [contrast menu](EXPERIMENTS.md) only when the
selected owner needs them. #991/#964 retain the current priority and large-trial
disposition, without any new numerical speed gate.
