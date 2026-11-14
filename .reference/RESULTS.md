# Private projection construction: measured reference

## Finding

The helper proposal preserved the complete receiving workload and all private inputs. In one uninstrumented retained helper pair, wall time changed from **14.281960105 s to 11.883021960 s** (16.80% lower), and CPU time from **14.222222806 s to 11.858247503 s** (16.62% lower). This supports consuming the small helper change for review. It is one ordered observation, without repetition, variance estimates, a numerical gate, or a production throughput claim.

The diagnostic companion pair observed **46 deep copies becoming zero** and private-construction CPU changing from **2.951598833 s to 0.765594347 s**. Every reader still parsed and validated afresh. Construction counts describe work; fresh decoding still constructs the required mutable dictionaries and lists. No allocator, peak-memory, tracemalloc, or RSS measurement was made.

Primary evidence: [original replay report](evidence/replay/run.json), [replay lane](evidence/replay-lane.json), [original execution receipt](evidence/private-projection-replay-execution.json), and [verification report](evidence/verification/run.json). [Archive provenance](evidence/archive-provenance.json) maps original files to unchanged archived bytes. [Manifest](evidence/manifest.json) pins the final evidence files.

## Exact source and implementation

- Audited receiving checkpoint: `67b4ccd9419be86a60dece5c5c16bee43f7ce935` on `codex/964-session-owner` / PR #979, explicitly specified in [request 6884822328](https://github.com/nashspence/riverhog/issues/991#issuecomment-6884822328).
- Exact clean implementation used for every test, gate, and replay phase: **`7d2aebb21891bced44a703753040f91d9a351f0c`**, tree `f7db5c9f64f7a878339dea94761fe024c33ee990`, directly parented by the audited receiving checkpoint.
- The final reference adds evidence and updates this handoff under `.reference/`; its helper/test blobs remain identical to that tested implementation. The exact final reference commit is recorded in the owning #991 comment.
- Baseline helper SHA-256: `87ccc6657a2a3dbab1bf74652aea86a9495a316be2ded7808836f9bd4cf58b5f`. Proposal helper SHA-256: `335a6cc036caf987dbf3585713411a38bdde3bd786ad369dc7b1c56a38046ec8`.
- Reader SHA-256: **`a6fa11e73a3886dbf950bba34e310ab27857c5e87af6ca77bb6cad756a059f40`**, byte-identical to the receiving checkpoint. The accepted lookup/numeric reader behavior, tests, canonical implementation, dependency locks, and actual caller/product paths were unchanged.

Only two executable files differ from the receiving checkpoint: `tests/support/growth_batch.py` and `tests/unit/test_growth_observer_batch.py`. The helper diff is 16 added / 14 removed lines. There is no production implementation change in this reference.

The baseline replay imports the exact helper source from the audited Git object into a temporary module and temporarily selects its class/aliases in the candidate checkout. The proposal replay selects the candidate helper. All other executed implementation paths are identical to the receiving checkpoint. This isolates the requested helper change; it does not claim that a full historical checkout was separately qualified.

## Representation and bounds

The baseline retains a canonical encoding of the whole admitted projection. Each populated flush decodes that union, and each operation group deep-copies its complete reachable mapping. The proposal retains a 112-byte encoded header with an empty declarations mapping and one canonical byte encoding for each admitted declaration. Admission reuses those exact row bytes for the existing per-operation declaration digest. Each group freshly decodes the header and its complete reachable rows using the original captured-source traversal.

Retained representation is proportional to admitted encoded metadata plus admitted bindings/delegate edges and the existing bounded pending-root state. There are no retained decoded per-owner projections, overlapping closure copies, compiled contexts, accepted results, or numeric/unit result caches. Declaration IDs select row encodings; exact operations retain their separate authority through `_sources` and `_roots`. Initial source validation continues to reject multiple source owners for one declaration ID.

[Static source audit](evidence/source-audit.json) confirms that `source`, `_current`, `sample`, `receive`, and `__exit__` are unchanged. Initial full projection validation, full source-content verification at receiving boundaries, exact source binding/operation/declaration/delegate identities, explicit descendant admission, sample seals, poisoning, and release remain intact. The existing reader budgets and canonical semantics are unchanged.

## Retained workload and equivalence

The archived inputs came from public helper reference `5e21cc9c75c86ec08ab4b1c755d98c36bd4e5195`; no new actual caller turn or product operation was created. The [adapted replay](evidence/replay/replay-script.py.txt) derives from its frozen `4bec4b8c…025ba2` harness. Its timer/action-builder logic was preserved; the executed legacy routing was removed and both compared helpers use the maintained extended route.

Each phase uses root limit **64**, explicit descendant admission, the same **830 retained events / 148 dispatched actions**, one passive Native metadata constructor, **16 actual Core sample emissions**, and **eight actual observer setup/completion lifecycles**. Primitive wrappers are installed/restored with `recording=False`; literal primitive observations are replayed. The Native pause scope is empty. Product bodies, fresh archive fixtures, journal advances, and publication are absent.

| Work, per diagnostic phase | Baseline | Proposal |
| --- | ---: | ---: |
| Source projections / admitted declarations | 1 / 100 | 1 / 100 |
| Independent completed root occurrences | 122 | 122 |
| Sample-node checks, **including roots** | 1,078 | 1,078 |
| Fresh private groups / public reader calls | 46 / 46 | 46 / 46 |
| Fresh compilations / declaration occurrences | 47 / 929 | 47 / 929 |
| Unit visits | 560,860 | 560,860 |
| Canonical encoding calls | 1,923 | 1,923 |
| SHA-256 constructions | 1,922 | 1,922 |
| Digest calls / hex digest calls | 844 / 1,078 | 844 / 1,078 |
| Source lookups / whole-source checks | 411 / 5 | 411 / 5 |
| Private deep-copy calls | 46 | 0 |
| JSON decoding | 2 whole unions | 46 headers + 829 declaration rows |
| Total canonical private input bytes delivered | 30,849,497 | 30,849,497 |

All nonconstruction counters match exactly. The two diagnostic phases compare the actual private byte lists directly, plus each group's complete reachable closure, declaration order, sample-token order, and actual object identity at both reader and compiler. Mutable trees are disjoint across every group and from the source projection and baseline decoded union. The original 46-group receipt matches exactly. No independent child root or repeated nested obligation is removed.

The source union remains `f8eae73e293eac5e9ed7dcf01d706d9176d4f198e13ef61203d06d0df41c0ef1` (4,512,672 canonical bytes). Samples remain `a9e69163ba9aac6c5cefd4f582c609997217421528840694449f24007f6c2b20`; events remain `5e559ea611938996d6b47df6588e4a9f2fb7c5f5623a0942b13cf1f144333a25`. The prior helper replay receipt is `b62367b740cf428e5c7d39f8233237e63ca7ed226f65951b926de9e91cb6a844`.

## Timing and remaining cost

One diagnostic baseline/proposal pair ran first, followed by one uninstrumented baseline/proposal pair, sequentially in the same process and pinned environment. No further replay attempts or workload campaigns were run. Imports, baseline module construction, input decoding/copying, canonical equality checks, mutable-tree inspection, report serialization, and product execution are outside the timed intervals. Plain timing includes the helper lifecycle and the maintained replay dispatcher without the diagnostic hooks.

| Uninstrumented observation | Baseline (s) | Proposal (s) |
| --- | ---: | ---: |
| Wall | 14.281960105 | 11.883021960 |
| CPU | 14.222222806 | 11.858247503 |

The earlier 6.947-second integration observation is not this comparison's baseline and was not used to calculate an improvement. Fixed ordering, one observation per helper, and different allocation/collection pressure limit attribution. This is an elapsed observation of this retained helper workload.

The following are **exclusive diagnostic CPU intervals**, with clocks, counters, hook bookkeeping and retained-reference overhead. They must not be added to or used as an allocation of the uninstrumented CPU values.

| Diagnostic category | Baseline CPU (s) | Proposal CPU (s) |
| --- | ---: | ---: |
| Source declaration projection, excluding compilation | 0.333463461 | 0.040352950 |
| Fresh parsing and projection compilation | 6.419587936 | 5.874299242 |
| Private projection construction | 2.951598833 | 0.765594347 |
| Source checks, declaration digests, and sample seals | 4.692366650 | 4.079850454 |
| Sample bounds and delegate composition | 1.238165339 | 1.119449722 |
| Primitive observer setup and teardown | 0.002336305 | 0.002141550 |
| Observer orchestration and replay dispatch | 0.011176369 | 0.010401149 |
| Total diagnostic CPU interval | 15.648712424 | 11.892104034 |

Fresh parsing/compilation and source/digest/seal verification remain substantial costs, with unchanged counts. The source/digest category includes admission row encoding, full source checks, sample seals, and per-node declaration encoding/hashing. Private construction includes union/header envelope encoding, JSON decoding and complete private materialization. The dispatcher has its own explicitly named timer; it is not mislabeled as digest work. Nonconstruction timings vary despite identical work counts, so their movement is not attributed to the proposal. Scoped timer totals have boundary overhead and are not an independent throughput measurement.

## Construction and encoded payloads, separately from memory

The baseline retains **one 4,512,672-byte union buffer**. The proposal retains **100 row buffers totaling 4,506,145 bytes plus one 112-byte header**, or **4,506,257 encoded bytes** in total. These are exact byte payload lengths, excluding Python object, mapping, key and reference overhead.

The baseline decoder processes 9,025,344 bytes across two union decodes and then performs 46 deep copies. The proposal decoder processes 30,791,171 row bytes plus 5,152 header bytes across 875 decodes. All 30,849,497 canonical private projection bytes still reach readers. The change replaces Python copying with fresh JSON decoding; it does not eliminate the required private structures.

Canonical encoding calls stay at 1,923 while their total encoded bytes change from 71,790,792 to 67,278,232: the large union envelope encoding becomes the 112-byte header. Existing row encodings supply the existing digests and the new private-row source together.

Diagnostic passes deliberately retain private/source/decoded references for post-timing isolation checks. Those references do not carry into the plain timing passes. Neither these diagnostic trees nor the encoded byte lengths are peak-memory, RSS, or allocation measurements. Structural linear boundedness follows from the retained representation, not from a claimed memory benchmark.

## Focused adversarial validation

**82 selected cases passed: 37 batch cases and 45 lifetime cases**, with every setup/call/teardown phase passing and no expected failures. The strengthened tests retain and mutate every nested dictionary and list across overlapping groups, two flushes, and both group orders; verify complete unused metadata and fresh parsing; require separate source admissions for identical rows; and check unused-source mutation at empty exit both before any work and after a successful flush. Shared release assertions cover the new header/row storage on existing success, failure, poisoned-scope and exception paths. Existing source callback/mutation, exact seals, descendant admission, independent child obligations, and invalid sample checks remain in the selected coverage.

This is **partial verification of the batch test module**. The following actual caller fixture tests were explicitly deselected and did not run:

- `tests/unit/test_growth_observer_batch.py::test_actual_caller_and_unit_turns_share_a_bounded_observer_batch`
- `tests/unit/test_growth_observer_batch.py::test_actual_caller_and_unit_turns_receive_every_descendant`

| Check | Original / wrapper exit | Maintained lane elapsed (s) |
| --- | ---: | ---: |
| focused-tests | 0 / 0 | 10.193783 |
| ruff | 0 / 0 | 0.569537 |
| format | 0 / 0 | 0.381613 |
| reuse | 0 / 0 | 1.668396 |
| mypy | 0 / 0 | 49.054292 |

The pytest controller records 7.318676603 seconds; its console reports `82 passed, 2 deselected in 7.31s`. Lane time includes its maintained wrapper scope. Direct pinned equivalents of the lint tools were used, with the exact Python/uv/dependency versions checked against `mise.toml` and `uv.lock`: Python 3.12.3, uv 0.11.24, pytest 9.1.1, Ruff 0.16.0, REUSE 6.2.0 and mypy 2.3.0, plus the locked pytest plugins. `mise` itself was unavailable; no alternate tool versions were substituted.

## Original processes, restoration and cleanup

The original replay child, its maintained timing wrapper/lane, and its outer launcher all exited **0**. Every validation command also preserves its original and wrapper exit **0**. Reports identify the same clean implementation at start and end; all pinned files are unchanged. Helper aliases, primitive wrappers, operation descriptors, profile/trace hooks and the temporary baseline module were restored or removed. Both owner variants are closed with pending samples, source mappings, digests and retained projection storage released.

The verification temporary tree, replay scratch directory and launcher temporary root were actually removed; the replay root was empty before its removal. Their original receipts and logs are archived unchanged. The replay lane's 73.574679718 seconds is the entire child run, including setup, diagnostics, postchecks and serialization; it is not a helper performance observation.

## Reproduction and first receiving action

The current entrypoint is [HANDOFF.md](HANDOFF.md). Reconcile the two-file proposal against then-current integration source; use the existing integration rail for any accepted change. The final evidence tree does not change the implementation used by the tests.

For an evidence reproduction, use an isolated checkout of implementation `7d2aebb21891bced44a703753040f91d9a351f0c` and its pinned environment. Keep scripts, restored inputs, output, temporary directories and caches outside that checkout. Restore `samples.json` and `events.json` by base64-decoding and gzip-decompressing the corresponding files in `evidence/`, and verify their hashes above. Copy the archived replay and verification driver `.py.txt` files to external `.py` files; retain the original source identities and update only path arguments for the receiving environment. The [execution receipt](evidence/private-projection-replay-execution.json) contains the exact original child argv; it selects `--expected-source` for the tested implementation, the proposal helper SHA, the retained input directory and [prior group receipt](evidence/prior-helper-replay-run.json). The [launcher](evidence/private_projection_run.py.txt) demonstrates original-exit and cleanup capture. Reproduction is optional evidence work, not a new acceptance gate.

No broad unit suite, actual caller fixture, new archive/product operation, journal census, terminal/restart/composed qualification, distribution/build qualification, new publication, or 1,000/10,000/100,000 campaign was performed. This reference does not provide full CI or tested-merge qualification for its final evidence commit or current main. The incomplete census, held campaigns, Native publication cost and underutilized parallelism remain open with their integration owners. No whole-path cost clearance or new product authority follows from these results.
