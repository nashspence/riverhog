# Source-bound finite evidence

## Identity and result

| Role | Exact source |
| --- | --- |
| Issue-authorized audited base | `6e80898a793532beff7a1718effc0618e2472d58` |
| Clean tested implementation | `3e27e21b66da365b3b5385a1cefa2eaf47e3cc86` |
| Tested implementation tree | `871444add3653a813187399f69189182098b85e5` |
| Final handoff | The owning issue pins its published SHA; its append changes only `.reference` |

All twelve proposed test/support files in the final handoff are byte-identical to the clean tested implementation. Product/package implementation, declarations, dependency locks, and the public growth reader match the audited base. The public reader SHA-256 is `1adc9766c8d0c97cf97ce926695e6c38dc7d50458725e8947b57bfc94c90e376`. [source-integrity.json](evidence/source-integrity.json) records the changed-file blob identities and content hashes.

The extended scope retained **122 independent root occurrences and 1,078 successful sample-node checks**. Matched replay reduced projection calls **66 to 1**, declaration compilations **186 to 47**, and public-reader calls **120 to 46**, without dropping an independent child or nested check. Both routes used the same literal samples. A separate uninstrumented observation measured **24.991175 to 22.320676 seconds wall** and **24.919033 to 22.211838 seconds process CPU**. These are finite observer-helper observations; they do not qualify product throughput, current main, a whole census, or the interrupted diagnostic.

## Three-turn actual captures

Each successful capture executed exactly three ordinary service-owned caller turns of one 4,913-byte journal, `text_octets=0`, with zero unrelated rows. No Native fast-forward, case generator, trajectory census, or large fixture population was invoked. Capture hooks retained plain references/events without adding hash/JSON/tracing work during observation; serialization, hashes, inventories, and node counting happened after closure.

| Observed item | Baseline at `6e80898` | Candidate at `3e27e21` |
| --- | ---: | ---: |
| Actual caller / Native-owner turns | 3 / 3 | 3 / 3 |
| Retained independently received root objects | 122 | 122 |
| Public validation calls / root occurrences | 120 / 122 | 46 / 122 |
| Nested sample-node occurrences | 1,078 | 1,078 |
| Projection calls / declaration compilations | 66 / 186 | 1 / 47 |
| Native observer constructions/completions | 8 / 8 | 8 / 8 |
| Ordered capture events | 830 | 274 |
| Pending-root limit / descendant admission | 4 / no | 64 / yes |
| Roots received by the outer batch | 6 | 122 |
| Original child / maintained lane exits | 0 / 0 | 0 / 0 |
| Whole lane elapsed, including product work and cleanup | 54.526549 s | 57.106387 s |

The eight Native contexts were one retained opening, four session progress calls, and three session advances. Caller trees contained replay admission, replay advance, checkpoint progress, and Core preparation/framing. No additional Native fixture was needed for coverage.

Both captures began and ended on their exact clean source. The fixture generator/service closed, checked-out catalog connections were zero, observed Native storage closed, the private fixture directory was removed, primitive hooks were restored, and each batch released its retained state. The external temporary directories were empty afterward. Original [baseline](evidence/baseline-run.json) and [candidate](evidence/candidate-run.json) run records, lane receipts, logs, exact scripts, and losslessly compressed samples/events are retained.

### Actual captures are not a matched timing comparison

The captures have identical root/declaration inventories, delegate topology, and 1,078 node occurrences across 45 declarations. After receipt reordering, 113 complete root records match literally. The other nine differ only in byte-length observations: the candidate fixture path is ten bytes longer, propagating into lease-path/control dimensions and returned-metadata bytes; one replay-admission JSON measurement is one byte shorter and propagates through its parents. All hash-operation, SQL, VM-instruction, delegate-count, and other primitive metrics match. The path explanation follows the recorded paths; the one-byte JSON cause is consistent with incidental runtime metadata length, rather than independently proven by a retained product payload.

These differences are retained without normalization in [capture-parity.json](evidence/capture-parity.json). Consequently, the two actual lane durations above are separate execution observations. The performance comparison below instead uses one frozen baseline dataset, requiring exact canonical equality in both routes.

### Original writer failure and bounded recapture

The first identical baseline three-turn capture completed product work, validation, source checks, and fixture cleanup, then exited **1** because the external post-observation serializer passed tuple-valued declaration IDs to the strict canonical JSON writer. Its original failed run/lane/log/script are preserved under `writer-failed*` and `capture-writer-failed.py.txt`. It is not counted as a successful capture or a product failure.

Only that external serializer was corrected. Dummy examples of all 16 event types passed before one identical three-turn recapture. Total actual execution for this reference was **nine small turns**: three in the writer-failed baseline capture, three in its successful recapture, and three in the successful candidate capture. No further actual turns were run for timing, and the old three-turn regression fixture was deliberately excluded from the focused producer selection below.

## Matched literal helper replay

The control is **legacy routing on the proposal**, not an exact-source performance claim about the baseline implementation. Both routes run on clean `3e27e21`, using the retained baseline sample/event bytes. The proposal's reachable-subprojection behavior also applies to the legacy root batch. Its 66 projection-operation input tuples and 186 compilation / 120 reader counts nevertheless reproduce the original capture in order.

The replay invokes the actual Core metadata constructor, sixteen Core emitter calls, eight Native observer constructions/completions, and the original independent receiving sites. It includes the complete outer lifetime: source admission, metadata construction, wrapper setup/restoration, emission, sealing, all flushes, and release. Every sample equals its original canonical bytes afterward. Every original root occurrence is checked, including child records that also occur inside parent roots.

Input decoding/copying, imports, equality checks, report writing, and product execution are outside the paired timing interval. Native wrappers are installed against recording-disabled objects. The actual `Recorder.paused()` method runs with empty database/native resources, so populated resource-pause accounting is not reproduced. No product body executes. These limits prevent treating the result as whole-path or product performance.

### Separate uninstrumented observations

| Route on the same proposal | Wall seconds | Process CPU seconds |
| --- | ---: | ---: |
| Legacy routing, root limit 4 | 24.991175260 | 24.919032733 |
| Extended routing, descendants admitted, root limit 64 | 22.320676168 | 22.211837929 |
| Observed reduction | 10.69% | 10.86% |

There was one elapsed observation per route, after one separate instrumented pass per route. The order was instrumented legacy, instrumented extended, elapsed legacy, elapsed extended. This is not a statistical estimate or timing gate. The original replay child and maintained lane exited 0 without retry; whole lane elapsed was 100.825386 seconds, including all four passes and reporting overhead. Source remained exact/clean, all descriptors and primitive hooks were restored, all batches released, and the actual profile scratch directory was removed.

### Complete diagnostic counts

| Observed operation | Legacy routing | Extended routing |
| --- | ---: | ---: |
| Projection calls | 66 | 1 |
| Declaration compilations | 186 | 47 |
| Compiled declaration occurrences | 1,780 | 929 |
| Expression-unit traversals | 894,851 | 560,860 |
| Public-reader calls | 120 | 46 |
| Independent public-reader root occurrences | 122 | 122 |
| Successful sample-node checks | 1,078 | 1,078 |
| Fresh reachable subprojections | 4 | 46 |
| Source snapshot decodings | 2 | 2 |
| Whole-source verification calls | 5 | 5 |
| Cheap source lookups | 12 | 411 |
| Canonical encodings | 2,214 | 1,923 |
| SHA-256 constructions | 2,213 | 1,922 |

Per-declaration node counts, every public-reader root receipt, projection sizes, input identities, and descriptor-restoration checks are retained in [replay-run.json](evidence/replay-run.json). [replay-action-plan.json](evidence/replay-action-plan.json) records the retained event reduction into actual helper calls; it does not replace the raw event transcript.

### Four exclusive costs and the remainder

These are **instrumented** diagnostic observations, separate from the uninstrumented table. Nesting-aware timers attribute each interval to one active category; inclusive nested times are not summed.

| Category | Legacy wall / CPU seconds | Extended wall / CPU seconds |
| --- | ---: | ---: |
| Declaration projection and compilation | 19.145716 / 19.094849 | 13.284678 / 13.223841 |
| Source digest and sample-seal verification | 5.531916 / 5.518435 | 4.749473 / 4.741242 |
| Sample bounds and composition validation | 2.368638 / 2.362363 | 2.230655 / 2.226535 |
| Primitive-wrapper setup and teardown | 0.013246 / 0.013101 | 0.002639 / 0.002665 |
| Observer orchestration and replay dispatch | 0.051156 / 0.050935 | 0.065879 / 0.059382 |
| Whole diagnostic pass | 27.110750 / 27.039717 | 20.334164 / 20.254414 |

The last category includes dispatch, bookkeeping, and diagnostic overhead; it is not product work. The primitive category covers wrapper setup/restoration only. Declaration work remains substantial in this finite replay. The reference does not attribute other CPU in the integration agent's interrupted run, explain separate publication-helper cost, or establish whole-path growth closure.

## Focused verification on the clean implementation

All commands used the source's locked tools: Python 3.12.3, uv 0.11.24, pytest 9.1.1, Ruff 0.16.0, REUSE 6.2.0, and mypy 2.3.0, including the pinned pytest plugins. The verification driver checked tool pins and exact clean source before/after every gate and after cleanup. Gates ran sequentially after the replay ended.

| Check | Original exit / result | Maintained lane elapsed |
| --- | --- | ---: |
| Five-file focused pytest selection | 0; **162 passed**, 1 deselected | 23.513700 s |
| Repository Ruff check | 0; passed | 0.971360 s |
| Repository Ruff format check | 0; passed | 0.401784 s |
| Repository REUSE lint | 0; passed | 1.685801 s |
| Maintained `scripts/run_mypy.py` | 0; passed | 49.756512 s |

The focused selection includes all 45 new lifetime cases and 117 retained small cases. It covers exact source identity, foreign callable rejection, in-place mutation, whole-union checks for unselected declarations, complete reachable projections including unused edges, private-reader mutation isolation, exact supplied sample fields, invalid numeric/composition evidence, poisoned scopes, completed-child obligations on parent failure, closed-owner rejection, independent serialized validation, pure Native completion, paused Core receiving, and correct attribution of delayed batch failures. The earlier actual caller/unit regression is excluded only from this finite producer command because the explicit candidate capture executed and retained the required actual turns; no maintained integration gate was removed or weakened.

The original pytest controller recorded 162 passing setup/call/teardown sequences and a 21.284009-second pytest interval. Logs, original lane exits, source identities, driver bytes, pinned-tool observations, and the full controller receipt are under [evidence/verification](evidence/verification/). The driver exited 0 and removed its owned external temporary tree. `mise` was unavailable; these were direct pinned-environment commands, not a claim that the Make frontend ran.

Before the clean freeze, all 45 new cases and 33 existing small batch cases also passed exploratory checks, and a metadata-only Core recorder admitted 100 source owners with 30 bindings and zero per-owner projections. Those earlier observations are separate from the clean-source verification above.

## Artifact identities and limits

| Artifact | SHA-256 |
| --- | --- |
| Baseline samples, original canonical JSON | `a9e69163ba9aac6c5cefd4f582c609997217421528840694449f24007f6c2b20` |
| Baseline events, original canonical JSON | `5e559ea611938996d6b47df6588e4a9f2fb7c5f5623a0942b13cf1f144333a25` |
| Candidate samples, original canonical JSON | `c99f44401ea167eff0ea106505bb3917f2864b05b02716479fe4e90cadb762aa` |
| Candidate events, original canonical JSON | `0c1e8b13f031f5218a394771ac3c8265fc6edfb28947fa248a1ba89b83fa778b` |
| Frozen replay script | `4bec4b8cb0cdabcb9ab83a83f7abc96b1709dfd11db4211bb37ae81d8a025ba2` |
| Completed replay run receipt | `b62367b740cf428e5c7d39f8233237e63ca7ed226f65951b926de9e91cb6a844` |
| Verification driver | `d8162307af1dc048608717113bd77a164442eeb7b257e6c53cba009df17f615e` |
| Completed verification run receipt | `87c79a458f48a44e2c2cbf52754f764b161de0f408849860775a2e68c70c8a09` |

The baseline sample document is 1,989,073 bytes and the event document is 292,741 bytes. Samples/events are stored as lossless gzip/base64 text; reproduction restores the original JSON bytes and verifies their hashes. This is storage compression, not a new reader format or acceptance cache. [SHA256SUMS](SHA256SUMS) covers the complete reference packet except itself.

This packet adds no public package API, product/declaration change, custody/concurrency change, timing threshold, full census, large campaign, release qualification, or new authority to integrate. The 64-root Core admission bounds pending roots rather than bytes/RSS; existing reader admissions remain in force. Current CI and the normal integration rail retain their own authority.
