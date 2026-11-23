# Reproduce bounded startup and native worker-exit receiving

## Exact source and environment

- Audited authoritative reference base: `be483fd6d567616d5fcd355202d61f1e682efe75` on main.
- Revised patch target and executed implementation source: `0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f` on `codex/994-durability`.
- Previous reference: `3dce6780c4883ad96cb32ff3f7ef80883fef6528`, whose patch targeted `3d039b382ee33d66c6049dd69e498169aa75bf6d`.
- Original issue reproduction source: `fee80a186ae31765addfbd4b2e3f877f9eb8b286`. The unchanged physical execution owner is identical at all these source checkpoints and audited main.
- Execution host: Linux x86-64, kernel 6.18.57, glibc 2.41.
- Repository pins: CPython 3.12.3 and uv 0.11.24; locked workspace dependencies including pytest 9.1.1, Ruff 0.16.0, mypy 2.3.0 and setuptools 84.2.1.
- Native builds: C11, `-Wall -Wextra -Werror`; production and fault extensions remain separate. Windows and Darwin binaries were not executed.

The source refresh adds the integration agent's two finalization-test changes. They do not intersect the proposal, and no integration working tree was modified. Final evidence comes from a clean application of the revised patch in a separate checkout and locked environment.

## First focused verification

Fetch the reference and integration histories. Set `reference_sha` to the exact **current reference SHA recorded in the owning #1005 handoff**, then use separate detached checkouts:

```sh
git fetch origin codex/994-durability reference/external/1005-worker-exit-receiving
git worktree add --detach ../riverhog-1005-reference "$reference_sha"
git worktree add --detach ../riverhog-1005-proposal 0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f
git -C ../riverhog-1005-proposal apply --check ../riverhog-1005-reference/.reference/implementation.patch
git -C ../riverhog-1005-proposal apply ../riverhog-1005-reference/.reference/implementation.patch
cd ../riverhog-1005-proposal
mise x -- uv run --locked --all-packages --group dev python \
  ../riverhog-1005-reference/.reference/verify.py \
  --evidence ../riverhog-1005-evidence
```

The evidence destination must not already exist and must be outside the isolated source checkout. The verifier checks the source SHA, exact changed paths, patch digest and every proposed file hash, then repeats source/path/file checks at completion. It also pins uv, records the verifier's own digest and rejects a changed verifier. It creates an isolated evidence repository to materialize the previous proposal; it does not reset source, publish anything, modify integration or run aggressive scale workloads. The producer used the same repository-pinned tools through an explicit local uv/Python installation; those installation paths are incidental. Keep the same uv executable and cache configuration active for the verifier and its subprocesses: wheel tests build offline from that cache.

Add `--broader` and use a new evidence destination to include all workspace-boundary checks and generated-contract fixture generation. The default selects the three directly relevant ownership and dependency-direction checks. Neither mode weakens assertions or numeric limits.

## Two distinct failing-before comparisons

**Original native-exit defect.** The verifier extracts the actual unchanged physical execution owner from the current pinned source and runs all eight collection/workspace × target/Python-TLS tail × success/original-exception cases. It separately requires the safe assertion to fail against that actual owner. The maintained proposed implementation runs the corresponding passing-after cases within the suite.

**Previous reference's startup defect.** The verifier reads the immutable previous reference's manifest and implementation patch, verifies its patch digest, and materializes only its exact Python Worker and Core owner. Both resulting files must match their published hashes. The current native C source must match the previous native hash as well. The new probe then holds bootstrap before acknowledgement against that exact prior pair for both collection and workspace execution. A separate negative run requires the safe assertion to fail. This is not a synthetic bad backend or a naturally occurring startup fault claim.

The rest of the current checkout and environment are common to each comparison. The final report records both sets of source identities and observations. This isolates the relevant owner changes; it does not requalify every component of a historical release.

## Completed validation

| Execution | Actual result | Observed elapsed |
|---|---|---|
| Final clean applied patch, complete selected owner/package suite plus all workspace-boundary checks | **383 passed, 1 skipped** | **1,146.55 s pytest elapsed**; 1,151.346 s subprocess elapsed |
| Original unchanged physical owner | **8/8 reproduced unsafe early completion** | Per-case records in results.json |
| Exact previous reference's held-bootstrap matrix | **2/2 reproduced blocked coordination** | 9.554 s collection; 9.509 s workspace |
| Required safe assertions against both defective baselines | **Failed as expected** | Both recorded assertion failures |
| Ruff, format, scoped mypy, source/patch/path/file identity and whitespace checks | **Passed** | Per-command records |

All **19 verifier checks** returned their expected status. End-of-run source/path/file/verifier identity checks passed, and an independent postflight repeated the exact HEAD, path-set and all 26 proposed file hashes.

The final executed suite uses `--broader` and includes every case in the default focused selection. It is one execution and one pass count; the default subset was not separately rerun on the final clean checkout. The sole platform skip is the Darwin-only refusal witness on Linux. Elapsed times are observations, not product latency targets.

The selected suite retains all retained-resource tests and installed wheel checks, the existing Core execution/resource/retirement tests, actual physical polling growth, qualification/native evidence selection and dependency policy. The new Core bootstrap cases are maintained in those same owner test modules. The broader run also covers all workspace-boundary checks after the public diagnostic export and growth declarations changed.

The growth witnesses use the unchanged `qualification_numeric_policy()`, `bind_numeric_environment()` and ordinary `validate_execution_samples()`. Legacy JUnit supports the repository's existing `record_property` evidence with warnings still treated as errors. No test, capacity, population, quantum or declared numeric-policy limit is reduced.

A preliminary clean-checkout attempt stopped in an offline wheel-build fixture because the launcher inherited a different uv executable and its default cache. The final run uses the recorded pinned executable and the populated cache consistently; the verifier now checks uv before running. No wheel requirement or assertion was weakened. Final evidence below is from the corrected environment and the final measurement revision.

## What the startup cases require

While the original worker is held before acknowledgement, submission must already have returned with its native creation recorded. The original Future remains unset, and its exact slot and both capacity charges remain retained. An already-native-received independent original can be reported and collected, and a fresh independent original can be submitted, finish and be received before the held bootstrap is released. Explicit release then yields the original result exactly once and clears both ledgers.

The interrupted-start variants deliver real SIGINT after the actual native creation and before the Python reply returns. They retain the same native owner, slot, outcome identity and capacity, with one actual native start, while independent work progresses. Existing native fault-build tests separately exercise signal delivery and lost replies at the C effect/outcome boundary.

The failure variants raise the exact original exception before acknowledgement. Only after positive native exit does bounded polling report `WorkerBootstrapError` with that original exception as its cause. The semantic Future stays unset and the original slot/capacity stay charged. Repeated diagnosis performs no additional native join. Subsequent collection rotates to an independent ready original for the same receiver. Shutdown receives a later held worker before reporting the known, already-received bootstrap diagnostic; genuine coordinator interruption remains immediately propagating.

Public-wrapper tests separately hold both runtime registry locks and ordinary Thread initialization, deliberately create pending weak-reference removals, test initializer and pre-active-registration failures, and verify normal current-thread identity and exact cleanup. The coordinator never performs registry acquisition or pending-removal draining. Runtime initialization and those operations remain on the original native worker, covered by its eventual native exit receipt.

## Existing receiving evidence remains in scope

The original real-SIGINT cases continue to require two interrupted shutdown attempts with native exit still pending, no early outcome, exact original success/exception identity, workspace lease exclusion and capacity retention. Independent work and stale/foreign receivers remain covered.

The retirement consumer holds the actual directory phase, `.lease` namespace and physical/construction ledgers. A separate fault-native test holds a real pthread TLS destructor after Python cleanup and C entry return. Before/after effect seams retain native creation and receiving outcomes across lost replies. Synthetic unknown states prove quarantine behavior only; they do not claim an actual OS fault or its recoverability.

## Bounded-control and primitive observations

| Witness | Actual samples | Native/receiving observations |
|---|---:|---|
| Raw native worker | **111** | 3 starts, 108 polls, 57 Linux tryjoins, 3 exit receipts; 136-byte native owner |
| Public Python construction/start/poll | **228** | 6 starts, 216 polls, 114 tryjoins, 6 exit receipts, 108 final acknowledgement reads |
| Core submission | **45** | 9 actual native starts; zero waits, registry work, acknowledgement reads or own retirements |
| Core receiving | **18** | 36 native polls, 36 final acknowledgement reads and **36 own weak-reference retirements for 36 returned originals** |

All four witnesses validated through the maintained numeric machinery. Coordinator waits, bootstrap registry acquisitions/steps/enumerations, and pending-list cleanup steps were zero. Public-wrapper own retirements were zero because those operations retain their owner. Core collection's measured own-retirement count obeyed its per-quantum bound. No Windows wait or handle-close primitive was exercised on Linux.

The raw native witness counts actual native creation, poll, join/wait/close entries and positive exit receipts. Received retries perform no additional joins, waits or closes. The public-wrapper witness additionally observes caller-side Event/Condition waits, Thread/Worker joins, sleeps, runtime registry lock acquisitions, active/limbo/weak-registry operations, pending weak-reference removal steps and acknowledgement reads. Construction/start require zero waits, registry work and acknowledgement probes; a receive operation permits at most one final acknowledgement read after native exit.

Core composes those same actual wrapper observations into its existing full-key-width mapping and native-worker measurements. It additionally counts at most one own weak-reference retirement per removed worker, either direct removal or a pending-list append. Pending-list draining remains zero. Class-level poll observation and no extra retained slot dictionaries let ordinary last-reference cleanup occur inside the measured collection. Worker-owned work is distinguished by actual thread identity, so one native start counter cannot hide a Python wait. The native/public-wrapper witnesses retain unrelated-owner populations 1, 17 and 257. Core retains populations 1, 17 and 1025 and one/three-slot receive quanta. Native owner storage describes this build, not aggregate process memory. Explicit rendezvous deadlines are test cleanup watchdogs, not performance gates.

## Retained records and precise limits

[evidence/results.json](evidence/results.json) records the final run's actual commands, source identities, before observations and results. [evidence/summary.json](evidence/summary.json) extracts final counts, observations and growth measurements. The complete selected suite is retained once as [broader-tests.xml](evidence/broader-tests.xml), with its [output](evidence/broader-tests.log). The [original-defect expected assertion failure](evidence/baseline-required-safety-fails.log) and [previous-startup expected assertion failure](evidence/previous-startup-required-safety-fails.log) keep the comparisons distinct. The superseded handoff and its earlier results remain available in Git history.

No Windows/macOS execution, other Python minor-version bootstrap support, free-threaded execution, whole-repository Linux qualification, full integration/merge CI, provider/release/deployment, power-loss or aggressive scale qualification is claimed. The Windows backend is an unexecuted draft; Darwin still explicitly refuses worker construction. One startup coordinator retains each worker in its original process and main interpreter, as serialized by Core. Application shutdown must finish native receiving before interpreter finalization, including unacknowledged workers.

Post-acknowledgement bootstrap exceptions remain in `bootstrap_exception`; if no original Future exists, Core retains `outcome-missing`. The proposal does not add acknowledged delivery of a completion batch after Core removes a slot. It does not enable durable Core physical bits, final lifecycle cost settlement or independent #991 work. Platform disposition and integration acceptance remain with #1005 and the normal integration rail.
