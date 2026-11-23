# Reproduce the focused worker-exit reference

## Exact source and environment

- Authoritative reference base: `be483fd6d567616d5fcd355202d61f1e682efe75` on main.
- Proposed patch target and tested implementation source: `3d039b382ee33d66c6049dd69e498169aa75bf6d` on `codex/994-durability`.
- Issue reproduction source: `fee80a186ae31765addfbd4b2e3f877f9eb8b286`; its physical execution owner is unchanged at the tested checkpoint.
- Execution host: Linux x86-64, kernel 6.18.57, glibc 2.41.
- Repository pins used: CPython 3.12.3 and uv 0.11.24; locked workspace dependencies, including pytest 9.1.1, Ruff 0.16.0, mypy 2.3.0 and setuptools 84.2.1.
- Native builds: C11, `-Wall -Wextra -Werror`; actual production and fault extensions built independently. Windows and Darwin binaries were not executed.

At prepublication reconciliation, integration had advanced to `0b25c7c67f7b3ee8787d8d753d4b6d585e8cea1f`. Its two finalization-test changes do not intersect the 24 proposed paths, and the physical execution owner retains the same blob. The executed evidence below is pinned to `3d039b382ee33d66c6049dd69e498169aa75bf6d`; it does not qualify the newer integration tip.

## First focused verification

Use a new detached checkout of the experiment source. Do not apply this patch to the preserved integration working tree or treat its main-based reference tree as the implementation checkout.

```sh
git fetch origin codex/994-durability reference/external/1005-worker-exit-receiving
git worktree add --detach ../riverhog-1005-reference origin/reference/external/1005-worker-exit-receiving
git worktree add --detach ../riverhog-1005-proposal 3d039b382ee33d66c6049dd69e498169aa75bf6d
git -C ../riverhog-1005-proposal apply --check ../riverhog-1005-reference/.reference/implementation.patch
git -C ../riverhog-1005-proposal apply ../riverhog-1005-reference/.reference/implementation.patch
cd ../riverhog-1005-proposal
mise x -- uv run --locked --all-packages --group dev python \
  ../riverhog-1005-reference/.reference/verify.py \
  --evidence ../riverhog-1005-evidence
```

The evidence destination must not already exist. The verifier checks the exact source SHA, exact changed paths and proposed file hashes; it does not reset source, publish anything or run unrelated scale workloads. `proposal-manifest.json` records the patch digest and every changed file's content digest. The local producer used the same locked environment via an explicitly pinned uv/Python installation; tool installation paths are incidental and are not additional source authority.

The verifier first extracts the original physical execution owner from the pinned Git commit. It runs all eight baseline combinations using that actual owner and the same maintained isolated probe, and then explicitly requires the safe expectation to fail against that owner. It next runs scoped static checks and the complete selected focused suite against the proposed files. The rest of the checkout and environment are common to the before/after probe; this is a causal comparison of the actual owning module, not a claim that an entire unmodified repository release was requalified.

The focused suite includes all retained-resource tests and native wheel checks, six existing Core execution/resource/retirement test modules, Core physical polling growth, qualification selection, native evidence selection, three relevant ownership/dependency-direction checks and dependency policy. Its growth witnesses use the existing `qualification_numeric_policy()` and `bind_numeric_environment()` definitions and ordinary `validate_execution_samples()`. No numeric limit or assertion is weakened. Legacy JUnit permits the repository's existing `record_property` evidence while warnings remain errors.

## Completed validation

| Execution | Actual result | Observed pytest elapsed |
|---|---|---|
| First-step verifier's focused suite, clean applied patch and separate pinned environment | **342 passed, 1 skipped** | 166.36 s |
| Broader run of the same proposal, including all 24 workspace-boundary tests and their generated-contract fixture | **363 passed, 1 skipped** | 887.43 s |
| Actual unchanged-owner before matrix | **8/8 reproduced unsafe early completion** | Per-case observations in results.json |
| Required safety expectation against the unchanged owner | **Failed as expected** with the observed unsafe receipt | Recorded assertion failure |
| Ruff, format, scoped mypy, Git whitespace and patch/file identity checks | **Passed** | Per-command observations in results.json |

The single skip in each suite is the Darwin-only refusal witness on a Linux host. The focused pass set is contained in the broader pass set; these counts must not be added as distinct coverage. Both used exactly the same proposed file hashes and patch digest. The broader run completed successfully, including full contract-fixture generation; the first-step command selects the three relevant boundary checks to keep routine consumption focused. No repository assertion or test was changed to shorten that command.

Native worker growth validated **111 actual samples**: 3 native starts, 108 polls, 57 Linux tryjoin entries, 3 positive exit receipts, no Windows waits/closes, and 136-byte native owner storage. Core validated **45 submission samples and 18 receiving samples**, observing 9 actual starts and 36 actual native receive polls within their declared quanta. Full primitive records are retained in the broader JUnit below.

## Evidence interpretation

The exact final check results, elapsed times, baseline observations and source/patch identity are recorded in [evidence/results.json](evidence/results.json). [evidence/summary.json](evidence/summary.json) extracts the final test counts, before/after facts and validated primitive samples. The complete superset record is retained once as [broader-tests.xml](evidence/broader-tests.xml), with its [command record](evidence/broader-results.json) and [output](evidence/broader-tests.log). The [focused output](evidence/focused-tests.log) and [expected baseline assertion failure](evidence/baseline-required-safety-fails.log) distinguish the two execution scopes. This avoids duplicating the same large primitive records for the overlapping shorter run.

The central pass condition is that an original native thread remains held and observable after real coordinator SIGINT, **no completion is collected**, the original slot and both capacity charges remain, and the same outcome becomes available only after explicit release and native exit. Success and original-exception variants run for both collection and workspace paths, with a target tail and a Python thread-local finalizer. Safe cases perform two interrupted shutdown attempts and also verify live workspace lease exclusion and independent work.

The separate retirement consumer test verifies the actual directory phase, `.lease` namespace and physical/construction ledgers. The native test separately holds a real pthread TLS destructor after Python cleanup and C entry return. Injected before/after native-effect failures establish retained outcome ordering; synthetic unknown native results establish quarantine only, not the occurrence or recoverability of a real OS failure.

Growth observations count actual native creation, receive polling, join/wait/close entries and positive exit receipts. Already-received retries perform no additional native joins, waits or closes. The native witness varies unrelated retained-owner populations 1, 17 and 257; Core retains its existing 1, 17 and 1025 populations and one/three-slot receiving quanta. Native fixed-size storage is a measurement of this build, not an aggregate process-memory guarantee. Timing values are observations, not new performance targets.

During development, the maintained Core growth witness exposed a startup race in the proposal: a timed-out bootstrap-event observation could be followed by actual acknowledgement and native exit before the diagnostic check. The implementation now checks the stable event after positive receiving. Independent review also found a no-worker limbo-registration cleanup gap. Both corrections have deterministic maintained regressions. Final evidence is from the corrected proposal.

## Qualification not claimed

No Windows or macOS execution, free-threaded execution, other Python minor-version bootstrap qualification, whole-repository Linux qualification, full integration/merge CI, PostgreSQL deployment, provider, release, power-loss or aggressive scale qualification was performed. The Windows backend remains an unexecuted proposal. Darwin lacks an implemented worker backend and explicitly refuses construction. The original process/main interpreter must retain the owner; there is one startup coordinator per worker, as serialized by Core.

The reference contains no durable Core physical-bit/cost-settlement implementation and does not establish acknowledged delivery of a Python completion batch after Core removes a slot. Those limits and the supported-platform integration decision remain explicit in [DESIGN.md](DESIGN.md). Publishing this reference does not complete #1005 or authorize resuming deferred #991 work.
