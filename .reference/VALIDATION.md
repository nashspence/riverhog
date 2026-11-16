# Actual evidence and limits

Evidence collected on 2026-11-16 for the #997 reference. Audited implementation: **`c28eceaa2dfec1b3712135469ba12230e74f5ffa`**, the current observed #979 head. Product and maintained test paths are unchanged. Only `.reference/` is added. The #995/#996 references are pinned inputs, not installed implementations in this branch.

## 1. Final results

| Evidence | Actual outcome | What it establishes |
| --- | --- | --- |
| Final combined reference suite | **44 passed, 0 failed/errors/skipped**, 74.589366111 s recorded pytest time; 76.856210676 s including process setup | 32 SDK and 12 Core actual-owner/fixture cases on the final four Python files. |
| Separate unchanged safety selection | **9 passed, 0 failed/errors/skipped**, 54.520288268 s recorded pytest time | Current #981 HTTP retry, #984 crypto/dispatch/late-seal/receiving, #989 actual Native receiving/independent-progress/close-refusal witnesses. |
| Maintained authenticated/encrypted F1 baseline | Passed on **clean exact c28eceaa**; complete publication and three full independent reader passes; child and workspace cleanup received | Current public application composition with preserved complete history. It does not run the proposed public cut or new Native executor. |
| SDK counted experiment | Successful real SDK/Native/POSIX run with explicit API fault fixture | Original observation/journal/decision reuse, intact control-only custody after damaged local journal, guarded warm read-barrier comparison. |
| Core counted experiment | Successful actual encrypted raw/pack cases at 64/128/256 KiB, controlled individual settlement versus existing batch | Bounded exact returned effects can share one catalog checkpoint transaction; original admission/provider/receiving remains. |
| Ruff 0.16.0 | `check` passed; `format --check` reports all four files formatted | Reference Python style only. |
| mypy 2.3.0 | Core experiment passed `--strict --follow-imports=skip --ignore-missing-imports` | The local Core harness with imported owners skipped; not full product or SDK typing. |
| Repository Make wrappers | `make profile` and `make lint` blocked at missing `mise`, wrapper 127 / Make 2 | Neither Make gate passed. Focused owner execution is separately reported. |

The 44-case run is the final reference cohort; do not add the preceding 32-case SDK, 12-case Core or other intermediate reruns to it. The nine maintained test identities are a separate, explicitly listed cohort. A profile success is not another pytest case.

Original artifacts:

- [REFERENCE_TESTS.xml](evidence/REFERENCE_TESTS.xml), [REFERENCE_TESTS.txt](evidence/REFERENCE_TESTS.txt), [REFERENCE_TEST_COMMAND.json](evidence/REFERENCE_TEST_COMMAND.json), [REFERENCE_TEST_TIMING.json](evidence/REFERENCE_TEST_TIMING.json).
- [SAFETY_PASSED.xml](evidence/SAFETY_PASSED.xml), [SAFETY_PASSED_TIMING.json](evidence/SAFETY_PASSED_TIMING.json); [blocked initial collection](evidence/SAFETY_BLOCKED.xml) and [its timing](evidence/SAFETY_BLOCKED_TIMING.json).
- [F1_BASELINE.json](evidence/F1_BASELINE.json), [final phase receipt](evidence/F1_BASELINE_PHASE.json), [exact workload](evidence/F1_WORKLOAD.json).
- [SDK_PREPARED_PRIMARY.json](evidence/SDK_PREPARED_PRIMARY.json), [CORE_BATCH.json](evidence/CORE_BATCH.json); separate Core focused [JUnit](evidence/CORE_BATCH_TESTS.xml), [output](evidence/CORE_BATCH_TESTS.txt) and [command receipt](evidence/CORE_BATCH_VALIDATION.json).
- [VALIDATION.json](VALIDATION.json) records final source/runtime/command metadata and artifact digests. The published Git tree is verified separately at the exact handoff SHA recorded on #997.

The final reference and separate safety timing record truthfully report `source_clean=false`: `.reference` was being authored. This is distinct from the clean F1 baseline and does not satisfy exact-final-source/merge-checkout product CI. Source hashes in those records identify unchanged product input, not the later published reference commit.

## 2. Real F1 baseline

The unchanged maintained `scripts.transfer_profile` parent ran `tests.support.qualification.publication_profile` through its original subprocess/receiving path. Run ID: `48fb45d2-0e13-4915-b65b-e72020aab459`. Exit status 0; `completion_verified`, `child_received` and `workspace_cleanup_received` are true. Source checks were clean before and after. Source and upload scratch were removed before independent verification.

| Quantity | Actual result |
| --- | ---: |
| Publication | 45.802746878 s |
| Parent execution envelope, including readers and required cleanup | 64.411533234 s |
| Fresh reader | 4.142022230 s; 58 stored reads; 167,370 stored octets |
| Warm reader | 4.191056257 s; 41 stored reads; 135,013 stored octets |
| Evicted reader | 4.161700759 s; 58 stored reads; 167,370 stored octets |
| Verified members/payload | 1 member; 65,536 bytes |
| Complete retained Native history | 4 physical journals; 5 entries; 19,873 journal bytes |
| Bound and required history | 3 journals; 4 entries; 17,966 journal bytes |
| Verification per pass | 21 stored objects; 20 whole-object checks; 1 part check |

All passes agree on archive root `edce19a2521c6fbcf9f2347d1df62bafb55b71b80596a05c47bcef1e9ee3e108`. Conditions are authenticated in-process TestClient HTTP, SQLite 3.45.1, real encrypted in-memory archive storage, maintained scrypt work factor 1, no producer cache, one-member append, 0.05-second coordinator polling and the unchanged 900-second finalization deadline. Fresh/evicted refer to the Native reader/cache; OS caches were not cleared. This is not installed independent recovery, production memory qualification or a remote provider run.

The raw report says `not-compared` / `no-measured-reference`. No optional-checkpoint count, mandatory/optional transaction attribution or physical synchronization was measured in this F1 run. There is no old/restart/adaptive matched comparison and no claimed new-path speedup. The observed 45.8-second baseline is context, not a timing gate. Its phase file retains only the final phase, not a complete per-phase event trace.

The ordinary `make profile` attempt could not start because `mise` is unavailable. The successful focused fallback invoked the unchanged maintained Python owner with the actual source-owned qualification numeric profile. The exact command and checked numeric identity are in [QUALIFICATION](QUALIFICATION.md#6-reproducible-environment-and-invocation). No source check, fixture, configured deadline, authentication, encryption or required reader was removed.

## 3. SDK source experiment

[prepared_primary_experiment.py](implementation/prepared_primary_experiment.py) uses the repository's real `PrimaryPublisher`, `PrimaryStore`, Native `BoundedSourceObserver`, canonical journal authoring/validation and POSIX files. Its API object is a deliberately explicit in-process fault fixture. Its typed receipt is fixture-supplied; it is not independently authenticated remote custody. The unchanged HTTP safety test and F1 cover the separate real service boundary.

The counted run closes/reopens the original store after staging-before-binding and accepted-decision reply loss. It records **one observation and one staged journal**, with two staging calls, two binding attempts and two identical decision calls. The complete original prepared admission is **16,910 bytes**. Independent Native validation uses the exact collection-production contract catalog and selected original prefix. The complete receipt is bound to the original manifest/primary; a damaged redundant local journal does not trigger replacement authorship.

| Observed phase | Python `fsync` calls | Preimage content readers / bytes | Control bytes read | Elapsed |
| --- | ---: | ---: | ---: | ---: |
| Four current warm control reads | 16 | 0 / 0 | 10,556 | 5.430193 ms |
| Conservative proposed epoch entry, separately charged | 4 | 0 / 0 | 2,639 | 1.471182 ms |
| Four guarded warm control reads | 0 | 0 / 0 | 10,556 | 3.681141 ms |
| Cold receipt, reclaim and exact retry | 59 | 0 / 0 | 22,630 | 20.443231 ms |

Including entry, the proposed read group uses **4 rather than 16 observed fsync calls**, and reads 13,195 rather than 10,556 control bytes. Its single elapsed total is 5.152323 ms versus 5.430193 ms; that small difference is not a reliable performance estimate. The useful evidence is removal of repeated synchronization while retaining the same exact four control checks and reporting entry cost. Production status must reuse a proof already established by mandatory execution; it must not synchronize to create one.

The cold receipt/reclaim phase retains all 59 actual Python fsync calls, reads no preimage content, performs no directory enumeration, and releases the original reserved byte extent exactly once. It opens five role inodes for metadata validation before unlink; metadata opens are not preimage content reads. Total Python SHA-256 work includes controls and is reported separately in the raw artifact.

The proposed read lease is guarded and single-threaded. It invalidates on writes/creation/cleanup/candidate discovery/exceptions, verifies original control integrity and owner identity, and does not accept new receipts or authorize replay. The final SDK tests cover all five preimage roles with missing/truncated/changed bytes; missing decision acknowledgement; altered full receipt/member/primary/completion/self-seal; a changed repeated receipt; stale handle; changed retry descriptor; cold barriers; original manifest corruption; and forbidden lease writes, child/store creation and cleanup. Production requires backend-local lifetime state, not the experiment's process-global patch. No SQLite C sync, Windows/other filesystem, physical-device or power-loss claim is made.

## 4. Core source experiment

[core_batch_experiment.py](implementation/core_batch_experiment.py) uses unchanged `ArchiveUploadWrites`, `ArchiveWriteAdmission.apply_reports`, `SqlAlchemyArchiveUploadCheckpointStore`, actual age encryption and the maintained direct filesystem adapter fixture. The comparator settles returned effects individually and applies the logical checkpoint only with the final result; the grouped arm uses the existing source batch. Neither changes admission, provider calls, original report acknowledgement/retirement or mandatory encryption state.

| Kind | Plaintext | Actual provider part calls | Result commits, individual → batch | All observed commit attempts, individual → batch |
| --- | ---: | ---: | ---: | ---: |
| Raw segment | 64 KiB | 2 | 2 → 1 | 31 → 30 |
| Raw segment | 128 KiB | 3 | 3 → 1 | 35 → 33 |
| Raw segment | 256 KiB | 5 | 5 → 1 | 43 → 39 |
| Pack | 64 KiB | 2 | 2 → 1 | 30 → 29 |
| Pack | 128 KiB | 3 | 3 → 1 | 34 → 32 |
| Pack | 256 KiB | 5 | 5 → 1 | 42 → 38 |

These are actual harness observations, **not a comparison of old and redesigned F1**. The all-attempt column includes fixture setup/verification and provider-boundary admission-check transactions; those have separate stages in the raw report and are not described as shipped-path overhead. SQLAlchemy's engine commit event records an attempt before the DBAPI call; `Session.after_commit` records successful return separately. Neither measures a physical sync. Nested phase times are inclusive and must not be summed.

For matching kind/size, provider call counts and request octets match; each execution retains its own exact encryption/checkpoint identities. Separately generated randomized ciphertext need not be byte-identical across cases. Every completed case verifies its stored checkpoint equals the actual returned checkpoint, applies the logical result once and receives all original effects. Timings vary: some grouped cases are slower in this single noisy run. No speedup or durable-media conclusion is inferred.

The 12 Core tests preserve known rollback versus commit-reply loss, exact same-batch retry with no provider resend, changed checkpoint refusal, duplicate/mixed-volume reports, changed closure request/executor, positive observation without closure retaining charge and blocking completion, and terminal selection accepting exact old reports while fencing the next effect. This is private owner evidence using source paths for the adapter, not installed adapter or public route activation.

## 5. Commands, runtime and earlier findings

The separate locked environment uses Python **3.12.3**, uv **0.11.24**, SQLite **3.45.1**, pytest **9.1.1**, Ruff **0.16.0**, mypy **2.3.0**, SQLAlchemy **2.0.51**, FastAPI **0.141.1**, Starlette **1.3.1**, cryptography **50.0.0** and httpx2 **2.12.0**. It contains the 70 selected Core/SDK/Native-test packages plus 31 root dev-group dependencies. The #996 environment was not resynced. The unchanged `uv.lock` SHA-256 is `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`.

The final test command/environment is preserved verbatim in REFERENCE_TEST_COMMAND. It binds the source-owned numeric policy, loads `scripts.ci_timing`, uses external temporary/state/pytest directories, selects only `.reference/implementation`, and includes the unchanged filesystem/ASGI source roots. The final four Python files were not changed afterward. Their hashes are recorded in VALIDATION.json.

Reference static checks, run from the checkout:

```bash
/workspace/scratch/939b84c043b0/toolchain/bin/ruff check .reference/implementation
/workspace/scratch/939b84c043b0/toolchain/bin/ruff format --check .reference/implementation
/workspace/scratch/939b84c043b0/toolchain/bin/mypy --strict --follow-imports=skip --ignore-missing-imports .reference/implementation/core_batch_experiment.py
```

Standalone measurement entrypoints are branch-local reference tools, not new maintained `make profile` scenarios:

```bash
TMPDIR=/absolute/task-temp \
PYTHONPATH=.:some-implementations/riverhog/storage/filesystem/src:packages/riverhog-storage-adapter-asgi-support/src \
/absolute/core-sdk-env/bin/python .reference/implementation/core_batch_experiment.py --output /absolute/core-result.json

TMPDIR=/absolute/task-temp \
/absolute/core-sdk-env/bin/python .reference/implementation/prepared_primary_experiment.py /absolute/new-sdk-state > /absolute/sdk-result.json
```

Use a new SDK state path for each standalone run; it deliberately retains the original captured identities of that run. Reference tools are reproduced with the pinned environment described in QUALIFICATION. Product profiling remains `make profile`.

Earlier outcomes are retained honestly:

- The safety cohort first failed collection on two missing filesystem-adapter imports (exit 4, 7.735645617 s); no tests ran. The same unchanged selection then passed with explicit checkout source roots. Both records are included.
- The initial SDK sample omitted the required production contract catalog from its independent Native validator. The fixture now supplies the actual canonical authoring catalog; validation was not bypassed.
- The initial SDK test run had 25 passes and four fixture-expectation failures (53.62 s): metadata inode opens were incorrectly treated as content reads, a wrong-member test receipt was internally invalid, and one case expected a narrower error class than the unchanged Pydantic validation error. Corrected cases retain the original semantic/error behavior. The final combined suite includes all corrected and additional mutation-exclusion cases.
- Early Core pytest runs used the task environment's fallback temporary directory inside the checkout. After all original fixture owners closed and processes exited, those confirmed task-owned outputs were moved outside the checkout for preservation. The final combined run uses explicit external temporary directories. No product operational state was deleted or reset.
- `make profile` and `make lint` could not start without `mise`; no complete repository gate, broad campaign or provider qualification was substituted or claimed.

## 6. Remaining qualification is concrete

The reference does not implement the proposed shared publication conductor, public effect/terminal activation, catalog/index revision protocol, state refusal/initialization or cross-component progress contract. It does not demonstrate real default-policy optional eligibility or restart on that new path. [QUALIFICATION.md](QUALIFICATION.md) specifies the first real slice, mandatory failure windows, complete history/index/readers, short-writer/independent-progress checks, mutation races, exact original receiving, state/backend/platform/installed gates and matched old/restart/adaptive measurements.

Complete SQLite/PostgreSQL, Linux/macOS/Windows, installed independent recovery, actual power-loss/physical synchronization, full-lifecycle growth and exact-final-source/tested-merge CI remain unperformed here. Required correctness/growth/evidence failures remain failures; measured timing is not a product gate. No provider/release/merge permission or resumption of #991 follows from these reference results. Publication is accompanied by a fresh branch/tree/parent readback and check discovery recorded on the owning issue; absent reference-branch checks are not a CI pass.
