# Validation actually performed

This is a #903 reference packet for [#1000](https://github.com/nashspence/riverhog/issues/1000), not delivered durability-cutover qualification. The actual product source is **c28eceaa2dfec1b3712135469ba12230e74f5ffa**, tree **044a0e1e3eecf0b4183836017269fc208b6603d2**, on the #979 integration rail. All product paths remain unchanged. [Final source/issue refresh](evidence/source_review.json) confirms the same integration head, unchanged #994/#1000 bodies and the inspected main SHA; main was not tested.

## 1. Environment and provenance

The dedicated environment uses locked **CPython 3.12.3**, SQLite **3.45.1**, pytest **9.1.1**, Ruff **0.16.0**, mypy **2.3.0**, and the repository's frozen all-package/development dependency set. The host is Linux x86_64. The exact interpreter, loaded owner paths, versions and lock digest are in [environment.json](evidence/environment.json) and [safety_environment.json](evidence/safety_environment.json). The latter records the source-owned numeric limits: `max_work=907920445364`, `max_integer_bits=84`, qualification profile, reader SHA-256 `4f08bc6148007c7194b2451cdd885b04ada30806b6ee1f6c5e6d29433885720a`.

The dedicated environment was installed with the pinned uv 0.11.24 using `uv sync --frozen --all-packages --group dev` and the pinned Python. Initial offline cache resolution lacked botocore; normal frozen synchronization succeeded. No lock or product file was edited. Imported product owners resolve to this checkout, not an earlier reference environment. Temporary directories and emitted execution evidence were outside product source.

The standalone F1 observation ran **before reference edits or concurrent test work**, with clean, unchanged source before/after. Later tests accurately record a dirty checkout while `.reference/` was authored. Before/after source snapshots and hashes are boundary observations under exclusive product-path ownership, not a detector of transient edits reverted between snapshots. The final reference suite hashes all seven reference Python inputs before and after execution.

## 2. Final test results

| Executed cohort | Actual result | Pytest elapsed | Original retained evidence |
|---|---:|---:|---|
| Final combined reference suite | **66 passed; 5 subtests passed** | **4.22 s** | [Invocation/input hashes](evidence/reference_final_command.json), [stdout](evidence/reference_final.txt), [JUnit](evidence/reference_final.xml), [original timing](evidence/reference_final_timing/pytest/unit/7d9d3741bf79482084c61cf00f46c088/424ec7e27de449bfa6cb2e20d4a7070f/controller.json) |
| Maintained qualification/evidence consumers | **201 passed** | **93.08 s** | [Invocation/selection](evidence/owned_validation_command.json), [stdout](evidence/owned_validation.txt), [JUnit](evidence/owned_validation.xml) |
| Maintained measurement/profile owners | **69 passed** | **80.53 s** | [Invocation](evidence/measurements_maintained_invocation.json), [stdout](evidence/measurements_maintained_stdout.log), [JUnit](evidence/measurements_maintained_junit.xml) |
| Maintained safety/fault owners | **32 passed; 2 skipped** | **36.49 s** | [Command](evidence/safety_baseline_command.json), [result](evidence/safety_baseline_pytest.json), [JUnit](evidence/safety_baseline_junit.xml) |
| Maintained state owners | **5 passed** | **2.26 s** | [Command](evidence/coverage_state_command.json), [stdout](evidence/coverage_state_stdout.txt), [JUnit](evidence/coverage_state.xml) |
| Maintained small growth cohort | **3 passed** | **8.14 s** | [Command](evidence/safety_growth_final_command.json), [result](evidence/safety_growth_final_pytest.json), [JUnit](evidence/safety_growth_final_junit.xml), [75-sample summary](evidence/safety_growth_summary.json) |

The final reference suite consists of 48 comparison cases, 11 source-guard cases and seven actual Native interruption/binding cases. Its original process interval was **5.454513460 seconds**. The five subtests belong to the guard cases; they are not five additional top-level tests. Earlier lane runs and the initial combined success are retained, but are not added again to these final counts. A standalone profile execution is also not a pytest case.

The 201 maintained consumer cases exercise original execution/attempt reconciliation, missing/duplicated/forged witnesses, source drift, platform and scope mismatches, retained subprocess cancellation/receiving, incomplete descendants, capacity and numeric-policy requirements. Their original process interval was **95.808057157 seconds**. The 69 measurement cases cover the existing strict measurement/profile contracts and real Native reader-state behavior; original process time was **83.694272396 seconds**. Exact selections and output are preserved, not inferred from a green aggregate.

### Actual process interruption

The reference starts an actual POSIX Native child with retained state, waits for the exact declared transaction boundary, sends SIGKILL, positively reaps that original child, then cold-reopens the retained store:

- Kill before source/parent commit: recovered cursor **0**.
- Kill after source/parent commit: recovered cursor **7**.
- Both cold resumes reach exactly the original journal's direct-validation result.
- Changed source version/currentness, worker epoch or algorithm cannot adopt or rewrite the retained original; an invalid cut is rejected before store creation.

The final suite's original fault records are [before commit](evidence/reference_final_faults/safety_before_source_commit.json) and [after commit](evidence/reference_final_faults/safety_after_source_commit.json). This is an actual current-contract process-kill witness. It does not establish optional generation behavior of unimplemented redesigned code, OS/backend crash consistency, power-loss durability, Windows support or real-provider behavior. [Fault matrix and exact limitations](SAFETY_AND_GROWTH.md)

The maintained safety cohort also covers abrupt exits, atomicity, Core exact arming/receiving, SQLite construction/close faults and compiled retained-resource C close/sync faults. The two skips are Linux quota/VFS tests whose required SQLite VFS API is hidden by this pinned runtime. They remain unmet native-I/O evidence. The existing Core abrupt-exit test stops after durable arming but before physical provider entry; it is not a post-entry/late-write witness.

### Small actual growth evidence

The three maintained growth cases produced **75 samples**: 12 retained-corpus transaction samples, 45 physical-workspace submission samples and 18 receiving samples. They vary actual retained count/control width and bounded work quantum. These are unchanged owner witnesses, not new-generation snapshot/restore/reclamation qualification. The existing helpers validate under the explicit numeric policy but do not retain returned per-validation usage in raw pytest properties. This packet neither reconstructs an original usage receipt nor claims complete collected growth-gate acceptance.

## 3. One clean-source maintained F1 observation

Run **8111d918-d0ec-4e8d-952c-02bd19b76234** exercised the actual existing F1/C1/K2/Q1 producer and independent Native readers. It published one 65,536-byte member, retained four physical journals/five entries/19,873 journal bytes, and verified all 21 stored commitments, including the part commitment, after original source and upload scratch were absent. The original child exited zero and workspace cleanup was received.

| Observed interval/work | Result |
|---|---:|
| Existing helper `publication_seconds` | 44.761003893 s |
| Complete original parent envelope | 65.034581815 s |
| Outer invocation | 65.059421626 s |
| Fresh / warm / evicted verification | 3.934616697 / 3.882517286 / 3.831112246 s |
| Stored reads | 58 / 41 / 58 |
| Stored octets | 167,370 / 135,013 / 167,370 |

[Original invocation/source](evidence/measurements_f1_invocation.json) · [profile](evidence/measurements_f1_profile.json) · [phase record](evidence/measurements_f1_phase.json) · [workload](evidence/measurements_f1_workload.json) · [stdout](evidence/measurements_f1_stdout.json)

The current helper's publication interval includes fixture setup/evidence authoring, producer completion/close, catalog checks, scratch deletion and source unlink. The parent interval also includes three complete verification passes. **Neither is an isolated durable-user-result timer, and their difference is not cleanup tail.** The proposed instrumentation fixes these boundaries before any matched three-arm claim.

This is authenticated in-process TestClient HTTP, SQLite, encrypted in-memory archive storage, scrypt factor 1 and the maintained declared fixture conditions. Fresh/warm/evicted refer to Native reader state, not host OS cache clearing. The observer retains sets/ciphertext in memory and does not establish production memory bounds. The raw record correctly says `not-compared` / `no-measured-reference`; no redesigned B/C pair, optional-store count, SQL count, physical-sync count, production latency floor or speedup is asserted. [Complete conditions and selected comparison design](MEASUREMENTS.md)

## 4. Source-derived discovery

The real existing discovery owners enumerated 91 released distributions, 105 Python package detections, 3,881 public Python surfaces, 21 configuration-validator detections, 156 environment reads, nine state owners, 179 operations, 413 growth declarations including reachable delegates, 207 delegate edges, 55 qualification workloads, 18 runtime images and four installation roots. These are generated observations, not a manually maintained source registry or proof every owner has been qualified.

The growth projection says **`not-executed`**. Its 3,513 unestablished public surfaces neither prove runtime failure nor require finishing all of #964/#469 before the first slice. Affected private delegates still require reconciliation. [Summary](evidence/coverage_summary.json) · [discovery execution](evidence/coverage_execution.json) · [raw/compressed identities](evidence/coverage_artifacts.json)

Large raw JSON is preserved losslessly with deterministic gzip and both compressed/uncompressed hashes. The exact helper used for the full scan is also retained. Later changes strengthen source-before/after guards and qualify imports; that final helper's guard tests and import smoke pass, but the full scan was not repeated. Actual tracked-product snapshots matched across **2,564 files**, digest `d97668b98f8481c1132a184aff3e28adf4b231b57736598754d17225722a3a5f`. [Coverage method and exact gate mapping](COVERAGE_AND_CUTOVER.md)

## 5. Static checks, corrected attempts and blocked gates

Final pinned Ruff lint and formatting pass for all seven reference Python files. Strict mypy with `--follow-imports=silent` passes for the four implementation/runner modules. This is scoped reference checking, not the repository's full lint/type gate. [Final commands, outputs and input hashes](evidence/reference_static.json)

Original adverse/setup observations are retained:

- The growth cohort initially had three setup errors because JUnit xunit2 rejects `record_property` under warnings-as-errors. The exact same three nodes passed using the maintained lane's `junit_family=xunit1`. [Initial result](evidence/safety_growth_setup_failure_pytest.json) · [initial JUnit](evidence/safety_growth_junit.xml)
- Measurement development found one import-order and nine typing diagnostics, then corrected them without ignores or `Any`; its final 48 tests and static checks pass. [Lane static history](evidence/measurements_static_checks.json)
- The first aggregate static check found a trailing runner newline and discovery import-resolution/export issues. Qualified imports of the existing owners and the defining `CI_SHARDS` owner resolved them; the final combined suite ran after those edits. [Initial aggregate static result](evidence/reference_static_initial.json) · [initial successful combined result](evidence/reference_initial.txt)

Actual `make lint` and the valid F1 `make profile` invocation both stopped at toolchain preflight because **mise is absent** (make exit 2, preflight command exit 127). [Exact attempts](evidence/make_preflight.json) Direct Python producer success does not qualify either Make wrapper. Docker, age, age-plugin-batchpass and psql are also unavailable here. No installed wheel/image/Compose, PostgreSQL, macOS/Windows, real-provider, backend/machine-crash or power-loss qualification was performed for this packet.

No full repository unit, build, distribution, Linux lifecycle or exact final implementation/tested-merge CI qualification is claimed. The reference branch does not trigger the usual PR/main CI path; actual publication status is recorded in the owning issue. Required missing/skipped/blocked scopes remain incomplete, not waived. Follow [POLICY_AND_CLOSURE.md](POLICY_AND_CLOSURE.md) and the actual existing gates on the eventual implementation source.

## 6. Reproduction and evidence integrity

With the repository's locked Python/dependencies active, from the checkout root run:

```bash
python .reference/implementation/run_reference_checks.py
```

This external producer runs only the reference witnesses, binds the source-owned numeric policy, uses a fresh external temporary directory and preserves original stdout, JUnit, timing and fault receipts. It is not a new maintained developer-facing command. It never runs a provider or the held large profiling campaigns. Exact maintained-cohort commands are in the linked invocation records; any deliberate rerun is a new execution with its own identity, not reconstruction of an old missing receipt.

[VALIDATION.json](VALIDATION.json) binds every other file in this packet by relative path, byte count and SHA-256, including source, documents, raw evidence and compressed snapshots. It excludes itself to avoid a circular digest. Git supplies the published tree/commit identity; #1000 records the actual published SHA after the remote tree is verified. Do not modify or relabel historical original evidence to make a later source appear qualified.
