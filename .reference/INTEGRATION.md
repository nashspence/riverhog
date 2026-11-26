# Shared #64 implementation research for Riverhog
Research snapshot: 2026-11-26 UTC. Read-only source review; no consumer campaign or production changes executed.

## Exact source and authority

- Active shared main: `29d2af516a91d762094da1da55c71d07d79e095c`, tree `93b8538a2c409b211a2758f6e37c21e4bb6a1080`.
- [all#65](https://github.com/nashspence/all/pull/65) is merged; merged head was `0e8d83f36a62bd850bee837ee2e3403b61b910f5`.
- [all#66](https://github.com/nashspence/all/issues/66) is closed completed and expressly approves `public-linux-x64-4cpu-prepared-ext4-v2` for small storage-dependent CI timing in Riverhog/Fishhat.
- Qualified performance implementation source: `5086428a29871eeb267ea7225537aa0225930bac`. Enrollment and #65 report byte-identical activated performance modules.
- [all#64](https://github.com/nashspence/all/issues/64) remains open: shared delivery complete, consumer numerical contracts/bindings/required paths and later-date core-profile maintenance still pending.
- The older ed6c2dd consumer handoff is useful for historical source mapping; active operation ownership and acceptance outcomes are selected anew under #991 but predates approved storage enrollment and #65 merge. Do not copy its obsolete “no enrollment / main has no runner” status into the new handoff.

Pinned primary sources:
1. [performance contract](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/docs/performance.md)
2. [prepared-storage operational recipe](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/docs/prepared-storage.md)
3. [versioned enrollment](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/docs/storage-profile-enrollment.md)
4. [authority parser](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/all_policy/performance/contracts.py)
5. [outer runner](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/all_policy/performance/runner.py)
6. [warm wrapper](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/all_policy/performance/worker.py)
7. [profile admission](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/all_policy/performance/profiles.py)
8. [prepared resource implementation](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/all_policy/performance/prepared_storage.py)
9. [example limits](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/examples/performance/limits.json) and [example bindings](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/examples/performance/bindings.json)
10. [central weekly refresh](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/.github/workflows/storage-profile-refresh.yml)
11. [package metadata](https://github.com/nashspence/all/blob/29d2af516a91d762094da1da55c71d07d79e095c/pyproject.toml)

## Exact adoption shape

Package `nashspence-all` version metadata is merely `0`; install by full source SHA above, never trust that version string as provenance. Keep locked dependencies and installed shared machinery outside candidate source. Use a dedicated single-threaded isolated process (runner installs Linux subreaper and pidfd lifecycle handling):
```sh
"$PERF_PYTHON" -I -B -m all_policy performance ...
```
The prepared-v2 shared interpreter AND timed Python operation require exactly Python **3.12.16**. The older native/native-sync/tmpfs profiles require exactly 3.11.9; do not combine those profiles into one interpreter cohort casually.

Select three independently identified inputs in the trusted workflow:
- installed shared revision;
- independently approved contract commit;
- exact candidate commit/installed artifact.

At the approved contract commit:
- `.all/performance/limits.json`: only `{"schema":1,"operations":{id:{"profile":ID,"max_wall_ns":POSITIVE_INTEGER}}}`.
- `.all/performance/bindings.json`: `schema`, literal `repository:"nashspence/riverhog"`, and exactly the same operation IDs.
- Every binding has `mode`, nonempty `unit`, nonempty `completion`, and `approved_files` mapping relative adapter/fixture/verifier paths to SHA-256.
- Command mode requires nonempty argument arrays `command` and `verify`; optional `prepare`. Substitutions are `{python}`, `{subject}`, `{work}`, `{repeat}`.
- Request mode requires `adapter` in approved_files and `cold_operation` naming a command binding in the same contract.
- IDs match `[a-z][a-z0-9_]{0,79}`; max 32 operations/cohort.
- No unresolved/null budget sentinel is accepted. Keep unapproved numbers as a design proposal, not a fake active JSON contract.
- `git show APPROVED_SHA:path` loads authority from Git objects, not editable candidate files; approved-file hashes are checked before/after. Review/workflow protection must actually control the approved SHA. A full SHA or “separate file” alone does not establish independent approval.
- Candidate checkout must be clean, including ignored and untracked files. Install/build/output paths outside it. Source SHA does not prove any arbitrary installed binary came from it; retain source-to-wheel/install provenance and bind the actual code import.

Warm adapter functions: `prepare(subject,work)`, `run(context,repeat)`, `verify(context,result,repeat)->True`, optional `close(context)`. Repeat IDs are 0,1,2. Parent times request send through completed response; child timing claims do not control verdict.
- prepare may construct input fixtures and genuinely persistent service handles; it cannot precompute required product work.
- run includes the promised production initialization/processing/output/sync/ack/locks/joins/drain.
- verify observes already completed outputs and must not wait for omitted work.
- close cannot provide deferred publication/flush/processing.
- Startup, verification and final worker teardown are reported separately for warm mode; every warm operation has a reviewed cold full-command counterpart including normally recurring opening/import/startup/completion.
- For publication, no receipt-before-custody-drain substitution. Actual product upload concurrency p1/p2/p4, not an adapter process pool pretending serial publications demonstrate parallelism.

## Prepared storage lifecycle and guards

Requires **public GitHub-hosted Ubuntu 24.04 x64**, four available CPUs, >=14 GiB effective memory. Two owned 256 MiB ext4 loop filesystems with verified direct I/O, barriers, data=ordered, max_batch_time=0. Backing root keeps its existing data mode; helper changes/restores only barrier/batching options. Protected root-owned immutable 0400 manifest binds source/run/repository/ref/device/backing identity.

Only trusted `push`, `workflow_dispatch`, `schedule` on explicitly allowed `refs/heads/main` are supported by recipe. Ordinary `pull_request` does not satisfy this guard. A PR candidate can be checked out by a trusted main dispatch lane; do not spoof GITHUB_REF/event or weaken the shared guard. Require exact-candidate result association and actual enforcement by the selected campaign/release acceptance consumer; adding YAML alone does not prove that acceptance consumes it. Merge-required status enforcement needs a separate explicit governance decision.

Run root preparation/operation/verification/cleanup in the disposable approved worker with the actual GitHub context preserved. Keep the interpreter/control reports on responsive control storage, outside tested mounts and clean candidate checkout.

```sh
"$PERF_PYTHON" -I -B -m all_policy.performance.prepared_storage setup \
  --repository nashspence/riverhog --allowed-ref refs/heads/main \
  --worker "$GITHUB_JOB" --report "$CONTROL/preparation.json" \
  > "$CONTROL/resource-paths.json"

# Extract ROOT_LOOP_A and MANIFEST from successful setup JSON.
"$PERF_PYTHON" -I -B -m all_policy performance \
  --authority /work/approved-contract \
  --approved-revision APPROVED_CONTRACT_FULL_SHA \
  --repository nashspence/riverhog --subject /work/candidate \
  --storage "public-linux-x64-4cpu-prepared-ext4-v2=$ROOT_LOOP_A" \
  > "$CONTROL/performance.json"

"$PERF_PYTHON" -I -B -m all_policy.performance.prepared_storage verify \
  --repository nashspence/riverhog --allowed-ref refs/heads/main \
  --manifest "$MANIFEST" --report "$CONTROL/postflight.json"

"$PERF_PYTHON" -I -B -m all_policy.performance.prepared_storage cleanup \
  --repository nashspence/riverhog --allowed-ref refs/heads/main \
  --manifest "$MANIFEST" --report "$CONTROL/cleanup.json"
```

These are illustrative exact APIs, not a complete failure-safe shell script. Implement separate unconditional cleanup and evidence-upload steps. A verification failure must not skip cleanup, and successful cleanup must not erase the earlier gate exit. Failed setup can be cleaned from original protected failure evidence via `cleanup --partial-report ORIGINAL_REPORT`; do not fabricate a manifest.
- Setup/verify/cleanup watchdog defaults 180/60/120 seconds; product budgets are distinct.
- `prepared_storage.inspect(work)` returns errors and both resource_roots; use it inside actual adapter worker to bind operation storage without reimplementing manifest inspection.
- Admit both actual filesystems before candidate execution. Keep all five probe timing series: 3 each of serial CPU, p4 CPU, SHA256, warm reads, fdatasync64.
- V2 admits observed configuration/custody and correct bounded probes. No fixed probe-speed envelope, time normalization or “slow diagnostic means ignore product miss.”
- Two filesystems share backing host/failure domain. This is synchronized timing infrastructure, not physical power-loss or independent-device proof, nor a replacement for Riverhog's own host/OS/backend fault scope.
- 256 MiB per mount constrains usable fixture footprint; do not silently enlarge profile, omit history, or extrapolate to release scale.

## Verdicts, total cost, runner bounds

For each case all three observations must complete correctly and fit the unchanged raw budget.
- 0 pass; 1 miss; 2 unqualified; 3 failed (incorrect/incomplete/invalid/changed-source).
- A single over-budget observation is retained as mixed miss; averages/minimum/reruns never clear it.
- Admission is before candidate work; postflight cannot retroactively erase a miss.
- No automatic runner retry or selection of faster workers.
- Runtime limits: 30 seconds/phase and 300 seconds/cohort, max 32 operations. Current historic ~30.5s F1 or hundreds-of-seconds F16 would likely hit incomplete/failed execution, not produce a valid completed miss. Do not widen common watchdogs locally, shrink canonical extent or relabel timeout as budget pass. Use existing narrow diagnostics to improve the complete path into the small CI execution envelope; retain failed evidence, and request a shared capability change only for an actual product need.
- Keep product wall time separate from setup/admission/startup/verification/cleanup and total gate work. A warm mean cannot be reported as cold latency.
- Shared `qualify_storage` collector is for unapproved infrastructure evidence (exit 2 even if numerical screens pass), not ordinary per-PR consumer qualification. Consumers should use the approved prepared profile, not repeat central campaigns.

## What current evidence establishes

Prospective v2 source campaign: 12 original workers, no retries/replacements; 288/288 generic observations within unchanged 320/1280ms screens; 72/72 real consumer observations complete/correct. Riverhog witness = 16 fresh 4 KiB `PrimaryFiles.put_bytes` calls/window, 36 observations 10.871–69.181 ms. Its separately recorded startup was 2.242–3.555s. These are narrower local primitive results, not full CollectionProducer publication/upload, supported backend/provider or release qualification. 320ms is an experimental comparison, NOT approved Riverhog numerical authority. Fishhat's 473.010732 and 605.141475ms misses remain; approving resource did not waive them.

New first integrated Riverhog case should be a complete fresh 16-member `CollectionProducer.publish_inputs()` on the supported actual backend at native configured upload concurrency 1,2,4. Preserve auth/encryption/provenance/history/primary state/progress, finish receipt and original worker/custody drain; verify exact independent reading/recovery. Actual settings (notably poll_seconds defaults) must be part of approved binding. Old history profiler's one append/member, in-process HTTP and in-memory encrypted test store are useful diagnostics but do not qualify actual storage or parallel upload.

## Startable versus blocked acceptance

The maintainer abandons the entire #994 program, including #995–#1000. Shared-runner adoption and #991’s newly scoped operation outcomes do not depend on completing those issues. PR #1001 is preserved only as optional source/evidence material; salvage does not retain its architecture or acceptance program. Continue from verified main, selecting independently useful changes explicitly.

Start now: adopt the new policy/precedence, reconcile stale instructions, inventory material operations, define/propose numerical contracts, implement/review thin bindings and actual backend topology, integrate pinned shared runner/paired resource, improve real production operations, retain misses/failures and useful existing safety/growth evidence.

Acceptance requires: independently approved numerical limits and semantic binding; real trusted required workflow and exact-candidate evidence consumer; actual admitted resources; all three complete correct raw observations within limits; preserved applicable native fault/custody/recovery evidence. No source-only proposal, central primitive results, green structural policy job, or mere shared-package installation clears these.

Prepared v2 resource is already approved/available. Older core-profile later-date/image validation and finite refresh/availability rule are still explicit #64 shared-owner obligations; avoid representing those profiles as dependable required gates until settled. This is not a new global blocker on available v2 consumer work.

Prepared storage weekly refresh is already in shared main, pins frozen source 5086428..., twelve workers, first attempts only, final all-jobs-success requirement. Failed/incomplete refresh suspends shared approval pending recorded review; no instant revocation of pinned consumers is implemented. Consumers must follow current shared decisions. Do not make every Riverhog PR run this campaign. Large full workflows/recovery/provider qualification outside CI remain separately budgeted/repeated and bound to claimed scale/backend/history, with explicit release owner.

## Worked main-dispatch integration and bootstrap

This is a proposed implementation sequence, not a claim that a workflow or protection setting is already installed. The owning transition issue selects recurring CI evidence required for campaign and applicable release acceptance as the initial consumer, through the existing qualification/actions/workflow-evidence owners. A merge-required protection change needs an explicit additional governance decision; the optional mechanics below describe that extension.

Current pinned Riverhog source is `be483fd6d567616d5fcd355202d61f1e682efe75`. Its [CI workflow](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/.github/workflows/ci.yml) has `CI gate` depending on plan/repository/units/compose/client-platforms/images only. The [validation observer](https://github.com/nashspence/riverhog/blob/be483fd6d567616d5fcd355202d61f1e682efe75/.github/workflows/validation-evidence.yml) listens only to CI/CodeQL and checks out `github.workflow_sha`. Neither automatically consumes a new main-dispatched performance report. The policy research also identifies the current repository's no-pre-push-gate policy; reconcile that explicitly rather than silently turning this into a new merge blocker.

1. Land the reviewed bootstrap workflow and thin approved adapter/verifier/fixture files on main using existing acceptance. Bootstrap success is only integration evidence, not numerical performance acceptance. Independently approve the six initial numerical/semantic contract rows, then pin that reviewed contract commit from trusted main configuration. Keep implementation optimization candidates separate from authority changes. No candidate input may select its own authority SHA, profile, raw limits, allowlist or alternate adapter hash.
2. Dispatch **on actual `refs/heads/main`** with an independently selected exact candidate SHA and intended acceptance target as data. For PR candidates, also resolve the current PR head through GitHub and require exact equality. For direct-commit candidates, bind the result to the requested exact commit and verify that the campaign/release consumer requires that same commit. Record candidate/head/base where applicable and `github.workflow_sha` plus run/attempt. Checkout trusted control/authority/shared sources separately from the clean candidate. Keep real `GITHUB_SHA`/ref/event as dispatch context; the candidate revision is a distinct report field. Avoid ordinary `pull_request` or PR `workflow_call` context for prepared storage; the latter inherits the caller event/ref.
3. Run setup, admission, the six-case cohort, verification, unconditional cleanup, and evidence upload on a disposable approved public Ubuntu24.04 worker. The producer has only needed read permissions, no persistent checkout credentials and no publishing credential in the candidate environment. The installed shared source and Python3.12.16 come from trusted pins. The profile guard's implementation can accept explicit push/dispatch branch allowlists, but the **approved v2 recipe is main-only**; do not expand that approval by exploiting a more generic helper parameter.
4. A separate publisher/observer job checks out trusted control source only and never executes downloaded candidate artifacts. It verifies the exact run id/attempt, workflow identity/ref/revision, independently selected candidate/authority/shared pins, complete artifact set, producer result, cleanup disposition, and raw gate exit/verdict. Retain all failed/mixed/missing evidence. For PR candidates, re-resolve the live PR head before publishing current-head success; a stale result remains attached to its old candidate. For direct-commit candidates, verify the exact requested commit and acceptance target again.
5. If repository policy decides this is required for **merge**, post an explicit uniquely named Check Run to `head_sha=CANDIDATE_SHA`, with run URL and evidence identity. A workflow_dispatch run's automatic check belongs to its dispatch branch's commit; green main YAML is not evidence on the candidate. A check publisher can use job-scoped `checks: write` with the GitHub App installation token, while the measurement job remains read-only. Use the existing trusted evidence machinery if extended to enforce the same provenance rather than creating a general report service.
6. Record and read back the actual required-check/rules configuration and expected source app if merge enforcement is adopted. A same-name check alone is not proof of trusted provenance: GitHub Actions app attribution does not distinguish workflows. The reviewed-code boundary must protect which workflow/publisher can grant acceptance; if stronger isolation is required, use a dedicated check-writer App credential scoped to the protected main publisher. Do not claim hostile-code isolation from this reviewed-code runner.
7. Only a valid raw `pass` plus complete successful infrastructure/evidence lifecycle maps to check `success`. `miss`, `unqualified`, `failed`, phase timeout, missing reports, cancelled runs and pending authority must fail or remain pending as acceptance requires; never translate them to `neutral` or `skipped`, which GitHub can accept for required checks. Preserve the original semantic verdict in report and check explanation. A missing publisher result must not accidentally grant success.

If existing governance keeps direct-push/merge latitude, make the exact-candidate result a required **campaign/release acceptance record** with an explicit consumer instead. State that narrower enforcement honestly. Installing a workflow or documenting a check name does not prove either kind of acceptance is wired.

Primary GitHub platform sources for the above mechanics:
- [workflow_dispatch and workflow_call event SHA/ref behavior](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#workflow_dispatch): dispatch uses the last commit of the selected branch/tag; reusable workflows inherit caller context.
- [Create a check run](https://docs.github.com/en/rest/checks/runs?apiVersion=2022-11-28#create-a-check-run): `head_sha`, check name and Checks-write App capability.
- [GITHUB_TOKEN](https://docs.github.com/en/actions/concepts/security/github_token): per-job repository-scoped GitHub App installation token.
- [Require status checks](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches/about-protected-branches#require-status-checks-before-merging): source app selection and successful/skipped/neutral acceptance.

## Finite first contract proposal and budget decisions

Preserve #991's established fixture geometry for the first actual product binding: **16 × 65,536-byte payloads**, F16/C1/K2/Q1 (shared root16, shared claim1, retained journals2, required1). The policy research traced this to #991 and `publication_geometry.py` at the pinned Riverhog revision, blob `760567488ec926d74cc4bfcb596d04e5faa78e98`. Each repeat must publish fresh identified members/history at the approved extent. Use bounded approved output verification per observation, and retain applicable exact-source fresh/warm/evicted Native and independent recovery qualification after removal of source/upload scratch separately; do not repeat the entire expensive proof inside every verification phase. Confirm the new preparation actually stages the required journal/history topology on real prepared-ext4 resources; changing only labels is insufficient. These are retained #991 fixture dimensions, not a required production retention architecture or a commitment to complete #994. Approve production durability/recovery outcomes independently. The central4KiB primitive witness does not define this fixture.

| Proposed operation ID | Mode | Approved timing boundary | Numerical design input |
| --- | --- | --- | --- |
| `publish_collection16_p1` | request | Complete public `CollectionProducer.publish_inputs()` at actual upload concurrency1 | `B1` raw positive ns |
| `publish_collection16_p2` | request | Same fixture/semantics at actual supported concurrency2 | `B2` raw positive ns |
| `publish_collection16_p4` | request | Same fixture/semantics at actual supported concurrency4 | `B4` raw positive ns |
| `publish_collection16_p1_cold` | command | Same p1 unit including ordinary process/import/open/startup and complete drain | `C1` raw positive ns |
| `publish_collection16_p2_cold` | command | Same p2 unit including ordinary process/import/open/startup and complete drain | `C2` raw positive ns |
| `publish_collection16_p4_cold` | command | Same p4 unit including ordinary process/import/open/startup and complete drain | `C4` raw positive ns |

All six select `public-linux-x64-4cpu-prepared-ext4-v2`; each request row names its corresponding cold row. Shared max32-operation bound leaves room for separately designed retrieval/verification/repair/CLI units later. Six rows × three observations do not guarantee fitting300s: retain phase/cohort watchdog outcomes and optimize real work. The existing public polling default2.0s is part of production semantics unless independently changed/approved; the legacy helper's0.05s cannot silently define acceptance.

The existing filesystem recovery qualification topology is a useful correctness starting point, but policy-source research identified test settings that cannot silently migrate into this timing contract: `scripts/test_filesystem_recovery_qualification.sh` uses archive scrypt work factor10 while runtime default is18; client `/state` is tmpfs and adapter storage a Docker named volume. Pin the actually supported encryption/KDF/security configuration, map all relevant persistent production storage to the admitted ext4 resources (including actual container mounts), and document any legitimate volatile scratch separately. Reusing topology does not approve reduced security work or unrelated storage.

`B1/B2/B4/C1/C2/C4` are explicit **unresolved design decisions**, not valid limits JSON, not derived from current baseline, and not acceptance waivers. Complete the implementation/proposal and record them for independent design approval; other startable policy, inventory, provenance, topology and optimization work need not wait. Independently approve actual finite numbers before shared acceptance execution. Equal limits promise no speedup. Do not copy100ms/150ms primitive screens or320ms infrastructure comparison into complete publication budgets.

## Timeout evidence precision

The runner's `_measure` retains observation rows on run/verify errors, including observed elapsed time where timing began, `completed`, `verified`, and error; incomplete warm workers stop further repeats. Startup/drain errors appear in excluded stages. The outer decision is `failed` when incomplete or a stage errors. A completed verified observation over the approved raw number is a `miss`; a phase watchdog is not an admitted completed latency result, even if its observed partial elapsed time exceeds that number. The final cohort precedence is failed, then miss, then unqualified, then pass. Thus a later failure can give the overall cohort `failed` while earlier complete misses remain present in operation evidence. Keep that evidence and do not flatten the final status to claim all observations were merely misses or all met budget.

Distinguish the phase: operation/startup/verification/drain timeout produces `failed`; a **profile preflight timeout** is caught as admission evidence and produces `unqualified`; setup/cleanup watchdog failure is infrastructure failure outside the product observation verdict. Every case blocks acceptance, but the original classification must remain accurate.

