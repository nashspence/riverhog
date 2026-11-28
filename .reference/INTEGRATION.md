# Minimal integration with current shared support

Start with the work-account and one useful simplification in [POLICY.md](POLICY.md). This page connects that real operation to existing shared machinery; it does not prescribe a new benchmarking subsystem.

## 1. Exact current contract

Use [all@66b8897c212ce826ef6249ebca767ba0306842bc](https://github.com/nashspence/all/tree/66b8897c212ce826ef6249ebca767ba0306842bc). #75 implemented #68, #77 corrected a test, and #76 activated the reviewed package. Qualified implementation source is `6758fd2b75a92fd44277350ca8eac981c32a8010`. Package version metadata alone is not provenance.

Primary executable documentation:

- [Performance contract](https://github.com/nashspence/all/blob/66b8897c212ce826ef6249ebca767ba0306842bc/docs/performance.md)
- [Current approval and refresh](https://github.com/nashspence/all/blob/66b8897c212ce826ef6249ebca767ba0306842bc/docs/profile-refresh.md)
- [Prepared-storage recipe](https://github.com/nashspence/all/blob/66b8897c212ce826ef6249ebca767ba0306842bc/docs/prepared-storage.md)
- [Authority loader](https://github.com/nashspence/all/blob/66b8897c212ce826ef6249ebca767ba0306842bc/all_policy/performance/contracts.py)
- [Allowance tests](https://github.com/nashspence/all/blob/66b8897c212ce826ef6249ebca767ba0306842bc/tests/test_allowances.py)

The prior handoff's old package pin and absence of executable suspension/core-profile readiness are obsolete. A historical active status is not current approval.

## 2. One declaration, existing bindings

At the independently approved contract commit, use schema 2 in `.all/performance/limits.json`. An operation may remain an ordinary `{profile,max_wall_ns}` row or declare:

```text
operation:
  profile: approved shared profile ID
  core_ns: independently approved positive integer
  optional_ns:
    progress: independently approved nonnegative integer, or null
    resumability: independently approved nonnegative integer, or null
    logging: independently approved nonnegative integer, or null
```

This is a design worksheet, not active JSON or invented approval. Do not maintain derived numerical rows separately. The loader derives core, each applicable singleton and all-on, deduplicating only its equivalent zero/one-feature configuration. Equal numeric limits or apparently equal outputs do not permit case omission.

Each derived ID has its existing reviewed binding with an exact ordered `features` array (progress, resumability, logging). Core uses `[]`. The shared parent passes `ALL_PERFORMANCE_FEATURES` containing all three booleans. The production adapter must apply and propagate it; the environment variable does not stop producers on its own.

Keep operation unit/completion, adapter/fixture/verifier hashes and cold counterparts in the approved `.all/performance/bindings.json`. The runner reads pinned approved Git objects and checks approved files before/after. The candidate cannot activate easier limits, applicability, fixtures, reset rules or boundaries.

## 3. Honest real activity and result witness

For allowance bindings, the command verifier emits only one JSON object on stdout (at most 64 KiB), or warm verify returns:

```json
{"verified": true, "activity": {"progress": 0, "resumability": 0, "logging": 0}}
```

Those zeroes illustrate core mode only. Every count must be an exact nonnegative integer; an enabled feature requires completed activity greater than zero, a disabled feature requires zero. The approved verifier independently validates required output and extracts genuine per-observation producer facts. Do not echo selection flags, hand-write “exercised”, count stale artifacts, or emit zero because instrumentation missed the producer.

Reuse existing output/state/log/progress facts and focused producer hooks where necessary. Keep any added observation small and describe its cost. Do not introduce a general event/accounting runtime. If a periodic feature cannot be honestly exercised in this fixture, state the missing regime rather than claiming it qualified or changing it to null.

Warm adapter interface remains `prepare(subject,work)`, `run(context,repeat)`, `verify(context,result,repeat)`, optional `close(context)`; repeats are 0/1/2. Run includes all work for the promised result, locks/joins/sync and receiving. Verify observes completed work; close supplies no deferred product work. Every warm binding has the same-selection/profile cold command counterpart including ordinary startup/opening/completion.

## 4. Small pilot and feasibility

Propose one p1 warm/cold complete fresh collection publication with the reviewed canonical 16×65,536-byte/history geometry. These fixture dimensions prescribe neither a production retention architecture nor #994's machinery.

Include only genuinely applicable features. With three applicable features, warm/cold produces 10 cases and 30 observations; multiplying by p1/p2/p4 produces 30 cases/90 observations. The current runner executes the entire expanded authority, has no case filter or automatic sharding, and requires matching cold bindings in it. Its limits are 32 cases, 300s per cohort and 30s per phase.

Account for admission, startup, observations, verification and drain before selecting the pilot scope. If the original slow path cannot complete, retain incomplete evidence and use narrow diagnostics to simplify it. Do not silently partition the contract, drop modes, raise a watchdog or shrink a canonical unit. A genuine shared constraint warrants the smallest shared change with applicable review/qualification.

The F1/F2/F4 marginal diagnostic series is distinct from canonical acceptance and does not require a new runner. Add p2/p4 and other regimes as explicit #991 follow-ups with honest unqualified scope until their approved evidence exists. Do not repeat expensive complete fault/recovery campaigns inside every 30-second verification phase; preserve their applicable exact-source evidence separately.

Use supported actual production security, KDF, polling and backend configuration. Ensure the actual database/archive/workspace/container mounts execute on admitted storage; an in-memory test store, tmpfs client state or unrelated Docker volume cannot inherit that claim. Every repeat publishes fresh members/history; an already completed publication cannot be a fast no-op.

## 5. Current approval and prepared storage

```sh
# Optional diagnosis only: the performance command already checks this.
"$PERF_PYTHON" -I -B -m all_policy performance-status \
  public-linux-x64-4cpu-prepared-ext4-v2

"$PERF_PYTHON" -I -B -m all_policy performance \
  --authority /work/approved-contract \
  --approved-revision APPROVED_CONTRACT_FULL_SHA \
  --repository nashspence/riverhog \
  --subject /work/candidate \
  --storage "public-linux-x64-4cpu-prepared-ext4-v2=$ROOT_LOOP_A"
```

Status takes positional profile IDs; no IDs checks native-api-v1 and prepared-ext4-v2. It returns 0 active / 2 unavailable. Performance itself obtains current approval before admission/candidate execution. A current-authority exception is failed/exit3 without an operation sample; do not report it as a latency miss.

The parent uses GH_TOKEN, then GITHUB_TOKEN, otherwise public reads, against the fixed shared main authority, compact profile-evidence and live run inventory. Supply only needed read capability to the trusted parent; candidate subprocesses receive neither token. No cached-green, local-approval or bypass flag exists. Ten-day freshness, sticky original failures and reviewed reinstatement enforce availability/comparability prospectively. There is no per-consumer twelve-worker research campaign.

Prepared-v2 requires exact Python3.12.16, public GitHub-hosted Ubuntu24.04 x64, four available CPUs, at least14GiB effective memory, and the shared setup of two owned256MiB ext4 resources. It is configuration-admitted; probe timings stay diagnostic and do not scale budgets. Both mounts and protected backing identities must admit. The pair shares a failure domain and does not certify physical power loss.

Follow the current central recipe for setup/verify/cleanup using actual trusted push/dispatch/schedule context on allowlisted refs/heads/main. Do not copy the implementation or spoof event/ref. Keep control/interpreter/evidence on responsive storage outside candidate/test roots. Always run cleanup and retain evidence, including setup failures; use the actual protected partial report where needed. Postflight or cleanup failure cannot become a green acceptance result.

## 6. Wire acceptance without changing its meaning

1. Review/bootstrap thin controls, adapter/verifier, contract authority and trusted workflow using current repository governance. A bootstrap green result is not numerical acceptance.
2. From real trusted main context, independently select candidate and intended acceptance target. Record installed shared revision, approved contract revision, candidate source/artifact provenance and run/attempt. The candidate checkout must be clean including ignored files; builds/dependencies/outputs stay outside it.
3. Execute admission and the complete cohort on the approved resource. Retain raw rows, actual feature witnesses, source/profile/authority snapshots, excluded costs, total work and infrastructure finalization.
4. A trusted result consumer executes no downloaded candidate artifacts. Validate the complete source/run/attempt/pins and successful infrastructure lifecycle. Map only a correct complete raw pass to success. Missing, failed, miss, unqualified, skipped, cancelled or stale results cannot satisfy required acceptance.
5. Wire this result into the existing qualification/actions/workflow-evidence owners for campaign and applicable release acceptance. For PRs, confirm the exact live head before current-head acceptance; direct-commit work binds the exact requested commit. A dispatch's automatic check belongs to main, not the separately selected candidate.
6. If an explicit later governance decision makes this merge-required, publish the result on candidate head and install/read back the actual rule and trusted provenance. Current release.toml's no-PR/pre-push-gate policy is not silently changed by adding YAML.

Pass/miss/unqualified/failed remain distinct. Every complete raw observation must fit its fixed configuration limit. A product run/start/verify/drain timeout is failed; profile preflight timeout is unqualified; setup/cleanup timeout is infrastructure failure. Earlier complete misses remain in the evidence even when a later failure determines the cohort result. Unavailability does not manufacture a pass or erase a valid prior miss.

## 7. Keep shared changes small

Use Riverhog's first real operation to expose an actual problem, not to speculate about every future consumer. A shared fix should name the failing consumer case, the general requirement and one relevant counterexample; preserve independently approved limits and source/profile qualification.

Fishhat later supplies its own operation, core authority, optional conveniences and values. Its protected checkpoint and acceptance commitments remain core; a serial CAS does not acquire p4 merely because Riverhog publishes collections. The reusable surface is the declaration/control/witness vocabulary and shared procedure, not Riverhog's fixture or architecture.
