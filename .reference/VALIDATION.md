# Validation and reproducibility

This is reference research for #991 under #903. Product files are unchanged from
`8ce49de94c32ae39562aab5ea4178b1dc8c75e77`. The one-member diagnostic ran on that
clean implementation checkout with observer files outside it. The delivered
observer Python bytes match the hashes recorded by the actual helper. Later
publication adds only `.reference/` material; it does not claim that the final
reference commit's full CI or an integration merge was qualified.

## Performed

| Check | Actual result and boundary |
| --- | --- |
| Current authority/source review | Read #903, #991 and its clarifications/checkpoints, relevant #964/#990 checkpoints and PR #979; inspected exact source owners and maintained growth/lifecycle tests. |
| Source relationship | Clean source at `8ce49de...`; runtime product paths match the prior `2eb7e48...` reference. Only the two PostgreSQL qualification paths differ outside `.reference/`. [Audit](evidence/source-audit.json) |
| Probe semantic check | Passed on pinned Python 3.12.3: actual admitted cursor/connection identity, complete grant receiving, one-shot executemany generator, SQL/SQLAlchemy errors, return and KeyboardInterrupt identity, nested timing, iterator exhaustion, original thread join. [Receipt](evidence/probe-selfcheck.json) |
| Bootstrap activation | Exact helper invocation with no workload intentionally returned status 2; produced the expected incomplete-verification diagnostic, with 23 hooks installed and clean unchanged source. No publication ran in this negative check. [Receipt](evidence/probe-activation-check.json) |
| One real maintained profile | Passed one 65,536-byte member, F1/C1/K2/Q1; authenticated HTTP, encrypted fixture store, exact Native history, all three cache-state passes, source/upload removal, original helper and workspace receiving. [Profile](evidence/one-member-profile.json), [Make receipt](evidence/profile-make.json) |
| Attribution integrity | Valid observer, no overflow/unfinished spans, three verification calls reconciled to the profile, nonnegative nested exclusive values, covered CPU no greater than process CPU, clean unchanged source and matching observer bytes. [Summary](evidence/one-member-summary.json) |
| Static reference code checks | Pinned Ruff lint and formatting for all three observer Python files. No product behavior changes are represented by this result. |
| Publication integrity | Supplied blobs/tree, sole audited-base parent and final remote identity are checked at publication; exact published SHA and final lint result are recorded in #991. |

The probe succeeded with 50,968 catalog cursor attempts including 1,172 raised
attempts. Error families were not collected. That is an observed handled-error
count, not a product failure, a count of retries, or proof that all errors were
the same contention. The semantic selfcheck's deliberate SQL exceptions and
the no-workload activation check are retained separately from publication.

The maintained profile reports `completion_verified=true`, helper exit 0,
`child_received=true`, and `workspace_cleanup_received=true`. The enclosing Make
process also exited 0 and was received. Its execution duration was 139.056 s;
the nested profile duration was 136.777 s. A zero-byte uv lock file remained in
the outer task temporary directory after profile cleanup; it was removed after
receiving, leaving that task temporary directory empty. This is distinct from
the profile's own workspace receipt. [Conditions before](evidence/conditions-before.json),
[conditions after](evidence/conditions-after.json)

## Exact workload and commands

The input is [one-member.json](experiments/one-member.json). It selects
one append, F1/C1/K2/Q1, 65,536 payload bytes, the maintained 0.05-second
coordinator polling, and a 300-second finalization resource condition. It does
not weaken exact output, history, encryption, authorization, custody or receiving.
The encrypted in-memory test store, scrypt factor 1, disabled producer cache,
SQLite and in-process HTTP are diagnostic conditions, not production or provider
qualification.

The actual command used the real Makefile with:

```bash
PYTHONPATH="$observer_directory:$audited_worktree" \
RIVERHOG_991_DIAGNOSTIC_DIR="$external_observer_output" \
TMPDIR="$external_tmp" \
make profile \
  'UV_RUN=mise x python uv -- uv run --locked --all-packages --group dev' \
  args="--scenario collection-publication \
  --publication-workload $external_one_member_json \
  --output $external_profile_output --finalization-timeout-seconds 300"
```

Those variables above name the actual external paths retained in the condition
and Make receipts; the portable executable reproduction is in the
[probe instructions](experiments/probe/README.md). The override selects the
already-pinned Python/uv tools, without changing Make recipes, test selection,
the workload or acceptance conditions. Dependencies came from the repository
locks. A reference checkout that includes these files will have a different
source SHA; its actual source and observer identity must be recorded anew.

## Evidence verification

The original complete observer JSON is retained losslessly as gzip; its raw
uncompressed SHA-256 and the compressed file hash are both in
[manifest.json](evidence/manifest.json). Decompress and verify before inspecting
individual thread buckets. [one-member-summary.json](evidence/one-member-summary.json)
contains the derived phase, operation and observed-thread-role totals. Its
phase durations are explicitly seconds; raw observer clocks are nanoseconds.
The summary is not a replacement for the original observation or the maintained
profile's output and receiving evidence.

Clock bookkeeping/calibration, startup coverage, observed thread lifetime IDs,
cross-phase calls, finite budgets and scope limitations are in the raw report.
Neither observer bookkeeping time nor empty-call calibration is subtracted to
manufacture a throughput baseline. Inclusive owner, child and worker time is
not added as elapsed time. Counts spanning different owners are not divided
to invent a per-operation cost.

The previous 16-member raw profile was read and its SHA-256 verified. Historical
1/2/4 and the new integration-owner diagnostic were read from issue comments;
their raw sampler/observer records were not independently inspected here.
[Prior observations](evidence/prior-observations.json) distinguishes these
evidence classes and retains failed/interrupted attempts as such.

## Not performed or established

No production optimization, complete caller/delegate growth conformance,
whole-path attribution, matched throughput improvement/regression, precise
lock/IO attribution or complete active/idle/queue/critical-path trace is claimed.
Ordinary disposable Native SQLite bypasses admitted-cursor counters, catalog
cursor events omit fetch/commit/pool work, and measured thread CPU covers only
selected scopes. Remaining CPU and other residuals stay explicit.

No 1,000/10,000/100,000 trial, preserved 128-file workload, live backend/platform
campaign, independent installed recovery, full product suite or exact-source
integration CI/tested merge was run for this reference. Existing results remain
with their actual sources and owners. No merge, release, provider qualification
or runtime activation was performed. Follow the finite next slice and current
#991/#964 disposition rather than treating this research as authority to restart
costly campaigns.
