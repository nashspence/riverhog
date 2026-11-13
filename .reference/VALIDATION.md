# Validation and consumption

This is local reference evidence for #990 under #903, not full integration CI.
The proposed usage policy in [DESIGN.md](DESIGN.md) still requires explicit
acceptance through the owning issue.

## Source identities

- Audited base and baseline witness:
  `f7bf8b2b6644d80bf2edae4414c3d18574e511c8` on `codex/964-session-owner` / PR #979.
- Clean local experiment checkpoint:
  `cad153b519dd4a7a17dba210b30cd84d12d3cafa`.
- The published GitHub handoff SHA is pinned in #990. Publication reconstructs
  the supplied final tree with the audited base as parent and verifies the exact
  tree SHA. The local experiment SHA is not represented as a published commit.
- Product and test files at the final handoff equal the experiment checkpoint.
  Later changes are limited to `.reference/` documentation and evidence.
  Authentication-suite file digests are in [auth-tests.json](evidence/auth-tests.json).

The 59 authentication cases ran immediately before the experiment checkpoint on
the same product/test contents. The held-writer candidate, 17 control cases,
publication/rebuild cases, full lint and maintained profile ran on the clean
checkpoint. The latter receipts are not evidence that current `main`, the
published handoff commit's full CI, or PR #979's tested merge was qualified.

## Actual results

| Check | Result | Evidence |
| --- | --- | --- |
| Baseline held writer | Expected `SQLITE_BUSY` (code 5), 5.046758 s for valid authentication while the writer remained held | [baseline](evidence/held-writer-baseline.json) |
| Candidate held writer | Authenticated in 0.002014 s while the same original writer remained held; usage omitted | [candidate](evidence/held-writer-candidate.json) |
| App-key service, usage, fences, API and CLI | 59 passed; 0 failures/errors/skips; 59.353 s | [summary](evidence/auth-tests.json), [JUnit](evidence/auth-tests.junit.xml) |
| Existing transaction, control-budget, registration-status and response-schema cases | 17 passed; 0 failures/errors/skips; 18.079 s | [summary](evidence/control-tests.json), [JUnit](evidence/control-tests.junit.xml) |
| Existing publication geometry and archive-only rebuild | 7 passed; 0 failures/errors/skips; 1,361.514 s | [summary](evidence/publication-regressions.json), [JUnit](evidence/publication-regressions.junit.xml) |
| Pinned `make lint` | Passed REUSE, format, Ruff and full workspace mypy on the clean checkpoint; 6.379 s | [maintained receipt](evidence/lint-candidate.json) |
| Real 16-member publication and three Native reader passes | Passed; publication 785.029 s; total profile 1,076.669 s; helper and workspace cleanup received | [full profile](evidence/publication-profile.json), [maintained make receipt](evidence/profile-make.json) |

The held-writer runs used the same witness bytes, pinned Python 3.12.3,
SQLAlchemy 2.0.51 and SQLite 3.45.1. Invalid/revoked/expired/deleted tokens remained
denied. Each run independently probed that the writer was still held, explicitly
released and joined that original thread, authenticated again, confirmed usage
recording after release, disposed the factory and removed its scratch. The
baseline's failure is the expected counterexample, not an accidentally green
product qualification.

The new maintained tests additionally cover empty and rotated-out tokens,
coherent key/grant snapshots across an actual WAL mutation, expiry during grant
loading, later owner rejection of old authority, interphase mutation guards,
monotonic observations, unchanged generations, narrow error propagation and
rollback/reuse after interruption of an actual SQL update.

Actual SQL observations at both 1 and 64 unrelated keys/current grants show one
writer admission and one primary-key-targeted update, with no key/grant SELECT
inside the SQLite observation transaction. Connection checkout/ping work is
outside that transaction observation. The existing retained-fence suite also preserves its one-point-read
checks at 1, 16, 129 and 1,000 grants. These are grant/control work observations;
they are not 1,000-member publication results. The four held-writer unit cycles
took 0.012663 s including verification readbacks, so that number is not presented
as pure request latency or a speedup ratio.

The three CI witnesses named in #990 are retained unchanged:

- `test_archive_only_rebuild_retains_shared_roots_and_exact_member_scope`;
- `test_real_publication_measures_independent_history_dimensions[2-2-0-None]`;
- `test_real_publication_measures_independent_history_dimensions[2-2-2-None]`.

The publication selection also retains the F=1 ciphertext corruption/recovery
exercise, other existing geometry coverage and invalid-geometry refusal. No
test substitutes authentication, suppresses its database errors, weakens exact
member/history assertions or joins the archive worker before each status call.

## Small authenticated publication

The exact workload is [publication-16.json](experiments/publication-16.json):
16 members of 65,536 payload bytes each (1,048,576 bytes total), F=16, C=1, K=2,
Q=1. It uses the maintained in-process authenticated ASGI path, SQLite, encrypted
test archive storage, bounded archive-coordinator polling and a 900-second
finalization resource condition. That condition is not a performance target or
a promise that all preparation and verification fit inside 900 seconds.

Publication completed in **785.029 s**. The full profile, including all three
verification passes and original-helper/cleanup receipt, took **1,076.669 s**;
the enclosing maintained Make invocation took **1,079.124 s**. The profile
records 1,335.717 verified payload bytes/s and 0.020381 verified members/s during
publication. These are whole-workload observations, not isolated auth timings.

| Native reader | Verification seconds | Actual stored-byte reads | Actual stored bytes |
| --- | ---: | ---: | ---: |
| fresh-native-reader | 103.619 | 328 | 1,908,741 |
| warm-native-reader | 90.190 | 236 | 1,639,812 |
| evicted-native-reader | 87.734 | 328 | 1,908,741 |

Every pass verified 16 members, 19 physical journals, the selected bound/retained
history extents, 16 shared-root attachments and one documentary primary. Each
checked 126 archive objects through 110 whole-object checks and 16 part checks.
The source and upload scratch had been removed. The same archive root was
verified throughout. `completion_verified`, `child_received` and
`workspace_cleanup_received` are true, with child exit status 0.

**Performance finding:** this small publication and its full-history verification
remain expensive in the measured environment. Investigating that cost within
the integration owner's existing profiling work is recommended. A matching
completed baseline is unavailable, so this evidence does not assign the cost to
the new observer or establish a throughput improvement. This recommendation
creates no new acceptance threshold or prerequisite.


[Execution context](evidence/publication-profile-context.json) records the exact
command, overlap and received workspace cleanup. The profile ran concurrently
with the tail of the seven-case publication/rebuild
selection, on the same host with separate disposable catalogs/workspaces. Lint
and the 17-case control run had completed. Its rates are diagnostic observations
under that load. There is no matching measured reference, no normalized speedup,
and no production-memory or independent-installed-recovery qualification. The
maintained fresh/warm/evicted Native reader states do not clear OS caches.

## Reproduce the focused first step

Start from a clean reference checkout. Keep output and temporary work outside it.
Use the repository-pinned tools and locked dependency graph:

```bash
evidence_root="$(mktemp -d /tmp/riverhog-990.XXXXXX)"
mkdir -p "$evidence_root/tmp"
export TMPDIR="$evidence_root/tmp"
mise x -- uv run --locked --all-packages --group dev python \
  .reference/experiments/held_writer.py \
  --source "$PWD" --expect authenticated --output "$evidence_root/held-writer.json"
```

The witness never prints a token. To reproduce the counterexample, run the same
witness file with a separate clean checkout and its own locked environment at
the audited base, passing `--source` for that checkout and `--expect busy`.

The focused authentication files are `tests/unit/test_app_key_usage_recording.py`,
`test_app_keys.py`, `test_app_key_authorization_fence.py`, `test_app_key_api.py`
and `test_cli_app_keys.py` in the same directory. Exact additional test nodes
and durations are in the evidence JSON/JUnit. For JUnit output use
`-o junit_family=legacy` so maintained `record_property` observations are
compatible with the repository's warnings-as-errors policy. Place `--basetemp`
and `--junitxml` outside the checkout.

The maintained calibration command is:

```bash
make profile args="--scenario collection-publication \
  --publication-workload .reference/experiments/publication-16.json \
  --output $evidence_root/publication-profile --finalization-timeout-seconds 900"
```

Here, `make lint` and `make profile` used the real Makefile recipes with
`UV_RUN='mise x python uv -- uv run --locked --all-packages --group dev'` to select
only the needed pinned Python/uv tools. This changes tool selection, not test
selection, fixture behavior or acceptance conditions. Python 3.12.3, uv 0.11.24,
Ruff 0.16.0 and all workspace dependencies came from the repository locks.

## Remaining qualification and finite consumption

No full exact-final-source/tested-merge CI, live PostgreSQL lock witness,
platform/installed/provider qualification, 1,000-member arm, preserved 128-file
profile, merge, release or runtime activation is claimed. PostgreSQL needs real
held-row and conflicting-relation tests, receipt of the original lock owners,
and resumed observation after release. Zero-wait lock admission does not create
a deadline for pool checkout, connection setup or storage/commit I/O.

The integration owner should record the explicit usage-policy decision in #990,
reconcile the three owner/test paths with current PR #979, repeat the focused
held-writer check on its replacement source, and use the existing qualification
rail. Record the material's finite disposition in #990 (adopted, reproduced,
partially used, superseded or rejected) with the actual integration source and
remaining qualifications. The reference does not create another release gate or
an obligation to adopt its patch.
