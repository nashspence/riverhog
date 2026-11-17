# Actual validation and limits

All product source tested here is **c28eceaa2dfec1b3712135469ba12230e74f5ffa**. Only `.reference/` is added. The proposal is not integrated product code, and a reference test that demonstrates an existing defect is not a claim that the defect has been fixed in Riverhog.

## Final reference result

The combined final reference suite contains **48 cases**: 12 adapter/copy, eight discovery, 17 materialization/recovery and 11 restore/actual-Core witnesses. It passed in **83.51s pytest /87.050893141s command wall time**. The final command, exact Python-file hashes, source identity, numeric policy and elapsed time are in [reference_final_command.json](evidence/reference_final_command.json); [JUnit](evidence/reference_final.xml), [stdout](evidence/reference_final.txt) and [maintained timing](evidence/reference_final_timing/) preserve the result. Input hashes are checked before and after execution. The finalized suite has no failures, errors or skips.

[final_static.json](evidence/final_static.json) records passing pinned Ruff lint/format for the reference code and strict mypy for the restore guard and materialization helper, with imports followed silently. This is scoped typing, not full repository typing. The ordinary `make lint` wrapper was attempted and blocked because `mise` is absent; it is not reported as passed. [environment.json](evidence/environment.json) records actual tools/dependencies and unchanged product diff.

The producer performed independent adapter, discovery, materialization and cross-owner design reviews. They corrected substantive proposal gaps: exact idempotent accounting acceptance; positive original execution closure before renewed paid consent; AWS immutable-version requirements; original stream liabilities across expiry/month reset; immutable-file versus SQL capacity ownership; protected receiving headroom; original session/stream/parent receiving; and preservation of files still writable by an operator. These are source-researched design decisions, not unexecuted product checks disguised as green evidence.

## Maintained unchanged-source baselines

These are **four separate focused cohorts**, not one uninterrupted full qualification campaign. There are 139 distinct selected maintained cases, all of which eventually completed successfully under the documented conditions. Initial failures and narrow corrections remain visible:

| Cohort | Initial result | Focused correction / final disposition | Evidence |
|---|---|---|---|
| Retrieval/cache/raw/pack owners | 69 passed, ten failed; 721.25s pytest  /736.758708548s whole command | Ten loopback HTTP cases failed at HTTPX construction because ambient SOCKS proxy support was unavailable in the locked environment. Only those ten were rerun with a loopback-only no-proxy test configuration: ten passed, 3.95s pytest  /5.432197094s command. No source/dependency change. | [Initial command](evidence/retrieval_baseline_command.json), [initial JUnit](evidence/retrieval_baseline.xml), [initial stdout](evidence/retrieval_baseline.txt), [correction](evidence/retrieval_loopback_correction_command.json), [corrected JUnit](evidence/retrieval_loopback_correction.xml) |
| FS/S3/copy/retirement | 16 passed, two setup errors; 36.28s pytest  /39.084196715s command | Two loopback tests use `record_property`, incompatible with default JUnit xunit2 under warnings-as-errors. Reran only these with `junit_family=xunit1`: two passed, 1.44s pytest  /2.756680411s command. | [Initial command](evidence/adapter_baseline.json), [JUnit](evidence/adapter_baseline.xml), [correction](evidence/adapter_baseline_junit_correction.json), [corrected JUnit](evidence/adapter_baseline_junit_correction.xml) |
| Discovery/index/current authorization/Native receiving | 13 passed; 267.24s pytest  /271.518583395s command | Includes actual encrypted two-member archive-only rebuild and exact member/history scope, without changing its maintained finalization timeout. No rerun needed. | [Command](evidence/discovery_BASELINE_VALIDATION.json), [JUnit](evidence/discovery_BASELINE.xml), [stdout](evidence/discovery_BASELINE.txt) |
| Planner/SDK local output/CLI/recovery publication | 29 passed; 107.48s pytest  /108.909510636s command | Includes exact hint/history, operator-edit repair, wrong-primary/history rejection and current native no-clobber publication. No rerun needed. | [Command](evidence/materialization_baseline_command.json), [JUnit](evidence/materialization_baseline.xml), [stdout](evidence/materialization_baseline.txt) |

The retrieval cohort covers all seven selected files, including current original write-execution/transport-closure behavior. The adapter cohort contains fixture and local filesystem/loopback behavior, not AWS/B2 provisioning. Discovery's encrypted fixture exercises actual Core/Native in-process owners; it is not installed independent recovery. Materialization's baseline official-age-dependent full recovery workflow was not run or counted after a skip.

## What the experiments actually measured

### Copy progress and archive backing

[adapter_comparison.json](evidence/adapter_comparison.json) compares the actual Core `process_due` owner with a reference subclass that omits advisory `_record_copy_progress` DML and preserves current point/fence checks. Both arms copy the same 20 objects / 105,358 stored bytes and preserve exact byte/metadata/placement/crypto/revision evidence. The original provider session count is one.

| Parts | Advisory UPDATEs, current → reference | SQLAlchemy commits | Observed `process_due` time |
|---:|---:|---:|---:|
|16|4 →0|75 →71|135.12 →119.54ms|
|64|8 →0|79 →71|353.94 →132.28ms|

These are single current-first observations. They exclude fixture construction and evidence queries. Callback grouping depends on scheduling; the reference keeps read-only checks and the 64-part arm has **168 total SQL statements versus 160** in current code. This is not a repeatable speedup or physical-sync claim. The semantic tests retain exact-session replay after counter loss, already-sealed object recovery after lost catalog acceptance, stale-fence refusal and cancellation receiving.

The actual filesystem adapter test shows that completed `segments.sqlite3` remains necessary for exact reads after the active write state has retired; removing it causes refusal. It is not optional progress. The S3 metadata-only fixture observes **2,050 parts  /17 public pages  /51 ListParts calls  /34,850 returned metadata rows**. That exposes repeated scans; no provider payload or latency was measured, and no linear-growth pass is claimed for that path. See [STORAGE_ADAPTERS.md](STORAGE_ADAPTERS.md).

### Discovery index and original receiving

[discovery_COUNTERS.json](evidence/discovery_COUNTERS.json) and [final counters](evidence/discovery_final_counters.json) exercise the actual Core prepared reader/index and SQLite. Both verify the same four metadata objects once, insert four descriptors, serve three repeated traversals with identical exact bytes/digests and retain query-only behavior. The unnecessary final explicit/traced data COMMIT changes **one →zero**. There is still real schema/journal/spill/close work, and neither arm creates an optional recovery generation. The small fixture supplies authenticated plaintext at the ObjectReader seam, not a full Native validation proof. A separate test uses actual Native journals/summaries to reproduce and correct lost-original eviction on before-close and after-close-reply faults. See [DISCOVERY_INDEXES.md](DISCOVERY_INDEXES.md).

### Restore identity and actual Core acceptance

[restore_current_policy.json](evidence/restore_current_policy.json) exercises real `AwsDeepArchiveReadPreparation` with a controlled provider client. After the fixture accepts a request and loses its reply, a later expired-window observation lets the unchanged policy send the identical restore request a second time. The reference original guard instead retains possible entry across reopen; the provider-lost-reply case makes one request, and the lost-arm-commit-reply-before-entry case makes zero and conservatively refuses re-entry. Sixty-four identical requested polls create **zero progress UPDATEs and zero commits**, with one initial provider request. The guard also rejects changed identity and stale fixture authority.

[restore_core_current.json](evidence/restore_core_current.json) uses the actual Core service, catalog and encrypted archive fixture. It demonstrates two current behaviors: two unreceived claims can select `prepare` for the same object; and a successful catalog commit followed by an injected lost reply leaves a real job while the current exception handler releases its recording allowance. Retrying returns that same job without reserving again. The allowance is a recording test seam, so this is not a real PostgreSQL/monthly-quota qualification. The proposed S/A/E rollover correction and full read-preparation protocol are specified but unimplemented here. See [RETRIEVAL_CACHE.md](RETRIEVAL_CACHE.md).

### Local materialization and independent recovery orchestration

[materialization_observations.json](evidence/materialization_observations.json) compares actual planner behavior with the reference parent-ID trie. At 16 members/depth 32, the favorable shared-prefix case retains identical output commitments while encoded index key/spelling bytes change from 127,872 to 482. This is not a claim of unbounded-depth contract failure: the sealed hint schema already limits depth to 32 and component length to 128. Both observations use disposable SQLite settings; no global retained-database synchronization change is measured. At 128 members/depth 8, the trie makes more SQL calls and is slower in the single observation (approximately 89.84ms versus 73.23ms), so no general speedup is claimed.

Actual faults exercise no-clobber/lost link reply, parent-sync failure, unowned scratch deletion, quarantine pathname replacement, lost final whole-output publication reply, original mapping-connection receiving and download-client receiving. Two full recovery-owner witnesses use an actual encrypted archive fixture and substitute only official-age decryption with the repository's authenticated `riverhog_age.iter_decrypt_age_scrypt` primitive. They do not qualify the age subprocess, its stdin/stdout/descendant receiving, installed isolation, Windows/macOS publication or power loss. See [MATERIALIZATION_RECOVERY.md](MATERIALIZATION_RECOVERY.md).

## Failed attempts and harness corrections

Raw failures remain alongside successful results. The adapter JUnit and retrieval proxy corrections above did not weaken product tests. Discovery's two early reference failures concerned tuple encoding and `dataclasses.asdict(Counter)` JSON reporting. Materialization reference fixtures initially exceeded the actual depth/component bounds and used a noncanonical tuple; they were corrected to the sealed schema and JSON contract. The restore harness initially imported `read_snapshot` from the wrong module, then needed the existing explicit disposable runtime-config fixture; both failures are retained under `restore_initial_*`. The final real-owner assertions were not weakened.

An earlier combined run already passed 48 cases; its output is retained as `reference_before_temp_correction.*`. Because `/tmp` is unavailable here, Python's temporary-directory fallback placed that run's fixtures beneath the checkout. Those completed fixture files were preserved outside the checkout, and the runner was corrected to choose the checkout's parent explicitly. The final combined run rebinds the exact final runner/code hashes. No product/fixture bytes were changed by that location correction. A preparatory copy command using the default system Python failed before running any tests because workspace packages were absent; the actual correction used the frozen interpreter.

## Environment, reproduction and scope

The isolated environment is `/workspace/scratch/939b84c043b0/storage-999-env`, frozen from the full workspace lock without editing it. Recorded versions: Python 3.12.3, SQLite 3.45.1, pytest 9.1.1, Ruff 0.16.0, mypy 2.3.0, SQLAlchemy 2.0.51 and httpx 0.28.1. The workspace also contains separately named HTTP libraries; each owner command records its actual imports. `uv.lock` SHA-256 is `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`.

Tests bind `scripts.validation_numeric.qualification_numeric_policy()` via `bind_numeric_environment`, with source-owned max_work 907920445364, max_integer_bits 84 and reader identity `4f08bc6148007c7194b2451cdd885b04ada30806b6ee1f6c5e6d29433885720a`. These are recorded resource conditions, not product size limits. Timing comes from `scripts.ci_timing` plus command wall time; JUnit and stdout can differ slightly in their timing boundaries.

To reproduce the final reference suite from an appropriately provisioned checkout and its locked interpreter:

```bash
python .reference/implementation/run_reference_checks.py
```

The runner writes reference evidence, creates fresh temporary fixture state outside the checkout, binds the owned numeric profile, and confines tests to local/controlled fixtures. Its no-proxy configuration is for that test process, with no live provider calls. It is not a new product profiling/qualification entrypoint; maintained measurements continue through `make profile`.

`mise`, Docker, `age`, `age-plugin-batchpass` and `psql` are absent. Full `make lint`, Linux/image/distribution/native matrix, PostgreSQL, official-age installed independent recovery, physical power-loss and real-provider qualification were not completed. No new production state migration, adapter protocol, paid-consent path, retained-copy executor, physical capacity backend or integrated long-work optional promotion was qualified. [QUALIFICATION.md](QUALIFICATION.md) closes those requirements with exact owners/cases/commands. A reference branch without triggered Actions/check runs is not a green CI result.

The source/issue/PR reconciliation, complete file hashes and publication tree comparison bind this packet to its actual delivered bytes. The owning #999 comment records the exact published reference commit. Publication does not approve integration, complete the issue or change release/provider authorization.

[artifact_check.json](evidence/artifact_check.json) records parsed JSON/XML, resolved document/source links and unchanged product paths. The authored Markdown/Python diff passes whitespace checks. Whole-diff `git diff --check` returns2 solely for trailing spaces in preserved raw failed-test stdout/JUnit; those diagnostics are retained in the report, and the original evidence has not been rewritten for cosmetic cleanliness. [VALIDATION.json](VALIDATION.json) inventories delivered artifact hashes excluding itself; the Git tree supplies the complete publication identity.
