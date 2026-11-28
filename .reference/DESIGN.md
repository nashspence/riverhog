# Completed journal preparation: consolidate repeated restoration

## Decision and boundary

Use the existing attempt-bound replay binding to select a smaller completed-preparation path. Restore every independently required checkpoint once in that path, then let the existing Native owner perform its independent validation and currentness checks.

This is a deliberately limited consolidation. A binding alone cannot safely replace all preparation checks. Removing those checks would require an independently justified replacement for their admission guarantees; this proposal does not add one. The small experiments show why a bare binding shortcut is insufficient. The proposal adds one owner-local predicate, no retained readiness state, database, scheduler, generic cache, worker capacity or cross-application import.

The [request](https://github.com/nashspence/riverhog/issues/991#issuecomment-7234184351) reports 288–290 repeated helper calls in a real intrusive F1. Their 13.790 seconds of summed inclusive attribution and 4.930 seconds of calling-thread CPU identify work worth investigating. They do not identify an avoidable fraction or elapsed saving. This reference supplies actual-caller work counts and rejection/lifetime witnesses; it does not rerun a production publication campaign.

## Actual original call graph

`SqlAlchemyCollectionUploadService._advance_provenance_journal_validation_unit` first captures a bounded source portion and reads exact validation work. It then calls epoch scan, topology capture, epoch union, epoch rank and replay preparation before every Native advance.

Completed helpers still restore their checkpoint metadata. Rank restoration calls union restoration; replay preparation calls rank restoration again and enters the source-fenced binding transaction. In an ordinary completed service preparation portion:

| Work | Original | Proposed admitted path |
| --- | ---: | ---: |
| Epoch checkpoint restoration | 1 | 1 |
| Topology checkpoint restoration | 1 | 1 |
| Union checkpoint restoration | 3 | 1 |
| Rank checkpoint restoration | 2 | 1 |
| Completed binding-publication transaction | 1 | 0 |
| Additional exact binding point probes | 0 | 2 |

This table excludes first bounded preparation and independently owned cold Native graph-certificate construction, which still restores union/rank. The experiment totals include both. The existing binding transaction usually compares an already stored value without writing it; eliminating its repeated invocation is not deleting a required initial durable acknowledgement.

The two changed production owners are `services/upload_journal_replay.py` and its actual caller in `services/collection_uploads.py`. The existing service-unit growth declaration explains the changed preparation path without changing its numerical bounds or delegate limits.

## Admission algorithm

`is_upload_journal_replay_prepared(factory, source, *, catalog)` performs these steps:

1. Use the existing `_probe` in its closed fresh snapshot. A stale source or absent binding returns false. A current-but-unprepared source is not positive readiness.
2. Decode the present canonical binding and compare it to the exact expected attempt, directory, journal, source extent/digest/version, frame count, catalog checkpoint digest and mode. Malformed or different bindings raise.
3. Restore epoch metadata with the original `_begin_scan`, topology metadata with the original `_begin`, and rank metadata with the original `_begin`, which restores union. Require complete states and the same Scan version across the returned work records.
4. Repeat the existing source/binding point probe after restoration. Require the binding still to be present and exact. A deletion or stale source returns false; a malformed or changed value raises.

On true, the service skips the five preparation advancement helpers and enters the original Native owner. On false it follows the original bounded preparation sequence; stale-source fences, partial progress, deferred behavior and contention retain their existing outcomes. The service selects one catalog object for admission, any original preparation, and Native advancement.

The final probe is intentional. Native source verification permits a current source with a missing binding because deferred preparation is a supported path. It therefore cannot replace the positive final check required by this particular completed-preparation admission.

These snapshots do not retain a lock through Native execution. Their result is an admission observation, not a new authority over future source changes. Subsequent Native source, worker, manifest and publication checks remain necessary.

## Why the checkpoint checks remain

An exact replay binding names source and attempt inputs; it does not independently validate every derived preparation row. A cold `UploadJournalGraphCertificates` constructor currently restores union/rank and then freezes the Scan manifest. It does not independently run the epoch and topology restorations.

Two reproduced baseline counterexamples changed the epoch or topology algorithm while leaving the replay binding exact. The original owner restoration rejected each change; the cold certificate constructor could freeze the changed metadata and report it current. This demonstrates a metadata-admission gap in the proposed bare shortcut, not publication of an incorrect semantic result. The final proposal retains the original checks, with actual cold-caller regressions.

| Owner | Retained checks |
| --- | --- |
| Epoch | Attempt/directory/algorithm, finite frame and byte extents, last Frame attempt/epoch/kind/digest presence and end offset, exact complete source end |
| Topology | Algorithm, canonical checkpoint, bounded frame extent, complete end/no active item, last Fact directory and digest |
| Union | Algorithm and checkpoint codecs, consumed facts and complete frame/count/phase, last source Fact digest and last Arc presence |
| Rank | Algorithm and canonical checkpoint, terminal epoch/phase and union counters, last Ranked vertex, processed Arc, graph Result and active vertex |

The proposal continues these checks once in the service preparation portion on every admitted turn. Frozen Scan headers alone do not detect arbitrary child-row deletion. These last-record sentinels also are not a proof that every derived row exists; Native continues to check the actual graph evidence it uses.

The twelve warm-caller corruption cases preserve the same original Native session and acknowledged progress while rejecting damaged preparation. After repairing the fixture, that same session/worker epoch advances. Missing binding deliberately selects original preparation; reestablishing it changes the Scan version and can invalidate an already frozen Native manifest. The existing lost-workspace outcome remains visible rather than silently adopting changed prepared authority.

## Native validation and publication remain independent

The proposal changes neither `UploadJournalValidationOwner` nor its source, graph-certificate, job, session or result owners.

- The source fence still checks the actual journal key/state, accepted and declared bytes, digests, capture attempt/directory/algorithm/phase/version, cursor and failure.
- The retained Native slot still compares the exact source object and catalog digest on each continuation. A closed owner rejects, and the existing slot lock remains nonblocking.
- Worker currentness still checks the exact job/workspace/source/certificate identity and worker epoch.
- The graph-certificate provider still compares its independently frozen complete manifest and verifies the actual source frame, graph, edge and rank rows used by Native.
- Native progress, advancement and finalization keep their existing currentness and private ledger fences. No accepted atom, graph check, semantic validation or progress publication is bypassed.
- The service's initial source/capture-directory and final captured-segment checks remain. Terminal publication still checks exact current inputs and guarded catalogue state before accepting an outcome.

The observed Native advancement, source/worker/certificate currentness, checkpoint load/fence, progress and publication wrapper counts are identical in each baseline/candidate experiment pair. This does not claim a census of all Native SQLite or filesystem work.

## Retirement, restart and receiving

The original epoch helper drains old-attempt preparation retirement before reporting readiness. The normal producer of that retirement is an attempt replacement, which resets the binding and preparation. Under these supported writes, a complete exact binding is reached only after that earlier drain; a new attempt cannot retain positive readiness for the old one. No retirement queue or retention policy is added.

Native commits still precede guarded catalog progress publication. Ambiguous acknowledgement is reconciled through the existing original workspace and committed prefix. Restart claims a new worker epoch; accepted work never recreates a missing Native database. Tests cover continuing an existing workspace and rejecting a lost one.

Original construction, session, storage and lease receiving remain owned by the same objects. Closing slots remain charged while receiving is pending or unknown. Borrowed readers retain their independent original storage lifetime. The six complete probes confirm construction/storage receiving, empty owner slots, no checked-out catalog connections, and subsequent removal of their test directories. Their primitive counters cover the validation-unit calls; final service-close receiving is separately asserted, not included in those primitive totals.

## Backend applicability

The reused probe is a SQLAlchemy source-fenced primary-key lookup with an outer join. The experiment captures that query and compiles it for SQLite and PostgreSQL; it adds no engine-specific expression. Existing preparation transactions, locks and guarded writes remain unchanged.

`state_schema.read_snapshot` explicitly begins SQLite snapshots when needed and selects PostgreSQL `REPEATABLE READ` with `SET TRANSACTION READ ONLY`, closing the Session in `finally`. PostgreSQL source-fenced preparation and job operations retain their `FOR UPDATE`/`SKIP LOCKED` and compare-and-set behavior. A readiness snapshot does not grant a lock across subsequent Native work on either engine.

Actual-caller tests here executed on SQLite. PostgreSQL runtime locking, retry/serialization behavior and query plans were not executed or qualified. SQL compilation is not a substitute for that qualification.

## Remaining work and integration scope

The one-frame fixture still needs 107 actual Native turns and substantial catalog/currentness work. Required checkpoint restoration, source-tail checks, Native semantics, graph verification, progress acknowledgements, recovery and receiving remain visible costs. Their retention here does not settle every future necessity review. A name such as progress or checkpoint does not establish optionality.

Apply the whole focused patch, including its maintained rejection and growth witnesses, through the normal integration rail. This slice is independent of the earlier neutral-history and activation references. It changes no archive format, KDF, Native budget, feature applicability or shared approval/limit. #1006 retains all#68 policy adoption and approved GitHub Actions operation maxima under current available/comparable conditions; #1007 retains downstream whole-path/growth/recovery acceptance. No large trial or new global hold is required by this reference.
