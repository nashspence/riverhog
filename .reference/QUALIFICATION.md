# Qualification and cutover contract for #996

**Non-authoritative integration handoff under [#903](https://github.com/nashspence/riverhog/issues/903).** This document specifies the evidence needed to integrate the proposal in [IMPLEMENTATION.md](IMPLEMENTATION.md) and [STORAGE.md](STORAGE.md). It does not assert that product integration, recovery, platform qualification or the parent issue is complete. Actual reference commands, outcomes and limitations belong in [VALIDATION.md](VALIDATION.md) and [VALIDATION.json](VALIDATION.json).

The audited product source is [#979](https://github.com/nashspence/riverhog/pull/979), `codex/964-session-owner@c28eceaa2dfec1b3712135469ba12230e74f5ffa`. The separate [#995 reference](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/HANDOFF.md) is input at `b545a77996fbc9fe98019796860473bda1bb1aa0`, not an installed dependency. Reconcile intervening source and accepted decisions before implementation. The controlling authorities are [AGENTS.md](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md), [#994](https://github.com/nashspence/riverhog/issues/994), [#996](https://github.com/nashspence/riverhog/issues/996), and the [first-slice breakdown](https://github.com/nashspence/riverhog/issues/994#issuecomment-6921730895). #994 [supersedes](https://github.com/nashspence/riverhog/issues/991#issuecomment-6921589278) the former no-batching/per-step-adoption constraint. It does not authorize archive-format changes, provider operations or the held large campaigns.

## 1. First real slice and implementation order

Qualification starts with actual Native semantics and the small real publication path. Do not postpone these until every #994 workflow has been converted.

1. **Ordinary and retained journal execution:** route both through the common live conductor, bounded epochs and existing semantic algorithms. Eliminate their parent/child recovery representation on ordinary yields. On captured cheap input, prove complete equal results in restart and default adaptive modes, with no optional generation-store initialization, candidate encoding, page capture or Core per-yield adoption. Disposable scratch may still exist.
2. **Immediate Core/SDK F1 slice:** integrate #995 policy/accounting and #997 generation/result-only adoption on authenticated encrypted publication. Preserve prepared-primary exact bytes/IDs before ambiguous effects, mandatory intent/outcome records and original resource receiving. Exercise fresh, warm and evicted independent readers. A Native-only counter experiment cannot discharge this row.
3. **Optional generations on the same executor:** add the selected private WAL capture and retained immutable eight-level page map. Demonstrate an actually eligible default-policy candidate on real sustained work, restore after real process interruption, and bounded fallback. Synthetic elapsed values belong in deterministic policy tests; they cannot establish real default-policy behavior or snapshot economics.
4. **Corpus and remaining consumers:** converge ordinary/retained corpus execution, coupled Core cursors, constructors, history/segmented-archive readers, SDK/CLI and independent recovery. Remove the obsolete durable executor/codecs only after these consumers use the new common owner.
5. **Lifecycle, growth and installed gates:** qualify the exact delivered source, including its tested merge checkout, through the existing required rails. Report each missing or blocked witness separately. Performance observations inform judgment; they do not waive correctness or create permission to publish.

## 2. Acceptance matrix

Each row requires a production-path witness. The reference experiments supply only the narrower evidence recorded in VALIDATION.md.

| Area | Required observation and pass condition |
| --- | --- |
| Semantic equivalence | Identical captured input bytes, catalog/profile configuration and selected history produce the same accepted/rejected result, materialized graph, IDs, hashes, source extent, foreign/fork/delivery/correction facts and required error precedence. Preserve archival framed-prefix checkpoint commitments. |
| Ordinary validation and constructors | `validate_journal_chunks`, `validate_journal`, journal-set validation, creation/append/assertion-batch/correction/observation helpers and archival `append_checkpoint` use the same semantic executor. Constructor-generated evidence is captured once before comparison; do not regenerate timestamps/IDs between arms. |
| Cheap and restart behavior | Zero optional store creation, resume codec round trips and durable checkpoint adoption on cheap default work; restart mode creates no optional generation on any workload. Cheap completion freezes the live DB for independently owned readers and persists the required exact-source semantic outcome, without forcing a final optional snapshot. Required archival/intent/outcome records still occur. Progress getters do fixed metadata work and cannot consume source or create snapshots. |
| Bounded epochs | One aggregate rows/octets/source/control/deadline budget covers all children in a call. Empty chunks/control-only work consume a bounded transition. Cancellation interrupts actual SQLite work through the admitted connection, rolls back the active epoch and leaves no live-state/DB mismatch. No caller transaction spans source/network waits. |
| Progress meaning | Processed progress can be ahead of the selected resumable frontier and can regress after interruption. An offered or prepared candidate is never reported as selected. A newer worker epoch fences the old worker without treating a volatile observation counter as authority. |
| Complete generation | Control, accepted prefix/hash continuation, framing/quarantine, semantic/staging/graph tables, child state and matching corpus/Core continuation come from one quiescent state. Restore cannot combine a newer cursor with older facts. Validate the full descriptor closure and fresh source binding before use. |
| Capture and bounded storage | Initial full capture occurs only after a finite grant. Later capture visits only the admitted WAL interval and changed map paths; no whole-history scan or restore-delta chain. Test page overwrite, repeated overwrite, growth, shrink/tombstones, unchanged pages, root sharing and page-size/binding refusal. WAL-envelope exhaustion suppresses further promotion in that executor without limiting accepted archive size. |
| Generation selection | A complete durable candidate is offered outside the catalog transaction. The short fenced owner transaction selects its exact descriptor, increments successful count and adopts the typed frontier together. Lost response reconciles by original grant/generation identity; stale workers cannot select. A candidate remains pinned until the outcome is known. |
| Recovery and fallback | Newest complete selected generation is tried first, then bounded retained predecessors, then exact authoritative replay. Corrupt/missing optional state preserves diagnostic provenance and cannot swallow source, intent, outcome or custody failures. Fallback below the accepted tip or loss of WAL continuity disables further promotion for that live executor. Accounting always uses its newest root. |
| Reader lifetime and retention | Default two usable generations, with additional prepared/pinned dependencies retained until their exact original reader/worker/selection receiving. Root retirement never removes data needed by a live result or predecessor. Failed close, unlink or callback leaves ownership reachable and charged. |
| Terminal derived result cache | A cold missing/corrupt derived workspace produces typed `DerivedNativeCacheUnavailable`, preserving its diagnostic and exact source/outcome binding. The explicit read/workflow owner coalesces bounded revalidation through the common executor and shares the admitted rebuilt view. Status reads and each member query must not independently replay the journal. Missing/invalid mandatory source, outcome or custody remains fatal under its existing classification. |
| Corpus closure | Ordinary and retained corpus share colocated semantic/order state. Source selection, imports, dependency/order facts and Core continuation match the snapshot. A complete journal is not a complete corpus. Missing child optional state revalidates its exact source; it never skips an input. |
| Current authority | Wrong source/prefix/catalog/profile/algorithm/directory/construction/worker is rejected at the relevant admission boundary. Test changed authority before and after external reads, candidate selection and result return. Immutable verified bytes may be cached; positive authorization may not. |
| Complete cost accounting | Actual optional attempts, failure tails, compaction and receiving are charged without resetting credit across retries/workers/phases or double-crediting child elapsed time. Actual interruption/restore/revalidation cost is reported separately from the failure-free optional budget; conservative restore cost reduces net saving `S` once. Reservation overrun/debt blocks new optional work, not semantic validity or mandatory completion. |
| Independent read/recovery | Fresh/warm/evicted canonical readers verify ciphertext, members and complete history. Installed offline recovery works from archive and keys without Core/catalog/checkpoint access, preserving unknown optional profiles as exact evidence under the existing contract. |
| Growth | Declarative bounds and primitive witnesses cover live execution, capture, immutable-map allocation, restore, root/pin metadata, collection, retries and receiving. Resident memory, spill bytes/files/connections and total retained growth remain admitted. No growing generation log, pin ledger or orphan tree is omitted. |

Source families are inventoried in IMPLEMENTATION.md. The principal test locations below are existing production tests, not a request to install the reference experiment as a runtime dependency.

## 3. Preserve semantic tests; rebaseline private traces

Keep fixed `packages/riverhog-provenance/examples/*.jsonseq` and corresponding `.materialized.json` bytes. Preserve `test_formal_examples.py`, `test_journal.py` and the semantic generic/row/edge/lifecycle/finish/correction/content/comparison/documentary/history/graph-certificate tests. In particular, `test_checkpoint_commits_framed_prefix_not_just_json` covers an archival record and must retain its exact framing and tampering assertions.

| Existing tests | Required disposition |
| --- | --- |
| `test_journal_session.py::test_public_session_matches_native_summary_and_exact_source` | Retain complete summary/materialization, raw SHA/extent, premature-result refusal, terminal behavior and independent result lifetime. Use as the first real Native smoke. |
| `test_entry_replay.py` exact phase/step counts and reopen-after-every-unit traces | Replace expectations tied to the old durable state machine with complete semantic equivalence across bounded epochs, interruption and selected-generation boundaries. Keep all acceptance predicates. |
| `test_retained_journal_session.py` exact-next-unit/version/no-reread assertions | Assert last complete selected frontier and correct recomputation. Processed work after that frontier may be replayed. Preserve currentness, changed-binding, terminal-fence, invalid source-window and no-write-status tests. |
| `test_retained_journal_workspace.py` actual worker-exit witness | Keep actual worker death and subsequent reopen. Change the expected frontier from every microstep to the last usable selected generation or mandatory anchor. |
| `test_validation_checkpoint.py`, `test_topology_checkpoint.py` | Replace superseded private codecs coherently. Preserve strict malformed/foreign/noncanonical rejection, shared child/parent rollback consistency, stale-owner refusal, warm corruption and actual crash windows. |
| `test_journal_result.py` | Preserve actual read-only connection, current source checks on both sides of admission, exact certificate/source binding, independent/parallel reader lifetimes and last-reader close. Rebaseline only the old fatal-on-missing/corrupt-derived-workspace assertions to typed `DerivedNativeCacheUnavailable` plus owner-managed bounded revalidation. Foreign source/outcome/custody and incomplete semantic outcomes retain refusal; cache absence cannot create a successful result. |
| `test_journal_storage_custody.py`, `test_journal_construction.py`, `test_corpus_construction.py`, `test_storage_receiving.py` | Extend constructor fault points to every new original storage/connection resource. Retain partial creation, lost handle/return, failed close, callback retry and positive original receiving. |
| `test_retained_corpus_faults.py`, `test_corpus_owned_placement.py` | Preserve parent/order rollback, lost commit response, current epoch/source bindings, replaced/lost DB and exact caller namespace/connection ownership. Add complete coupled-cursor generation restoration. |
| Native/Core growth tests and owned declarations | Rebaseline causal units when execution/publication changes; add capture/restore/collection tails. Raising constants, deleting traces or counting a mock instead of the original primitive does not qualify the new bounds. |

An old test asserting that any optional-workspace corruption is fatal may change only for validated fallback/replay of reconstructible state. Its mandatory/source/custody counterexamples remain fatal under their original error classification. New private format rejection is a clean hard cut; retaining an old codec beside the new one is not the selected design.

Add terminal-result counterexamples on cheap default and restart runs: complete with no generation store, obtain a reader after session close, keep the original frozen DB alive until the last reader receives close, then remove or corrupt only its derived cache and request a cold result. Assert the typed cache-unavailable result and one coalesced bounded rebuild for concurrent requests with the same exact source/outcome/configuration binding. Subsequent member/history queries reuse that admitted rebuilt view; progress/status queries cause no source reads or rebuild job. Change or remove mandatory source/outcome/custody independently and prove that each original failure escapes without cache fallback. Cancellation during a rebuild retains its original resources until receiving; a later caller cannot adopt an unfinished or differently bound view.

## 4. #995 integration tests

Use the common strict policy parser and decision function; Native reports candidates and actual costs rather than independently granting work. Defaults remain adaptive, 120 seconds net saved replay, optional fraction 1/8, unlimited successful optional creations and two usable retained generations. The surviving `ArchiveWriteControlRecord`, outside disposable `construction_json`, owns policy/accounting across the complete construction lifetime. [#995 handoff record](https://github.com/nashspence/riverhog/issues/995#issuecomment-6926007041).

Exercise all of the following at the owner seam:

- Completion and required durability win even at the eligibility boundary or with unknown optional estimates. Archival checkpoints and prepared-primary/intent/outcome/custody records are never suppressed by restart or the optional budget.
- With net saved replay `S = max(0, completed_replay - restore)`, failure-free candidate creation/lifecycle cost `C`, useful credited work `U`, and already charged/reserved optional work `O`, default admission requires `S >= 120s`, `C <= S/7` and `O + C <= U/7`. Test each inequality independently, including exact and one-nanosecond boundaries. A 30-second candidate needs at least 210 seconds net replay saved as well as adequate workflow credit. Subtract restore/load/validation once in `S`, do not also add future recovery cost to `C` or `O`, and report actual interruption/recovery work separately. Candidate preparation actually performed now remains in `C`.
- Known remaining work `R <= C` favors finishing. Unknown remaining work is distinct from zero; every unknown estimate/accounting state has explicit provenance and cannot create free credit. Long or unknown-length inputs earn eligibility only from actual completed useful work and conservative measured profiles.
- Zero count disables optional creation; zero fraction permits only mathematically zero-cost optional work if all other conditions hold. Held count slots differ from successful selections; actual charges differ from held reservations. Concurrent domains cannot spend one grant twice.
- Grant before optional initialization/encoding/WAL scan/capture; capture outside Core writer transaction; selected descriptor/count/frontier in one short fenced commit; original outcome reconciliation after lost response. Completion prevents a new redundant selection while still receiving already incurred work.
- Failures, overruns and cleanup remain charged. Crash uncertainty seals additional optional admission conservatively. Reopening an older data generation cannot rewind accounting, release unknown reservations or reset count.

Backend cost profiles must be finite, conservative and provenance-labelled for maintained paths. An unimplemented estimate that suppresses every checkpoint forever is not completed adaptive support. Record estimate-versus-actual tails and rerun the boundary/growth witnesses when profiles or storage mechanics change.

## 5. Storage, confidentiality and original receiving

The selected split is **one private live scratch SQLite connection in WAL/OFF** and a **separate retained immutable-object SQLite store permanently in DELETE/EXTRA**. The scratch file is disposable and is never a selected recovery generation. Recovery readers restore a selected root into independently owned scratch; they do not concurrently open the mutable live scratch DB. At terminal completion mutation stops and independently owned read-only result readers may borrow the frozen live DB. Its required exact-source outcome is durable; that derived DB does not become mandatory recovery evidence or trigger an otherwise inadmissible final snapshot. Assert actual returned PRAGMA values, exclusive original execution connection identity, page-size/format binding and supported local filesystem behavior. Do not toggle the sole retained store to a weaker mode for execution.

Use the concrete owner-local `NativeSnapshotResources` values in STORAGE.md, including initial 64 MiB WAL/131,072 dirty-key/64 staging-page defaults, and record all overrides as resource conditions. These are proposed backend engineering limits, not changes to #995's policy defaults or archive-size limits. Current sqlite-admission counts handles/operations, not physical bytes: test the new Native next-quantum write-envelope/sidecar accounting instead of substituting a connection-limit witness. If default tracking repeatedly suppresses the supported long path before its required captures, calibrate the finite resource default from actual measurements and record the disposition; do not silently raise the policy target or delete the long-path witness.

The task-local Python 3.12.3 runtime reports **SQLite 3.45.1**. That observation is not a production runtime certification. SQLite's current [WAL documentation](https://sqlite.org/wal.html#walreset) describes a reset race fixed in 3.51.3, with backports, requiring concurrent connections that write/checkpoint the same WAL database. The selected one-connection scratch and DELETE retained store avoid that described concurrency shape; prove the invariant on installed code. Audit actual SQLite versions, compile options, VFS and any relevant fixes on every supported production runtime before enabling the backend. Never infer the bundled SQLite version from the Python version.

SQLite [documents](https://sqlite.org/pragma.html#pragma_synchronous) that EXTRA additionally synchronizes the directory after DELETE-mode rollback-journal removal; OFF does not provide retained power-loss durability. These settings do not themselves prove Riverhog's filesystem/platform receiving or power-loss guarantees. Check macOS full-sync behavior through the supported VFS and Windows native file/ACL behavior through the original owner. A Python exception or process kill is not a power-loss simulation.

Current Native `JournalStore` uses ordinary SQLite for owner-private provenance assertions/maps and exact frame JSON; corpus stores comparable derived metadata. A page image must contain only that exact Native-owned state within the existing construction namespace. Exclude arbitrary caller/catalog/process snapshots, incoming artifact payloads, decrypted archive/cache objects, plaintext keys and cached authority. There is no cross-workflow plaintext page cache, deduplication or export. Any archive/retrieval storage continues to use its existing encrypted contract. Preserve owner privacy: current POSIX checks require private directories/files, exact descriptors and no substituted symlink/hardlink; Windows privacy is owned by the native ACL contract. [Pinned Native source](https://github.com/nashspence/riverhog/tree/c28eceaa2dfec1b3712135469ba12230e74f5ffa/packages/riverhog-provenance/src/riverhog_provenance), [workspace owner](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/riverhog/src/riverhog_core/journal_validation_workspace.py).

For each new store, sidecar, capture/spill or restore file, retain its original construction/admission record before effects. Extend the source-owned workspace filename/reclamation manifests; an unregistered second tree is not acceptable. Preserve `JournalConstruction`, `CorpusConstruction`, sqlite-admission and retained-resource ownership across partial opens, lost return values and final result readers. `_SnapshotStorage.release` must retain the final lease until actual DB close, namespace cleanup and callback receiving have completed. A failed close keeps the original reachable. A cancelled future, timeout, zero logical pins or catalog terminal marker cannot substitute for original close/join receiving.

At minimum, interrupt/fail each of these transitions: resource reservation; file/connection creation before handle delivery; scratch commit before live-state swap; initial capture; later changed-page collection; map-node/page writes; candidate seal; catalog selection before and after commit; reply loss; restored-reader open; last-reader close; retirement and bounded object collection. After each window, prove exact old/new usable roots, reachable originals, no selected mixed state, no lost charge and eventual retry receiving. Keep both a process-interruption witness and deterministic fault injection; label their different guarantees.

## 6. Hard cut and in-flight state

| State/contract | Concrete disposition and owner |
| --- | --- |
| Old private Native microstep workspace/codecs | Quiesce and join the old worker through its existing owner. After original handle/reader receiving, quarantine or reclaim optional derived workspace and replay exact authoritative source with the common executor. No general old-format reader or in-place private-codec migration. |
| Old always-retained terminal result DB contract | Cut callers and tests together to frozen live results plus mandatory exact-source outcome. Cold derived-cache absence/corruption returns `DerivedNativeCacheUnavailable`; the explicit owner coalesces bounded common-executor revalidation and retains the rebuilt view. Do not force a final optional snapshot, rebuild from status, or replay once per member query. Mandatory source/outcome/custody failures retain their original classification. |
| Captured source, authored IDs/timestamps, prepared-primary bytes, intent, sealed outcome and custody | Preserve exactly, including original construction/grant/effect identities. These are not optional cache state. If unavailable or incompatible, retain/refuse work through its explicit owner/operator settlement path; do not erase, regenerate or silently retry an ambiguous effect. |
| Progress and durable frontier | Replace old per-microstep version/hash meaning coherently with processed progress and selected `resume_frontier_json`; update jobs records, protocol/schema/API/client/CLI/generated contracts and tests together. No compatibility alias that still promises every yield is durable. |
| Core catalog schema and baseline | `riverhog_core.catalog_db`/`state_migrations` and the `state-schema` owner govern explicit initialization/upgrade and verification. Update metadata, owned baseline/migration and tests for policy/accounting/frontier fields together. Startup verifies/refuses incompatible state; it does not repair, reset or drop user state. Test existing populated catalogs and explicit operator upgrade on SQLite and PostgreSQL. |
| Other affected application state | Its existing state-schema owner updates its own baseline and explicit upgrade path. Native must not import or mutate Core/application schema on a caller's behalf. |
| Archive format and independent recovery | No format cut. Existing journals, framed archival checkpoint records, encrypted members and historic identities remain accepted by the same complete semantic rules. Recovery remains independent of optional workspaces. |

Pre-v1 compatibility policy permits removing obsolete public/private representations; it does not authorize discarding non-reconstructible in-flight work or adding startup migrations. Record concrete affected fields and test dispositions in the integration issue/PR. Keep this worktracking under `.reference`/GitHub; do not add future-work TODOs or issue plans to production modules.

## 7. Reproducible focused commands

Run from the exact `riverhog-996` checkout. These paths describe the prepared task-local environment; on another host install the same repository-pinned toolchain at equivalent paths. Use a unique writable scratch temp root and pytest basetemp **outside** the checkout so that exact-source checks stay meaningful.

```bash
mkdir -p /workspace/scratch/939b84c043b0/996-qualification-tmp

UV_CACHE_DIR=/workspace/scratch/939b84c043b0/toolchain/cache \
UV_PROJECT_ENVIRONMENT=/workspace/scratch/939b84c043b0/native-996-env \
/workspace/scratch/939b84c043b0/toolchain/uv-0.11.24/bin/uv sync \
  --locked --package riverhog-provenance --extra test --no-default-groups \
  --python /workspace/scratch/939b84c043b0/toolchain/python/cpython-3.12.3-linux-x86_64-gnu/bin/python3

TMPDIR=/workspace/scratch/939b84c043b0/996-qualification-tmp \
/workspace/scratch/939b84c043b0/native-996-env/bin/python -m pytest -q \
  --basetemp=/workspace/scratch/939b84c043b0/996-native-smoke-pytest \
  packages/riverhog-provenance/tests/test_journal_session.py::test_public_session_matches_native_summary_and_exact_source \
  --junitxml=/workspace/scratch/939b84c043b0/996-native-smoke.xml
```

The Native environment uses Python 3.12.3, uv 0.11.24 and the unchanged lock; the audited lock SHA-256 is `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`. The native retained-resource extension was built against that environment. Do not resync a shared environment while another owner is running tests. The host's unpinned uv is not a replacement for the repository toolchain.

The reference mechanism tests can be reproduced separately:

```bash
TMPDIR=/workspace/scratch/939b84c043b0/996-qualification-tmp \
/workspace/scratch/939b84c043b0/native-996-env/bin/python -m unittest discover \
  -s .reference/implementation -p test_generation_store.py -v

TMPDIR=/workspace/scratch/939b84c043b0/996-qualification-tmp \
PYTHONPATH=.reference/implementation \
/workspace/scratch/939b84c043b0/native-996-env/bin/python -m pytest -q \
  --basetemp=/workspace/scratch/939b84c043b0/996-reference-native-pytest \
  .reference/implementation/test_native_execution_experiment.py
```

See VALIDATION.md for the actual run and count; these commands are reproducibility instructions, not pass claims. The Native experiment replaces parent publication on real semantic inputs but leaves child representation/transactions. The storage prototype exercises actual SQLite files separately and bypasses production admission/receiving. Neither demonstrates their integrated production composition.

### Small maintained F1 fixture

Create a workload file outside the checkout with these exact contents:

```json
{
  "format": "riverhog-publication-workload/v1",
  "payload_sizes": [65536],
  "shared_root_members": 1,
  "shared_claim_members": 1,
  "retained_journals": 2,
  "required_journals": 1
}
```

After installing the full maintained server/client/dev toolchain, run on a clean exact-source checkout:

```bash
make profile args='--scenario collection-publication --publication-workload /absolute/996-f1-workload.json --output /absolute/996-f1-profile'
```

The Native-only environment above is insufficient for this target. The maintained owner is `scripts/transfer_profile.py::_profile_collection_publication`, invoking `tests.support.qualification.publication_profile` through original child-process receiving and checking source cleanliness/identity before and after. Its fixture uses authenticated in-process HTTP and encrypted in-memory archive storage, verifies full ciphertext/member/history results, and receives fixture resources. Fresh/evicted modes replace the canonical reader object; warm mode reuses immutable verified-byte cache. They do not claim an OS page-cache flush or production storage throughput. Include installed independent recovery as a separate witness. [Maintained fixture](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_profile.py).

Run matched old, redesigned-restart and redesigned-adaptive cases on the same workload, captured evidence, authentication/encryption and resource environment. Compare adaptive optional tax to redesigned restart; compare old to redesigned to locate the structural gain. Include actual eligibility and interruption workloads after the cheap F1 case. Do not restart the held 1,000/10,000/100,000 campaign or broaden provider qualification as part of this reference.

## 8. Measurement and complete qualification

Record API turns, actually processed semantic work, source reads/currentness checks, parent/child codec calls and encoded/decoded bytes, SQL/transaction attempts, selected generations, captured/hashed/copied pages/bytes, restore/collection work, actual cost/reservation outcomes, and elapsed time through final receiving. Reuse `tests.support.sqlite_observation.observe_admitted_scope` or its ownership-preserving pattern to observe the original admitted connection. Instrumentation calls each original once, preserves return/exception identity and is removed afterward.

SQL commits are not physical syncs. Wrapping Python `os.fsync` does not observe SQLite VFS synchronization; report physical sync unmeasured unless a qualified primitive tracer actually observes it. A reduced counter is a mechanism observation, not a performance percentage or complete growth proof. Retain source SHA, dependency/runtime identities, workload hash, measurement definitions, interruption point, reader mode and raw outputs for every comparison. Preserve the recorder/actual-primitive corrections required by [#993](https://github.com/nashspence/riverhog/issues/993).

| Qualification surface | Required evidence |
| --- | --- |
| Linux | Actual original directory/file/SQLite construction receiving, symlink/hardlink/owner rejection, full lifetime growth, subprocess death/join, dirty-root recovery and installed package behavior on the supported filesystem. |
| macOS | Same Native/SDK lifecycle through built installed wheels and the actual VFS/sync/file-identity behavior; do not extrapolate Linux results. |
| Windows | Actual ACL/native directory/file identity, sharing/rename/delete and SQLite sidecar behavior, worker/process receiving and installed wheel paths. POSIX mode assertions are not a Windows privacy witness. |
| SQLite catalog | Grant/frontier/count atomicity, fencing, lost commit replies, schema upgrade/refusal and original connection receiving with real SQLite transactions; distinct from the private Native SQLite store. |
| PostgreSQL catalog | The same owner transitions under real concurrent transactions/row locks and worker races, including failed/lost commit response and explicit schema cut. Native remains SQLite-backed; PostgreSQL qualifies the Core owner seam. |
| Fresh/warm/evicted and installed offline recovery | Complete authenticated encrypted reader verification plus independent archive/key recovery. No cached positive authority or hidden server/checkpoint dependency. |

Use focused owner tests while implementing and `make lint` before integration checkpoint pushes. The normal complete check-in targets remain `make lint`, `make unit`, `make dist-smoke`, `make build`; use `make linux-qualification` when complete local Linux integration qualification is needed. `make client-platform-qualification` runs the native installed path on each supported host; `scripts.ci_qualification.native_platform` enables the maintained timing recorder, Native construction/receiving tests, staged installation and listener lifecycle repetitions. Required PostgreSQL/database and filesystem tests remain in their existing maintained gates. [Qualification owner](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_qualification.py), [Makefile](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/Makefile).

The integration handoff requires passing required checks for the exact final source and its tested merge checkout, followed through completion after push. A skipped/blocked required witness is an explicit gap. The reference branch's focused evidence does not replace these gates or authorize provider activity, release, deployment, merging or final #994/#996 closure.
