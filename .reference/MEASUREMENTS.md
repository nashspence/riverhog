# Three-arm durability measurements

This is a selected implementation proposal for [#1000](https://github.com/nashspence/riverhog/issues/1000), under [#994](https://github.com/nashspence/riverhog/issues/994) and the [#903 reference policy](https://github.com/nashspence/riverhog/issues/903). The audited product source is **c28eceaa2dfec1b3712135469ba12230e74f5ffa**. Product code is unchanged here. The executable comparison helper demonstrates evidence checks and arithmetic; it neither implements the redesigned executor nor establishes source, custody, original-resource receiving or performance by assertion.

The selected integration retains **`make profile` as the sole maintained profiling command**. Extend its existing measurement and scenario owners with explicit durability comparison records. Collect one real workload through its normal owners, validate its results and original receiving, and only then compare matching observations. Keep latency observations, deterministic #995 admission, enforced growth bounds and correctness outcomes distinct. [Repository measurement policy][agents] · [current Make entry point][make] · [current measurement owner][measurement]

## 1. Source findings and the current baseline

| Current owner | Actual behavior at the audited source | Selected disposition |
| --- | --- | --- |
| `Makefile:profile` | Calls `python scripts/transfer_profile.py $(args)` through the maintained toolchain wrapper. | Retain this command; add flags and records here. No benchmark command, scheduler or separate workflow registry. |
| `scripts/performance_measurement.py` | Strict v1 context and v2 sample schemas; exact matching of scenario, workload, completed bytes and context; unique run identities; valid finite measured rates; target comparisons are report-only. | Reuse these validators and matching semantics. Add the focused cutover record/comparison beside them. Do not weaken existing comparisons to make historical results match. |
| Generic transfer path in `scripts/transfer_profile.py` | Measures command wall time and accepts an exact completion receipt; an absent receipt remains unverified, an invalid supplied receipt or failed execution is an error. Its v3 report may contain diagnostic transfer-log phase totals. | Preserve the existing path. Its command interval is not automatically a durable-result or fully-received interval for cutover work. |
| `summarize_transfer_log` | Sums per-operation queue/source/integrity/crypto/processing/remote/checkpoint/downstream durations. The output explicitly disclaims unique end-to-end goodput. | Retain diagnostic sums. They cannot become wall-clock useful time, root credit or attributable optional tax by subtraction. |
| `_profile_collection_publication` | Requires clean exact source, launches the original maintained helper through `run_validation_child`, validates its exact completion receipt, and removes the parent-owned scratch only after receiving it. Reports publication, reader observations and the full parent execution envelope. | Reuse this original lifecycle. Add separate owner milestones; preserve failures, source checks and receiving. |
| `publication_profile.run_publication_profile` | Publishes once and fully verifies fresh, warm and evicted Native readers. Uses authenticated in-process TestClient HTTP, SQLite and an encrypted in-memory archive store. Native cache changes do not clear OS caches. | Keep the real F1/F16 fixture and full verification. Its conditions are diagnostic and must remain explicit. |
| `publish_shared_history` | Its return includes `producer.finish`, producer/primary-store close, post-publication catalog checks, upload-scratch deletion and source unlink. Setup and evidence authoring also occur inside this function. | Do not call the existing `publication_seconds` an isolated durable-user-result latency. Measure the new milestones at their owning boundaries. |
| `scripts.ci_timing` and `validation_numeric` | Record source, test identities, elapsed observations, numeric policy and actual results; qualification resource limits have an owning source. | Reuse for the maintained tests and final qualification. A speed target never overrides a resource deadline or evidence failure. |

These findings come from the [profiler][profile], [publication helper][publication], [publication construction][construction], [full publication observer][observer], [process receiver][process], and their [measurement][measurement-tests], [profiler][profile-tests] and [publication][publication-tests] tests. The retained full-reader observer keeps identity sets and fixture ciphertext in memory; its successful execution is not a production memory bound or installed independent recovery proof.

### Observed once on clean current source

One direct invocation of the **unchanged maintained Python producer** ran F1/C1/K2/Q1: one 65,536-byte payload, one shared root attachment, one documentary member, two retained supplemental journals and one required supplemental journal. The exact [workload](evidence/measurements_f1_workload.json), [invocation and source identity](evidence/measurements_f1_invocation.json), [complete profile](evidence/measurements_f1_profile.json) and [final phase receipt](evidence/measurements_f1_phase.json) are retained.

| Observation | Actual value |
| --- | --- |
| Run ID | `8111d918-d0ec-4e8d-952c-02bd19b76234` |
| Current helper's publication interval | 44.761003893 seconds |
| Complete owned parent interval | 65.034581815 seconds |
| Fresh Native verification | 3.934616697 seconds; 58 actual stored reads; 167,370 stored octets |
| Warm Native verification | 3.882517286 seconds; 41 actual stored reads; 135,013 stored octets |
| Evicted Native verification | 3.831112246 seconds; 58 actual stored reads; 167,370 stored octets |
| Complete retained history | 4 physical journals; 5 entries; 19,873 journal bytes |
| Bound and required history | 3 journals; 4 entries; 17,966 journal bytes |
| Stored commitments checked per pass | 21 objects; 20 whole-object checks; 1 ordered part check |
| Original receiving | Child exit 0; child and workspace cleanup received; source clean and unchanged before/after |
| Comparison | `not-compared` / `no-measured-reference` |

Every reader checked the same published archive root, complete semantic history and stored commitments, after source files and upload scratch were absent. The timing uses Python 3.12.3, SQLite 3.45.1 and Linux on the task's overlay filesystem; the archive storage in this fixture is explicitly in memory. Archive scrypt work factor is 1, producer cache is disabled, append is one member at a time, coordinator polling is 0.05 seconds, and the unchanged finalization resource deadline is 900 seconds. [Raw profile](evidence/measurements_f1_profile.json)

**This is one current-implementation observation, not three-arm cutover evidence.** It did not measure optional checkpoints, SQL/commit counts, physical synchronization, policy cost, precise user-result latency or an isolated receiving tail. The difference between 65.035 and 44.761 seconds includes reader verification, process/fixture work and cleanup; it is not a cleanup measurement. Earlier #991/#997/#999 observations retain their own source and instrumentation and must not be spliced into a speedup against this run. The tool host lacks `mise`, so the maintained Make wrapper is not qualified by the direct Python invocation. No replacement wrapper was installed.

## 2. The three controlled arms

| Arm | Delivered implementation and policy | Required evidence |
| --- | --- | --- |
| A: `legacy` | Preserved pre-redesign product artifact, with its exact source and original operational settings. | The same mandatory semantic/safety obligations and output verifier must pass. If the old artifact cannot satisfy that contract, record the incompatibility instead of manufacturing a comparable reference. |
| B: `restart` | Redesigned product artifact with optional recovery disabled. | Same executor, source, installed artifacts, dependencies, safety, resource limits and policy settings as C, except `recovery_mode="restart"`. Mandatory evidence, intents, results, fences, custody and receiving remain enabled. |
| C: `adaptive` | Exactly B's redesigned artifact through its normal default runtime path. | The actual admitted root reports all five default settings below. A caller's arm label does not establish the effective policy or executor. |

The five canonical defaults are `recovery_mode="adaptive"`, `replay_target_seconds=120`, `max_optional_overhead_fraction=0.125`, `max_optional_checkpoints=null`, and `retained_checkpoint_generations=2`. B changes only the mode to `restart`. In product integration import the shared #995 policy owner rather than duplicating defaults in the profiler. The reference helper's literal mapping is only a testable comparison expectation; it is not another configuration authority.

At invocation admission, the existing runtime owner supplies the **effective frozen policy**, stable logical-workflow identity and selected executor/build identity. The profile receipt binds that original information. For remote components, consume the actual authenticated owner response and installed package/image identity through the existing qualification path; do not accept a client declaration as server configuration. Mode changes between a retry and resume do not create a new root, credit or measurement run. The helper in this branch cannot prove any of these runtime facts from JSON.

Preserve A as an immutable source/artifact input before removing its implementation. Do not ship a second legacy executor in the redesigned runtime. Run the same maintained measurement observer against the separately selected artifacts. If version-specific public API adapters are necessary, keep both adapters in that one pinned observer revision and hash the full observer. Record product and observer source identities separately; a new observer does not retroactively change an old report's instrumentation. B/C require identical product and executor identities, while the A/B product difference is the explicitly controlled redesign.

Two input contracts are separate:

- **Captured immutable reader/validator fixture:** all arms receive exactly the same admitted captured bytes, selected history, algorithm contract and expected semantic result. Compare the complete required result, including errors where applicable; do not require superseded private phase traces.
- **Fresh end-to-end publication:** use identical source payload bytes, geometry, documentary obligations and verification contract, but each execution has fresh operation/journal identities, random encryption material and its own accepted archive root. Verify each root and complete original documentary result. Cross-run UUID/ciphertext equality is neither expected nor a qualification shortcut. Captured fixtures separately establish exact semantic equivalence.

## 3. Concrete integration with the existing profiler

The chosen public surface adds **`--cutover-arm {legacy,restart,adaptive}`**, **`--cutover-context PATH`** and optional **`--cutover-reference PATH`** to `scripts.transfer_profile`. These are opt-in fields on `make profile`, including maintained publication and lifecycle scenarios; they do not select or implement the product policy. The actual owner configuration selects policy and the receipt verifies it. Reject partially supplied cutover arguments and incompatible scenario/context combinations before launch.

Keep existing generic `--reference` exclusively for its strict `riverhog-transfer-profile/v3` measured reference. **Do not overload it with a union of old records and arm bundles.** `--cutover-reference` accepts only a new exact `riverhog-durability-comparison-input/v1` manifest pointing to already received arm records in the same comparison group. Missing references yield structured `not-compared`; malformed explicitly supplied references fail. No old aliases, unknown fields, duplicate JSON keys, nonfinite values or format guessing are accepted.

Add the following focused functions to `scripts.performance_measurement`: `validate_durability_observation`, `compare_durability_block`, and `summarize_durability_blocks`. The producer emits `riverhog-durability-profile/v1`; the comparison result is `riverhog-durability-comparison/v1`. Their source-owned tests belong beside the current tests, and their installed invocation stays with the existing scenario owner. `measurement_comparison.py` in this branch supplies the executable arithmetic/matching core, not a production wire-schema replacement.

The strict cutover context has a group UUID, paired block UUID, ordinal position and referenced canonical condition manifests. Reuse the existing context's workload/environment/path fingerprints, logical byte domain, completion-boundary identifier, cache state and concurrency. The manifest expands these fingerprints into reviewable facts:

| Shared condition | Exact content to bind |
| --- | --- |
| Workload | Payload sizes/content identities; files/members; retained and required history; documentary fanout and relationship geometry; known/unknown-length admission; captured or fresh input contract. |
| Environment | OS/kernel/architecture; runtime and common non-product dependency versions; CPU/memory/resource allocation; backend and filesystem/storage/mount settings; external transport topology. Product wheels/images and newly introduced internal packages belong to the separately declared artifact, not a hidden environment change. |
| Resource and safety policy | Exact database/sync/admission/cancellation configuration; row/byte/memory/concurrency bounds; resource deadline; mandatory-effect/custody contract and verifier revision. The number of old commits is not a mandatory safety contract. |
| Cache/source conditions | Actual Native reader state, OS cache disposition, source availability/replay binding, provider/backend fixture state and prescribed preparation/reset steps. A requested `cold` label is not proof that a cache was cold. |
| Path and instrumentation | Equivalent public operation/delegate topology, observer code and counter definitions, completion semantics, clock origin definition, source/artifact validation and receiving protocol. Exclude the deliberately varied product artifact/mode from the shared path fingerprint. |
| Interrupted work | Exact semantic fault marker, fault kind, restart scope, source/state retention and intended current-authorization treatment. Normal work has no interruption plan. |

Every arm record retains its unique original run/receipt identity, exact product/observer sources and artifact digests, source checks, actual frozen policy, output verification receipt, the two milestones, typed counters with their observation scope, and links to raw owner evidence. `completed_bytes` and `completed_items` remain exact counts, not estimated geometry substituted after failure. A zero-byte member is valid work: the product cutover schema admits zero verified payload bytes and reports no byte-rate sample when the existing positive-byte v2 sample cannot represent it. Never substitute one byte. The executable reference helper intentionally demonstrates the positive-byte v2 path; integration must add the zero-byte timing/count case to the new wire-schema tests.

The reference manifest is a bounded input to this invocation, not a history database. Read it under the existing qualification numeric and `validation_storage_budget()` admission, validate exact individual records, and stream retained raw evidence rather than loading arbitrary event histories. One invocation executes one selected maintained workload; comparison ingestion does not launch prior runs, poll product status into execution, or create a generic scheduler. Source conditions and required lifecycle receipts must be checked by their existing owners **before** invoking the descriptive comparator. [Numeric owner][numeric] · [evidence capacity][capacity]

### Exact wire fields and adapter mapping

Implement these schemas in the existing measurement owner, with unknown/missing fields rejected at every object level. They describe measurement evidence, not a runtime owner registry. JSON integers are exact and exclude booleans; SHA-256 values are 64 lowercase hex characters, source SHAs are the existing 40-character Git identity, UUIDs use canonical hyphenated spelling. Writers use the repository's canonical JSON. Reader byte/numeric admission precedes parsing; unsupported versions never fall back to a guessed schema.

An `EvidenceRef` has exactly `path`, `bytes`, `sha256`: a nonempty relative POSIX path below the containing manifest's admitted evidence directory, exact nonnegative byte length and SHA-256 of the retained bytes. Reject absolute paths, empty/parent components, backslashes and resolution outside that directory, including symlink escape. Resolve references from the containing file, not the shell cwd; validate length/hash and use the existing evidence owner to establish origin. A digest alone does not establish original execution. Referenced condition documents are the canonical exports of their existing workload/configuration/artifact owners, not arbitrary command lines or environment dumps.

**`--cutover-context`** is exactly:

| Field | Type and meaning |
| --- | --- |
| `format` | Literal `riverhog-durability-context/v1`. |
| `comparison` | Exact existing `riverhog-performance-comparison-context/v1`; its `comparison_id` is the group UUID. This cutover requires `logical-payload` and `durable-user-result`. All existing cache/concurrency validation applies. |
| `block_id`, `order` | Paired block UUID and integer 0, 1 or 2. They identify an intended block/position, not elapsed-time order reconstructed afterward. |
| `input_kind` | `captured-immutable` or `fresh-publication`. |
| `bindings` | Exactly `workload`, `environment`, `path`, `conditions`, `instrumentation`, `interruption`. Each is an `EvidenceRef`; only `interruption` may be null, meaning normal work. The first three SHA-256s equal their corresponding `comparison` fingerprints. The conditions document contains the mandatory/resource/output-verifier and cache/source facts in the table above. |
| `expected_product` | Exactly `source_sha`, `artifact_sha256`, `executor_sha256`, `policy`. Policy is the complete canonical five-field snapshot for B/C or null for A. This is the expected selection; the actual runtime/artifact receipts must confirm it. |

**Each `riverhog-durability-profile/v1` record** has exactly the following fields. Every nullable field remains present.

| Field | Type and validation |
| --- | --- |
| `format`, `run_id`, `arm`, `selection` | Format literal, unique run UUID, the selected arm enum, and the complete validated context above. |
| `source` | Exactly `before` and `after`, each the existing `{source_sha, source_clean}` identity. A qualified observation requires both clean, identical, and matching `expected_product.source_sha`. |
| `artifact`, `policy` | Artifact has exactly `artifact_sha256`, `executor_sha256`; policy is the original actual five-field snapshot or null for A. Require exact agreement with selection and the arm rules. Preserve A's actual original configuration in its policy evidence even though A has no redesigned five-field snapshot. |
| `clock` | Exactly `origin_id`, `domain`, `implementation`, `resolution_ns`: this run UUID; literal `owned-parent-monotonic`; the actual clock implementation string; positive integer resolution rounded conservatively upward. Both milestone intervals use this origin. |
| `work` | Exactly `scenario`, `workload`, `completed_bytes`, `completed_items`. Use the maintained scenario and workload identities; completed counts are nonnegative integers or null when unavailable. Successful work requires actual counts and at least one completed item; payload bytes may be zero. |
| `result` | Exactly `state`, `identity_sha256`, `elapsed_ns`, `unavailable_reason`. State is `verified`, `rejected` or `unavailable`; identity and positive elapsed ns may be null. A `verified` result requires the actual identity and original output proof; its elapsed may be missing if the clock was unavailable. A captured fixture expecting a semantic refusal is verified only when its owning verifier proves that exact expected outcome. |
| `receiving` | Exactly `state`, `elapsed_ns`, `unavailable_reason`. State is `received`, `pending` or `unavailable`. Positive elapsed ns may be null. `received` requires the original receiving proof even when its clock observation was lost. When both intervals exist, receiving cannot precede result. |
| `counters`, `counter_unavailable` | Both contain every field named by the reference `Counters` type, with no extras. Each counter is a nonnegative integer or null; its reason is null precisely when a value was measured. Units and overlapping/summed scopes are fixed by the bound instrumentation definition. Known returns cannot exceed their known attempts. |
| `outcome` | Exactly `state`, `exit_status`, `reason`: `completed`, `failed` or `incomplete`; the actual integer child status or null; a bounded reason code or null. `completed` requires exit 0, verified output and received originals. An observed valid user result with an open tail is still `incomplete` overall. |
| `evidence` | Exactly `execution`, `source`, `artifact`, `policy`, `result`, `receiving`, `counters`, `auxiliary`. The first seven are `EvidenceRef` or null; `auxiliary` is an admitted list of references to the original detailed reader/fixture/envelope/failure observations. Every claimed known/verified fact requires the corresponding original proof. Null proof never inherits authority from another arm. |

Availability codes are `not-instrumented`, `unsupported-platform`, `not-applicable`, `original-pending`, `clock-lost`, `below-clock-resolution`, `measurement-lost`, or `execution-failed`. A missing counter or interval requires its actual code; do not put zero in its place. Outcome reasons use the existing owning execution/error identifiers, admitted under the evidence byte policy. Detailed errors remain in referenced original records. Failed/incomplete records remain valid report records but never silently become successful observations.

**`--cutover-reference`** is optional and has exactly `format="riverhog-durability-comparison-input/v1"`, `comparison_id` and `records`, an admitted list of `EvidenceRef` values pointing directly to these profile records. It cannot reference another bundle, launch a workload or supply the current invocation as its own reference. Validate every explicitly supplied record and its original proof first. Combine prior records with the newly observed current record, then partition by group/block/arm. The original run/receipt cannot occur twice; duplicates are errors, not extra repetitions. A different reference-group UUID is valid but unmatched to the current selection. Report valid records from another group/condition as unmatched with their identity retained; do not pool them or silently discard them.

The adapter creates the reference helper's `Observation` only after these validations: take block/order/input from `selection`, matching context from `selection.comparison`, instrumentation/conditions/interruption digests from the bound files, actual source/artifacts/policy and clock origin from the owner records, result/receiving intervals from their milestone fields, and `receipt_sha256` from the original execution receipt. It constructs the existing v2 sample from the verified positive byte count and result interval; the sample is a derived view, never a second authority. Require matching scenario, workload, **both** completed counts, input kind, shared comparison/binding fingerprints, and clock implementation/resolution. Clock/run origins are unique per execution, not equal across arms. B/C source/artifact/executor equality and A's explicit difference apply independently. For zero bytes, compare the same exact timing/count fields directly and omit byte-rate arithmetic. A valid result with missing elapsed is retained but not timed. If receiving is proved complete but its timestamp is unavailable, retain that distinction as `receiving-timing-unavailable`; do not translate it to the prototype's open-original-tail case. Known counters map directly; unknown counters remain null. Missing optional diagnostic counters do not erase valid elapsed observations, but cannot satisfy a qualification claim requiring those counters. The prototype implements the positive-byte, verified-result comparison subset, not this complete wire parser or proof validation.

**Argument and outcome rules are fixed:** neither cutover argument means existing behavior; `--cutover-arm` and `--cutover-context` must appear together; `--cutover-reference` is legal only with both. No reference is needed to record the first arm. Reject combining cutover arguments with generic `--context`, `--reference`, `--target-ratio` or `--transfer-log`; their older receipt/timing semantics are not implicitly imported. Preflight supplied files/proofs before launch: malformed/unreadable input, invalid hash/identity and conflicting duplicates are errors. An actual runtime artifact/policy differing from the expected selection is also an error, preserved with original receiving when discovered during execution. Valid but absent/unmatched A/B/C records produce `not-compared`, never a timing pass/failure. Actual failed execution remains an execution/qualification error with its raw record retained, including when that failure appears in a supplied reference; an unresolved original remains incomplete. Valid result timestamps may be compared while receiving is explicitly incomplete, but no full-lifecycle target or aggregate completion is claimed. A mere target miss remains report-only.

## 4. Completion boundaries and one clock origin

Use the owning profile parent/coordinator's monotonic clock for a run. Both milestones are **elapsed intervals from the same original admission origin**. Include the origin/run identity in the receipt. Observe child milestones through the existing returned result/receiving path and record the observation on that parent clock; do not subtract a child's timestamp from a parent timestamp. The underlying event may precede its acknowledged observation; the declared latency is to the verified user-visible receipt.

| Interval | Selected start/end and scope |
| --- | --- |
| Fixture preparation | Creation of test inputs and establishment of the service/backend fixture before measured workflow admission. Report separately. Real operation-specific mandatory identity/evidence setup after admission stays in workflow time. |
| `T_result` | Original operation admission to the first verified durable result receipt visible to the caller. Include ordinary queueing and required runtime work on that critical path. The receiver freezes this timestamp; later independent validation establishes whether the observation is valid. |
| `T_received` | The same admission origin to positive receipt of all original resources/required reclamation owned by this workflow. Services may remain running; unrelated service lifetime is not the workflow's cleanup. |
| Post-result tail | `T_received - T_result`, only when both belong to that one original clock domain and resource scope. Report cleanup that overlapped pre-result work separately in lifecycle counters; it is not added to this tail twice. |
| Independent reader passes | Complete ciphertext/history/payload verification under each declared reader state, with each observer's elapsed and work counts. Keep the exhaustive test observer separate from product runtime costs and product receiving. |
| Parent execution envelope | Helper startup, fixture work, measured workflow, verification, owned test resources and workspace cleanup. Preserve the current parent measurement as a distinct qualification interval. |

Record nanoseconds using `perf_counter_ns`/`monotonic_ns` at the owner; also record clock implementation/resolution and do not imply nanosecond physical accuracy. A process interruption can retain one surviving profile parent clock across child restart. If that parent/host clock is lost, keep separate segments and externally observed recovery milestones; there is no valid end-to-end subtraction unless an independently retained original observer actually covered the interval. Do not stitch host clocks or resumed monotonic origins into `T_received`.

An unresolved resource yields a missing receiving interval with its original retained owner evidence. A resource deadline may end the attempted measurement with an error or pending state; it never establishes original quiescence. The comparison can show a verified result interval with **receiving incomplete**, but cannot claim complete lifecycle cost, reclaim the workspace or produce a successful receiving observation. All artifact/paid-effect uncertainty remains with its original owner. Reuse `run_validation_child` and the existing received-resource mechanisms; a new `received: true` field is not a new authority. [Original child receiver][process]

For publication, place the result observation immediately around the original `IncrementalCollectionProducer.finish` result/receipt, with the semantic checks that already bind it to the final archive root. Capture workflow-owned stop/close/drain receiving independently. Keep fixture catalog checks, source deletion and the three full reader passes separately labelled. For other scenarios use their actual user-result and owner-resource boundaries, not a private phase with a similar name. Carry these semantic definitions into the matching context so an A/B/C comparison cannot mix command exit, publication selection and complete reclamation.

## 5. Normal-run arithmetic, noise and adverse results

For every matched B/C pair, report both raw times and the signed difference. Independently at the result and receiving boundaries, calculate:

\[
\delta_i = T_{adaptive,i}-T_{restart,i},\qquad
D_i=\max(0,\delta_i),\qquad
f_i=D_i/T_{adaptive,i}.
\]

Use exact integer cross-products for the observed default target: `8*D <= T_adaptive`, equivalently `7*D <= T_restart`. Report both boundary results; never select the more favorable denominator after seeing them. The durable-result interval describes user latency; the received interval and attributed lifecycle work expose delayed costs. A large fixed fixture/observer interval must not dilute either denominator. A/B comparisons describe the redesign; **A never supplies the optional-overhead denominator**.

The selected initial comparison for a useful changed owner is **three sequential paired blocks**, with execution orders A/B/C, B/C/A, C/A/B. This balances each arm's ordinal position. Prepare the declared state independently for every arm and keep workload concurrency fixed; do not run competing profile arms simultaneously. This is a finite small observation design, not authorization to run the held large campaigns. A first single block can expose a defect, but carries no claim that noise has been characterized. Do not discard slow completed runs or optional failures as warmup. Invalid environmental/fixture conditions are recorded as explicit unusable blocks with their original evidence.

Retain all block observations. Summaries include the paired signed mean/median, minimum/maximum, each `D_i` and ratio, the mean of pairwise clipped `D_i`, and the clipped mean of signed differences. These last two differ: paired differences -10 and +10 have mean 0, but their clipped values average 5. Showing only the latter would hide clipping bias. The reference tests exercise this case.

Call the range **observed paired spread**, not a confidence interval or worst-case bound. NIST's paired method operates on naturally matched differences; a confidence interval requires additional sampling/distribution assumptions and degrees of freedom. This proposal does not invent those assumptions from one or three diagnostic runs. No MTBF, universal latency guarantee or production speedup follows from policy algebra. [NIST paired observations](https://www.itl.nist.gov/div898/handbook/prc/section3/prc311.htm) · [NIST interval formulas](https://www.itl.nist.gov/div898/handbook/prc/section3/prc312.htm)

For normal work, three complete matched observations above the target at either declared boundary require an issue-local owner disposition: inspect attributable optional costs/reservations, correct underestimated setup/snapshot/restore/cleanup costs or excessive admission, and rerun only that affected matched cell. If an understood workload still cannot meet the intended economics, obtain an explicit maintainer disposition on the owning issue. Do not silently alter the denominator, classify optional work as mandatory, lower the default replay threshold or accept permanent unknown estimates as the supported long-work implementation. This repeated-miss flag is a descriptive escalation rule, not a significance test or CI timing failure. Correctness, evidence integrity, resource admission and deterministic policy tests retain their existing failure behavior.

## 6. Counters and full lifecycle cost

All measurements keep their units and attribution. Store `null` plus a bounded explanation for unavailable observations. Zero means the owner actually observed no such work. A successful run without the optional-store/attempt counters cannot prove the zero-optional-store promise. B observed creating optional recovery state is an invalid restart-arm record, not a noisy timing sample.

| Quantity | Required treatment |
| --- | --- |
| Critical-path elapsed | One owning monotonic interval per milestone. Do not derive it from summed child phase times. |
| Worker CPU | Actual per-original-process/thread CPU where supported, counted once after original receiving; keep observer CPU separate. Do not add a child-inclusive process aggregate to its nested child total. Missing platform support is unavailable. |
| Queue/blocking waits | Per-owner diagnostic sums and optionally an explicitly scoped union; neither is root useful credit. Do not infer CPU by subtracting overlapping waits from elapsed. |
| SQL and commit attempts | Actual observed statement/commit attempts and successful returns, with backend/connection and observation scope. A SQLAlchemy before-commit event is not a completed commit or physical synchronization. |
| Sync work | Observed filesystem/backend sync attempts and returns at the actual adapter/VFS/platform seam. Python `commit()`, rename and a successful sync-call observation are distinct from physical-device persistence or power-loss proof. No unsupported count is inferred. |
| Optional bytes | Actual snapshot/page/segment/dependency/selector and metadata bytes; distinguish encoded/written bytes, retained bytes and peak space. Include failed private generations, spill/compaction and reclamation in complete lifecycle observations. |
| Optional time | Original root/grant cost receipts include setup, serialization/hashing, write/sync/coordination/contention, failed attempts, lost-reply reconciliation and cleanup. Preserve reservations/unknown costs until the original owner reconciles them. No child credit, retry or terminal publication resets the root. |
| Policy/observer cost | Fixed policy bookkeeping is observed separately; incremental optional machinery remains charged as #995 specifies. Qualification collection, validation/readers and evidence-file writes have their own scope and overhead. |
| Replay/recovery | Lost useful work, actual replayed work, restore and validation, repeated transport bytes and original provider-effect attempts remain separate from normal optional cost. |

Use the existing operation/delegate growth hooks and owner evidence; extend each changed owner's actual primitive observer where required. Do not install a durable per-step product telemetry ledger to measure the removal of durable microsteps. Keep profile-local traces bounded by the existing evidence byte/numeric admission; reduce coalesced observations without inventing unobserved primitive counts. Instrumentation can perturb execution, so bind exactly the same observer across matched arms and report its scope. Uninstrumented and instrumented runs are different matching cells.

`D` is an end-to-end observational difference, not an exact sum of optional calls in a concurrent system. Attributed `optional_lifecycle_ns` may exceed or differ from `D`; preserve both. Failed optional attempts with no selected generation still consume real work, and successful generation publication does not settle later cleanup. Reporting transaction-count reduction without latency, useful work, correctness and retained tail evidence is insufficient.

## 7. Controlled interruptions and supported recovery

Use one declared semantic interruption plan for a paired comparison: e.g. the same accepted input frontier or before/after a required effect/result boundary. A disappearing old private transition is not a semantic marker. Preserve its failure counterexample at the new equivalent consistency/effect boundary. Adaptive-only generation-publication faults remain required safety tests, but are not matched three-arm latency cells when the other arms have no corresponding event.

For each interrupted run retain the original operation identity, selected input/history/configuration, exact fault marker and kind, last valid generation/mandatory frontier, actual loss/replay extent and complete receiving receipts. Report time to eventual valid result, original tail, restore/revalidation cost, repeated bytes, provider requests/paid effects and retained/reclaimed space. The same parent may observe a killed child and the replacement child; neither interruption nor replacement changes the logical root or gives extra optional budget. A parent/host crash without a continuous observer cannot yield a fabricated continuous elapsed interval.

Compare normal and interrupted work separately. In the reference helper the same paired arithmetic can describe eventual completion, but the normal-run 1/8 target is explicitly **not applied to the interrupted interval**: restoring/replaying lost work is not new useful-work credit or ordinary optional overhead. Recovery benefit is supported by actual reduced replay/validation/transfer work and eventual correct completion, not by a speculative interruption rate. There is no automatic provider retry/new identity after ambiguous entry; retain the existing exact intent and refusal/reconciliation rules from the affected owner.

Long supported paths must demonstrate a real default adaptive admission, complete independently usable generation, a subsequent interruption and actual restart/adoption. A permanent cost-unknown fallback cannot qualify the path. A non-checkpointable external tool accurately reports no mid-operation resume and retains its exact original outcome; do not add a fake cursor or give it useful credit for blocked tool time. Cheap replay must show zero optional attempts/generations **and zero optional-store initialization** while preserving mandatory persistence.

## 8. Finite workload coverage and existing holds

Keep existing F1/F16 authenticated/encrypted publication and maintained processing/lifecycle scenarios. The first useful owner slice needs short normal work, threshold-crossing supported work and an interrupted restart witness, without waiting for a new whole-repository benchmark campaign. Vary files/members, payload bytes, retained versus required history and relationship/fanout independently; record all dimensions rather than calling every workload merely “large.” Include unknown-length input, actual concurrency, fresh/warm/evicted readers and slow optional storage under the owning admission limits.

Use actual useful semantic work to cross the default 120-second saved-replay threshold. Sleeping, queue/provider/tool delay or lowering the policy for the only demonstration does not establish default adaptive support. Deterministic just-below/at/above threshold and estimator arithmetic belong to #995; owner experiments separately show the real executed promotion. For unsupported or blocked platforms/backends, preserve an explicit unqualified entry rather than treating a skip as success.

No F16, Docker/Compose, PostgreSQL, Windows/macOS, installed wheel/image, physical power-loss or live-provider campaign ran in this lane. Existing held 1,000/10,000/100,000 campaigns and provider permissions remain unchanged. Required final-source installed/backend/platform cases belong to the repository's existing qualification owners, with their original execution/receiving evidence; this file is not a replacement inventory or extra approval gate.

## 9. Executable reference and recorded validation

[measurement_comparison.py](implementation/measurement_comparison.py) is a side-effect-free comparison model using the real existing `scripts.performance_measurement` validators. It validates complete canonical default snapshots, exact shared conditions and B/C artifact identity, independently retains fresh-publication identities, rejects conflicting immutable results, prevents duplicate runs/receipts from becoming repetitions, preserves unknown counters and an open receiving tail, and computes exact fractions. Its common clock-origin field is a consistency check on already validated evidence, not a clock or original-resource implementation.

[test_measurement_comparison.py](implementation/test_measurement_comparison.py) has **48 passing cases**. Meaningful cases include at/below/above 1/8, a slow old executor that cannot mask a large B/C tax, a favorable user-result interval with an adverse receiving tail, negative signed differences, clipping bias, repeated adverse observations, mismatched source/artifact/cache/observer conditions, all five missing/custom defaults, optional work in restart mode, missing counters, conflicting captured semantics, distinct fresh identities, invalid counts/receipts, duplicate executions and separated interruption costs. This is deterministic evidence arithmetic, not a benchmark of redesigned product code. [Raw result](evidence/measurements_reference_stdout.log) · [JUnit](evidence/measurements_reference_junit.xml) · [timing](evidence/measurements_reference_timing.json) · [exact invocation and hashes](evidence/measurements_reference_invocation.json)

The unchanged maintained cohort is **69 passing cases** in `tests/unit/test_performance_measurement.py`, `tests/unit/test_transfer_profile_script.py` and `tests/unit/test_publication_profile.py`, including the real Native reader-state witness. Pytest reported 80.53 seconds; original process receipt was 83.694272396 seconds. The final reference cohort reported 0.12 seconds; original process receipt was 1.372882118 seconds. Both used the existing timing plugin and numeric policy. Do not add intermediate reruns or the successful standalone profile to these test counts. [Maintained raw result](evidence/measurements_maintained_stdout.log) · [JUnit](evidence/measurements_maintained_junit.xml) · [timing](evidence/measurements_maintained_timing.json) · [invocation](evidence/measurements_maintained_invocation.json)

Pinned Ruff 0.16.0 formatted/checked both owned Python files, and strict mypy passed for the comparison implementation. The initial lint check found one import-order error; the initial strict mypy check found nine narrowing/result-typing diagnostics. They were corrected with typed locals and a small `TypedDict`, without `Any`, ignores or warning suppression; format/lint, strict typing and all 48 tests then passed. [Static check commands, results and file hashes](evidence/measurements_static_checks.json). No product test failed in this lane. The test receipts correctly report source dirty while reference files were authored; the preceding standalone F1 receipt is separately clean at exact c28. The final merged implementation still requires its own exact-source qualification.

### Reproduction conditions

The dedicated `/workspace/scratch/939b84c043b0/qualification-1000-env` uses Python 3.12.3, pytest 9.1.1 and the unchanged lock SHA-256 `16b64145aa11bfbd6d2076a7a5adc783ed7d2e000237f9c31e9977ef48815f24`. It was created by the parent with pinned uv 0.11.24 and `uv sync --frozen --all-packages --group dev`; imported Core/Native/profiler paths were checked to be in this checkout. No previous environment was modified. External temp/output was `/workspace/scratch/939b84c043b0/1000-measurements-temp`. Numeric settings were the source-owned qualification profile: `max_work=907920445364`, `max_integer_bits=84`, numeric reader SHA-256 `4f08bc6148007c7194b2451cdd885b04ada30806b6ee1f6c5e6d29433885720a`.

The **maintained command to qualify in an equipped, clean product checkout** is:

```bash
make profile args='--scenario collection-publication --publication-workload /absolute/f1.json --output /absolute/f1-profile'
```

The actual direct observation used this existing owner after `bind_numeric_environment(os.environ, qualification_numeric_policy())`, with the recorded external `TMPDIR` and an assertion of clean c28 and correct loaded source paths:

```python
from scripts import transfer_profile

status = transfer_profile.main([
    "--scenario", "collection-publication",
    "--publication-workload", "/workspace/scratch/939b84c043b0/1000-measurements-temp/measurements_f1_workload.json",
    "--output", "/workspace/scratch/939b84c043b0/1000-measurements-temp/f1-profile",
])
```

Focused tests ran `python -m pytest -p scripts.ci_timing -o junit_family=xunit1`, the exact listed node paths, explicit external `--basetemp`, `--junitxml`, and `-q`. The complete commands, source state and selected numeric identity are in the linked invocation receipts. The reference schema/function changes proposed above are **not present in the product parser**, and the branch-local helper is intentionally not a new developer-facing command.

[agents]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md
[make]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/Makefile
[measurement]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/performance_measurement.py
[profile]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/transfer_profile.py
[publication]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_profile.py
[construction]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/canonical_history_scale.py
[observer]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/support/qualification/publication_evidence.py
[process]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/validation_process.py
[numeric]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/validation_numeric.py
[capacity]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/validation_resources.py
[measurement-tests]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_performance_measurement.py
[profile-tests]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_transfer_profile_script.py
[publication-tests]: https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/tests/unit/test_publication_profile.py
