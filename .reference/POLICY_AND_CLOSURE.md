# Exact policy qualification and aggregate closure

This is concrete external input for [#1000](https://github.com/nashspence/riverhog/issues/1000), governed by [#994](https://github.com/nashspence/riverhog/issues/994). The integration agent owns execution and the resulting GitHub evidence record. Nothing in this file closes either issue or resumes #991.

## 1. Deterministic admission tests stay with the common policy

Adopt the policy at #995's owning package and test its exact selected behavior there. Do not implement a second predicate in the qualification tool. The pinned [policy proposal](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/implementation/replay_policy.py), [predicate tests](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/implementation/test_replay_policy.py) and [accounting tests](https://github.com/nashspence/riverhog/blob/b545a77996fbc9fe98019796860473bda1bb1aa0/.reference/implementation/test_accounting_model.py) are executable input. Their earlier model results are not actual storage, authority, backend or cross-owner proof.

Use exact integral time units or the common policy's exact rational comparison; do not round a just-above budget down into admission. For beta=1/8, b=1/7. Candidate admission requires all of S>=120 seconds, 7*C<=S and 7*(O+C)<=U, after prerequisite and end-of-work priority checks. S is net saved recomputation after candidate load/verification; O contains actual charged attempts plus held lifecycle reservations. R unknown is distinct from zero.

| Test family | Concrete cases and required result |
|---|---|
| Defaults/configuration | adaptive,120,1/8,unset lifetime count,two usable generations; every supported owner receives those effective values. Reject bool-as-number, nonfinite/out-of-range values, unknown keys and superseded aliases. Server/operator policy cannot be overridden through a client-only option. |
| Target | At 120s minus one representable unit, exactly120s, plus one unit; only the latter two satisfy the target. No byte/member/microstep threshold substitutes for S. |
| Candidate budget | 30s candidate with S=210s minus/equal/plus one unit; include large workflow credit to prove the candidate constraint still applies independently. |
| Root budget | Hold S sufficient and vary U around 7*(O+C). Include concurrent disjoint reservations, failed-attempt charges, deferred cleanup reservations and actual overruns. |
| Restore subtraction | Equal gross replay but expensive verification can make net S fall below target or zero. Do not subtract the same restore cost again in C. |
| Completion priority | Completion or a mandatory anchor ready wins, including when optional mode/count/storage/accounting would otherwise refuse. No redundant final optional checkpoint. |
| Remaining work | R<C, R=C, R>C and unknown; equality prefers finishing. Changing estimates reassess later, without inserting artificial delay or forgetting earlier charges. |
| Cost unknown | Unknown or unadmitted C, missing saved-work/restore estimates and unsafe dependency closure suppress optional entry. Ordinary required work continues. Supported large-workflow adaptive capability must later be demonstrated with a finite admitted model. |
| Limits | Restart and count0 suppress optional setup, including a zero-cost candidate. Successful and in-flight count slots share one logical lifetime ceiling. Creation count, usable-generation retention and storage bytes remain separate. |
| Accounting | Overlapping fresh work earns elapsed credit once. Replays, polling/backoff, provider waits, phase changes and child/session replacement do not mint credit. Charges/remaining reservations reconcile exactly once after a lost reply. |
| Crash/cancellation | Unknown grants or accounting remain conservative; no refund/reissue from disappearance. Completion blocks new optional entry while preserving receiving of already admitted originals. Stale revisions cannot publish or settle a different generation. |
| Retention | A predecessor survives private construction and partial failure; only complete current-bound dependencies become selectable. Pinned readers may keep extra generations within storage admission. A count target never authorizes deletion of a live dependency. |

Test delivery through real owner admission/anchors, not only the pure function. Required cross-owner witnesses include two concurrent child domains competing for the same root allowance, restart after reservation/selection/lost result, a late original outcome during cancellation, and a root finishing while a cleanup liability remains. Existing current-owner transactions must protect the summary; the qualification observer must not cause per-turn durable accounting.

Deterministic controllable clocks test threshold decisions without sleeping. They cannot establish a measured two-minute default-policy product resume benefit. Actual long-path evidence uses real useful work and the default policy. A lowered target must be labeled in its configuration and cannot be mixed with default C observations.

## 2. Three arms and interrupted outcomes

Use the [measurement implementation](MEASUREMENTS.md) through the maintained `make profile` owner:

- A: preserved pre-redesign source, mandatory correctness intact, actual legacy behavior recorded.
- B: candidate redesigned source, restart mode; every mandatory barrier remains.
- C: the same candidate executor/source/artifact and conditions, default adaptive policy.

A/B comparison describes the executor change. B/C comparison estimates optional overhead; A is never the optional-budget denominator. For each complete matched normal-success pair, separately at the common-origin durable-result and original-resource-receiving boundaries, preserve signed delta=Tc-Tb, D=max(0,delta), D/Tc and the default equivalent D<=Tb/7. Interrupted eventual-completion differences remain separate and do not receive the normal-run 1/8 target. Preserve raw samples, order and adverse runs; a negative delta is noise/interaction evidence, not evidence of negative checkpoint cost. Range/median summaries are descriptive. Do not call a one-run range a confidence interval.

Do not sum nested/parallel wall times. Keep cumulative worker CPU, blocked/queue intervals, catalog statements/commit attempts, observable syncs, bytes, physical allocation, optional setup/attempt/reclaim costs and restored/replayed work separately typed. An unavailable sync counter is unknown, not zero. A COMMIT count is not a physical fsync count.

For interrupted runs report the observed fault model/boundary, original execution receiving, last usable generation, discarded work, restore verification, replayed network bytes/paid requests, time to eventual validated output and retained/cleanup space. Required failure or missing authority is retained as a failed/incomplete outcome, never filtered out of the sample set. Retrying a previously uncertain provider effect requires its owning reconciliation/consent protocol.

A repeatable excessive optional tax is resolved by the owning estimator/reservation/backoff mechanism: include omitted lifecycle cost, increase the conservative future estimate for that owner/backend, retain actual debt and suppress underfunded new candidates. Re-measure the affected matched condition after that concrete change. An exceptional unresolved tradeoff requires a specific maintainer disposition on #994/#1000. Timing remains observational; a controller that violates the deterministic predicate is a correctness failure.

## 3. Acceptance evidence is source-scoped

Use the source discovery described in [COVERAGE_AND_CUTOVER.md](COVERAGE_AND_CUTOVER.md). Keep owner/disposition tracking on GitHub, with one lead for each shared seam. The record for a changed or evidenced-no-change owner must identify:

- exact implementation source and owning operation/delegates/state/consumer;
- archival, mandatory, optional and volatile state actually reached;
- concrete retained obligation and failure counterexample, earliest required durability and minimal safe batch boundary;
- changed public recovery promise or explicit no-change rationale;
- exact executed node/workload/backend/platform/fault scope, source and receipt/artifact location;
- any remaining blocked qualification or unrelated owner obligation.

This is the required meaning of an issue-local disposition, not a new repository schema or handwritten exhaustive owner registry. Discovery and actual test definitions remain source authorities. A no-change decision must be backed by relevant code and behavior; an existing name or SQLite table does not establish mandatory durability. Owner-local primitive proof can be reused, but distinct public/workflow/installed routes still need their real composed evidence.

### Evidence levels

| Evidence | What it can establish | What it cannot establish |
|---|---|---|
| Source-derived inventory/contract | Discovered declared owner/consumer/binding at that source | That the behavior executed correctly or every dynamic path was discovered |
| Pure policy/comparator test | Deterministic decisions, record validation and arithmetic | Real clock economics, persisted generations, platform/provider safety |
| Actual exception/lost-reply witness | Its exercised control/effect/receipt window | Arbitrary process/OS/device crash behavior |
| Original child SIGKILL and cold reopen | Process death at the observed handshake and that backend's actual reopen | Power-loss ordering, a different OS/filesystem, external provider quiescence |
| Qualified native/backend fault witness | Exactly exercised native primitive/configuration and failure model | Universal hardware survival or support on an untested platform |
| Full public/installed lifecycle | Its authenticated route, actual built artifacts, outputs and receiving | Other versions, unrelated paths, real providers not exercised |
| Exact final-source plus tested-merge CI | Applicable required jobs and original evidence for those exact checkouts | Release/provider/deployment permission or closure without the scoped cutover claims |

SQLite's own crash tests distinguish simulated VFS reorder/corruption from ordinary process termination; filesystem research shows application crash protocols depend on concrete persistence properties. Therefore qualification labels must describe the actual injection/runtime and preserve their limits. See [SQLite testing](https://www.sqlite.org/testing.html#crash_testing) and [Pillai et al., OSDI2014](https://www.usenix.org/conference/osdi14/technical-sessions/presentation/pillai). These references motivate the distinction, not certify Riverhog.

## 4. Exact final source and tested merge

Record the candidate head H, intended base B and tested merge M. Derive them from Git/GitHub at execution and verify their exact relationship; a mutable branch name is navigation only. Each evidence owner records its actual checkout and effective command/toolchain/resources at start and finish. Source files must be exclusively owned throughout execution. An environment with editable imports into a different checkout does not qualify H merely because the shell is in H.

Current [CI](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/.github/workflows/ci.yml) checks out `inputs.ref || github.sha`. For `pull_request`, GitHub's event SHA normally identifies the tested merge, distinct from the head SHA; bind both explicitly rather than assuming the check label identifies H. [GitHub event documentation](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows#pull_request).

Consume the exact applicable source/attempt artifacts through [ci_actions.py](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/scripts/ci_actions.py) and its existing work definitions. Retain valid original completed work through retry only under the current proven-origin rules; do not substitute an old green result for a fresh failed execution. Stale base/head, changed dependency/configuration, different generated contracts or built artifact revisions require reconciled evidence. Do not claim H and M are the same qualification source.

The required local/checkpoint/full gates remain those in [AGENTS.md](https://github.com/nashspence/riverhog/blob/c28eceaa2dfec1b3712135469ba12230e74f5ffa/AGENTS.md). At minimum integrate focused owner checks and `make lint`, then applicable existing complete suites, SQLite/PostgreSQL, native clients, installed wheels/images/Compose and independent recovery. A missing tool blocks its actual gate; direct Python tests do not impersonate `make`, Docker, official-age, a native platform or live provider execution.

## 5. Finite aggregate closure record

The integration agent posts the final evidence decision to #994 only after the delivered implementation meets that issue's completion criteria. Use these concrete sections; fill them from actual receipts, not expectations. This reference does not post a speculative completion comment.

1. **Delivered source:** exact H/B/M identities, built/installed artifact identities, effective toolchain/resource policy and actual qualification run/attempts.
2. **Per-family disposition:** #995 policy/composition; #996 Native/generations; #997 public publication/custody; #998 processing/applications; #999 storage/read/recovery. Link each owner’s concrete implementation/no-change and executed evidence; explain shared evidence reuse once.
3. **Coverage reconciliation:** exact generated discovery/contract/state/component projections and resolution of every affected owner/consumer; removed superseded runtime/adoption paths; no omitted dynamic path or unexamined mandatory label.
4. **Safety/growth:** changed-boundary fault results and resource receiving, lifecycle growth/numeric evidence, exact archival/semantic equivalence, installed independent recovery; enumerate actual platform/backend limits and blocked/skipped witnesses.
5. **Economics:** matched A/B/C records, useful saved replay and interruption cost, user-result and terminal tail, signed/noisy/adverse observations and concrete optional over-admission dispositions. State unavailable comparisons plainly.
6. **Contract/state cut:** coordinated public/configuration consumers and generated artifacts; preserved protected identities/operator data; current-state refusal and supported upgrade survival. No startup erasure is disguised as migration.
7. **Decision and remaining work:** whether every #994 completion criterion is met; explicit unresolved cutover blockers if not; unrelated #964/#469 work that remains separately owned; finite disposition of each consumed reference.
8. **Precedence:** only after the parent is complete, or a separate explicit maintainer change, record the resulting #991 resumption disposition. Do not resume the held larger campaigns or infer merge/release/provider authorization.

If a required cutover witness is blocked, the closure decision is **not yet complete**, with its exact owner and needed environment recorded. That is an implementation/qualification condition, not an unresolved architecture choice. Stop once the finite cutover is demonstrated; do not prolong this reference as an alternate implementation branch.
