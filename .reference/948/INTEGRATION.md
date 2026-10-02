# Integration-agent reading map

External reference for #948 under #903; not an instruction to merge. Start with
DESIGN.md. Reconcile the proposal with the owning issue and then-current main.
Do not install this SQLite model or these documents as production contracts.

## Snapshot reconciliation

Original authoritative branch base: `239985271c9ec24942073f4005dd18da621d0251`.
Previous reference: `6d5e3985fb9c781052594641773ba36ea1e05b30`.
Latest main audited on 2026-10-02: `88a3b3ae7d0addec10ced70b9eb04c7f96e0332f`.

Both full source snapshots were downloaded using GitHub (Additional Tools).
Archive and manifest SHA-256, every file's SHA-256/Git blob, and reconstructed Git
tree identities were verified. Removing `.reference/` from the prior reference
reconstructs the original base tree exactly. All 328 tracked files under
`some-implementations/stove0/` are byte- and mode-identical between the snapshots.

Main has 12 modified and one new non-generated file since that base, concerning
provenance/recovery/local materialization and Review0 qualification diagnostics;
the other differences are generated contract artifacts. None changes the audited
Stove0 execution boundary. The reference is revised by appending to its existing
history, not by rewriting its published SHA or mixing newer production files into
its diff. Latest-main reconciliation is recorded separately from historical base.

## Source map

Paths below are relative to `some-implementations/stove0/` at audited main
`88a3b3ae7d0addec10ced70b9eb04c7f96e0332f`. Named functions and tests were inspected,
not every file in the repository. Hash verification is not semantic code review.

| Source | Relevant evidence |
| --- | --- |
| `application/server/src/stove0_core/scheduler.py` | Sequential `advance`; `run_once` also advances admission and departure |
| `application/server/src/stove0_core/coordinator.py` | `step` renews claims; `_observe_one`/nested observation are synchronous; `_queue_or_poll` binds target request before PUT; cancellation is another reconciliation path |
| `application/server/src/stove0_core/work_state.py` | Observation request persistence, `record_target_status`, claim rebinding and cancellation; repeated status currently goes through `_replace` |
| `packages/target-support/src/stove0_target_support/persistent.py` | `put_job`, `_submit`, `_run`, `cancel_job`, `_recover_interrupted`; queued/running/canceling recovery and success checkpoint precedence |
| `packages/target-support/src/stove0_target_support/execution.py` | Refreshable execution session and exact completion ownership; keep these rather than copying fixture flags |
| `packages/observer-support/src/stove0_observer_support/http_binding.py` | Synchronous observe call under blocking semaphore |
| `packages/observer-client/src/stove0_observer_client/client.py` | Synchronous terminal-result HTTP contract; default timeout is 300 seconds, not a scheduler-pass guarantee |
| `packages/protocol/src/stove0_protocol/models.py` | Semantic request/result identity versus claim/fence-bound invocation; terminal-only observation evidence |
| `application/server/src/stove0_core/preview.py` | Future waits, nested observation, preflight and claim-abandoning finally |
| `application/server/src/stove0_core/admission.py` | Durable candidate followed by synchronous preview in `_advance_candidate` |
| `application/server/src/stove0_api/app.py` | `/v1/work` re-previews and checks accepted digest; `/v1/workflow-previews` invokes preview directly |
| `application/server/src/stove0_core/departure.py` | Durable pending intent and retry date, but synchronous receipt acquisition |
| `packages/target-protocol/src/stove0_target_protocol/departure.py` | Artifact-free intent/receipt identity distinct from ordinary effect jobs |
| `targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/target.py` | Encoding execution and target options; preserve transformation identity |
| `targets/nvenc-av1-opus/target/src/a_stove0_nvenc_av1_opus_target/common.py` | FFmpeg cancellation/process wait and execution timeout; wrapper-based admission would consume that timeout |

Read root AGENTS.md, README.md, architecture.md and current #903/#948 as well.
The policy is baseline convergence before v1, not compatibility aliases for retired
surfaces. Exact code, executable schemas and tests remain authoritative after
normal integration. No generated closure or release baseline is edited here.

## Integration sequence and regression focus

First establish the bounded-control and never-started/possibly-started invariants
in the real runtime tests. Add dormant dispatch to target support while preserving
refresh, completion checkpoint and output custody handling. Then add pollable
observer/departure delivery and resumable caller state together; do not leave a
synchronous fallback in nested planning or preview admission.

Include the `/v1/work` accepted-preview revalidation path. Making preview pending
must neither rerun it inline nor bypass exact digest validation. Scheduling changes
cannot weaken stale-claim/implementation rejection or authorize output effects
from preview. Independently schedule claim maintenance, remote contacts and cleanup.

Existing `packages/target-support/tests/test_target_support.py` cases to retain:
`test_persistent_target_shutdown_and_operator_cancel_have_distinct_state`,
`test_running_target_receives_capability_refresh_without_persisting_secrets`,
`test_queued_target_constructs_runtime_with_refreshed_authority_before_claim_read`,
`test_uncertain_effect_commit_stays_interrupted_and_never_auto_repeats`,
`test_published_success_wins_late_cancel_and_cleanup_failure`, and
`test_persistent_target_resumes_sealed_publication_without_rerunning_operation`.
Also retain `test_departure_target.py` exact-intent and lost-response receipt replay.
The standalone reference suite does not replace these production tests.

The updated model adds checks for cancellation surviving restart, cancellation of
stopped interrupted work, foreign permit rejection, and already-proven completion
after lease loss. It still cannot prove process containment, transport deadlines,
capability freshness, completed checkpoint reconciliation or distributed CAS.
Pending is not failure; cancel is not rollback; a resource permit is not completion
authority. Preserve those distinctions in schemas, error handling and UI state.

## Evidence still required in normal integration

The actual NVIDIA target must wait on a synthetic external scheduler while unrelated
work progresses, without spawning waiting FFmpeg processes or consuming payload
slots. Cover restart before grant, canceled jobs receiving late grants, refreshed
authority, one valid execution after grant, and execution-only timeout accounting.

Exercise real HTTP concurrency and finite per-call/pass budgets, backlog limits,
fair scans, PostgreSQL multi-scheduler races, durable cancellation before dispatch,
preview/nested continuation, independent claim renewal, and effect reconciliation.
Derive stop/noncommit/completion facts from real supervision/evidence, never from
untrusted Booleans equivalent to this model's test inputs.

Update actual client/binding/schema/CLI/operator views, distribution dependencies,
state fixtures/baselines and generated contracts using repository-owned tools.
Run the normal focused suites and `make lint`, `make unit`, `make dist-smoke`,
`make build`, and required pushed-SHA checks on integrated production changes.

Open interpretation: bounded metadata/preflight belongs in the control allowance.
This reference recommends covering all affected preview/admission and departure
paths; it does not silently authorize changes to independent Review0 sampler APIs.
No universal scheduler backend, priority model, permit-renewal wire protocol, generic
workflow framework or mid-execution preemption is needed for this v1 change.
