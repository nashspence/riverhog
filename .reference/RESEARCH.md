# Primary research and exact-source anchors

Audited source is `dd09f4ab5e824935e680f26fba3d73d2484cb426`. The full-clone ZIP
SHA-256 is `f4d5330ca9e7bb26173782bd12ab7486cfd3de229a2bb2e5c7feac4b1598fb5f`;
base Git tree is `28a4cfe63be1e27e5a79c59e8d2146a5bb87f6f9`. No unpublished
integration source or preceding reference patch is used as the experiment base.

## Repository evidence, not new authority

[Issue #989](https://github.com/nashspence/riverhog/issues/989) supplies the failed
CI witness and 0.5803-second maintained-owner diagnostic. Those are issue-reported
observations, not new measurements by this packet. The issue scopes this to the
existing growth framework and explicitly preserves resource/receiving authority.

The following paths are all at the audited commit (prefix omitted for readability):

| Source | Finding used by this proposal |
| --- | --- |
| `riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py`: `advance`, `_select`, `_advance`, `_blocked`, old loop declaration | The actual scheduler has collection/resumption/submission phases, while the old graph attributes file/blocker work inline and bounds the return by L. Selection is bounded to three candidates per outer iteration. |
| `services/workspace_retirement_registry.py`: `collect`, `_submit`, `submit`, `resume_ready`, `_run`, `receive_stopped` | Exact original context, ordinary reservation and physical invocation survive uncertainty. Collection can count both an outcome and withdrawal. Capacity refusal may leave an unstarted closed context. Shutdown is a separate potentially waiting traversal. |
| `services/journal_retirement_registry.py`: `_scope`, `_create`, `_accept_progress`, `JournalRetirementTurn` | Complete controls are encoded before capacity refusal; actual Native work, full handled error formatting, blocker recording and close receiving belong to the original worker. |
| `services/physical_execution.py`: `_run`, `_collect`, `status`, `_PhysicalExecutionResult.release_exception_frames` | Future publication can precede thread exit. Collection uses the original thread's liveness/zero-wait join. Exception graph traversal and frame clearing are real receiving work, not a constant-size status bit. |
| `services/archive_construction_retention.py`; `physical_workspace_growth.py`; `workspace_retention_growth.py` | Retained ordinary charge and terminal physical worker admission are distinct. Reuse these existing mapping/identity/workspace cost contracts. |
| `journal_validation_retirement.py`; `retained_workspace_handles.py`; `packages/workspace-reclamation/src/workspace_reclamation/__init__.py` | Actual namespace/extent owners and reverse callback dependencies govern positive receiving. No reconstruction or GC shortcut is selected. |
| `tests/unit/test_upload_journal_validation_retirement_{catalog,loop}_growth.py`; `test_upload_journal_validation_retirement_growth.py` | Preserve independent catalog/control/epoch/cold/file dimensions and real delegated primitive observers; change the asynchronous interpretation, not the safety cases. |
| `tests/unit/test_journal_retirement_registry.py`; `tests/support/journal_retirement.py` | Maintained original-worker fixtures already express exact joins, ordinary charges, busy execution and close-only receiving. They are used directly, not replaced by a simulation. |

Links to the source root:
[Core services](https://github.com/nashspence/riverhog/tree/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services),
[maintained tests](https://github.com/nashspence/riverhog/tree/dd09f4ab5e824935e680f26fba3d73d2484cb426/tests/unit),
[growth framework](https://github.com/nashspence/riverhog/tree/dd09f4ab5e824935e680f26fba3d73d2484cb426/packages/riverhog-growth-contracts).

## External primary sources and design deductions

1. [CPython 3.12 futures documentation](https://docs.python.org/3.12/library/concurrent.futures.html)
   distinguishes scheduling a callable and retrieving its later result. Cancellation
   does not stop an already running call. This supports separate scopes; Riverhog's
   own original-worker contract supplies the stronger identity/custody predicates.
2. [Thread.join documentation](https://docs.python.org/3.12/library/threading.html#threading.Thread.join)
   describes waiting for termination and checking liveness after a timed join.
   A Future's completion is therefore not substituted for the actual thread join.
3. [Pinned CPython 3.12.3 Future implementation](https://github.com/python/cpython/blob/v3.12.3/Lib/concurrent/futures/_base.py)
   records FINISHED and wakes waiters when a result/exception is set; it does not
   join Riverhog's executing thread. The late-exit witness tests this distinction.
4. [Traceback documentation](https://docs.python.org/3.12/library/traceback.html#traceback.clear_frames)
   says clear_frames clears frame locals; exception graphs can retain large object
   graphs. The source-specific E/A/F bound and no-resource-inference rule are design
   deductions from this behavior plus Riverhog's exact traversal, not an external
   guarantee about application finalizers.
5. [Logging handlers](https://docs.python.org/3.12/library/logging.handlers.html)
   documents stream output/flush and queue-based handling. Logical diagnostic
   control counts do not prove a chosen sink is nonblocking. This handoff declares
   that limitation; it does not silently install another scheduler/logging policy.

The documentation routes may include later 3.12 maintenance wording. The pinned
3.12.3 implementation and the repository's exact locked runtime are the source and
execution anchors. No normative claim is inferred from a search snippet.

## Choices rejected

A larger integer assertion alone would still omit asynchronous file/receiving
work. Waiting in scheduling would reverse the accepted responsiveness boundary.
Charging every pending worker to each poll would double count work and could hide
failed attempts. Using Future.done, thread join, error formatting or frame disposal
as resource receiving would weaken #987/#988. Introducing a new tracing framework,
resource budget or scheduler is unnecessary: existing declarations and retained
identity witnesses can express these scopes directly.
