# Research and exact-source reconciliation

The audited checkpoint is `dd09f4ab5e824935e680f26fba3d73d2484cb426`.
The owning request is [#989](https://github.com/nashspence/riverhog/issues/989).
This document distinguishes source observations, documented language behavior and
the selected design. It is not a replacement for the repository's declarations.

## Owning implementation and existing evidence

All repository links in this section are pinned to the audited checkpoint.

| Owner | Source observation relevant to this handoff |
| --- | --- |
| [Upload journal retirement](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/upload_journal_validation_retirement.py) | The old loop declaration names inline select/unit/blocker children and a one-per-limit progress bound. The current `advance` instead collects, resumes, selects and submits. `_advance` requires the original turn. |
| [Journal registry and actor](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/journal_retirement_registry.py) | Exact job/control scope and passive actor construction are local here. Actual unit work, namespace receiving, error formatting and blocker recording run in the actor. |
| [Shared registry](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/workspace_retirement_registry.py) | Collection counts physical outcomes and ordinary withdrawals separately. Resumption submits only close work on the retained actor. The audited collection path also clears exception frames; this is the candidate's narrow execution-placement correction. |
| [Physical execution](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/physical_execution.py) | The actual thread, original identity and terminal slot are retained. Collection checks physical liveness and performs its own join before returning an outcome. Its existing growth declarations remain delegates. |
| [Ordinary retention](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/archive_construction_retention.py) | Original reservation authority and retirement are distinct from physical execution grants. Their existing declarations/observers are reused. |
| [Namespace receiver](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/retained_workspace_handles.py) | Reverse callbacks retain original Resource/Borrow pairs and stop at unresolved receiving. Callback removal requires actual success. |
| [Neutral file reclamation](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/packages/workspace-reclamation/src/workspace_reclamation/__init__.py) | The original file/directory receivers and admitted truncation work have their own primitive growth contracts. A scheduling result is not their receipt. |
| [Service scheduler](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/services/collection_uploads.py) | The parent scheduler counts a truthy retirement scheduling result once. That remains distinct from the child's bookkeeping count and eventual physical progress. |
| [Growth framework](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/packages/riverhog-growth-contracts/src/riverhog_growth_contracts/__init__.py) and [owner envelopes](https://github.com/nashspence/riverhog/blob/dd09f4ab5e824935e680f26fba3d73d2484cb426/riverhog/src/riverhog_core/growth_bounds.py) | Exact function identity is part of a source binding. Numeric monotone envelopes and child validation already exist; the reference does not add another growth framework. |

The retained test families are the loop, unit, catalog, service-scheduler,
journal-registry and discovery-registry tests under `tests/unit`, with the exact
ownership fixtures in `tests/support/journal_retirement.py`. They supply actual
SQLite snapshots/VM and driver counts, retained native-resource counters, file
extent checks, scope serialization, metadata admission, real threads and joins.
The new test observer delegates to these maintained observers instead of building
an alternate storage or execution model.

## Primary language/runtime references

[Python 3.12 concurrent.futures](https://docs.python.org/3.12/library/concurrent.futures.html)
documents submission as scheduling a callable and returning a Future. Running
work is not canceled by requesting cancellation of pending futures. **Design
inference:** a submission cannot be represented as an already-executed child
sample. The actual worker gets a separate measured root linked to the submission.
This does not claim the repository uses ThreadPoolExecutor for this owner.

[Python 3.12 threading](https://docs.python.org/3.12/library/threading.html)
documents join/liveness separately and explicitly notes that thread identifiers
can be recycled. **Selected evidence rule:** retain actual worker objects and
original context/reservation identity; use case-local tokens, not numeric thread
IDs or wall-clock overlap, for attribution. Join establishes thread termination,
not an application resource receipt.

[Python 3.12 traceback](https://docs.python.org/3.12/library/traceback.html)
documents that `clear_frames` clears traceback frame locals. **Design inference:**
this is real exceptional execution work, potentially releasing referenced objects;
it is not a constant bool check and does not certify a descriptor or SQLite
receiver. The proposed cut moves it from collection to original close execution,
while preserving the first failed outcome and every resource-owning actor.

The 3.12 documentation is a maintained patch-series reference, not proof that
arbitrary callbacks/finalizers have bounded latency. The workflow selects the
repository's locked CPython version; source/runtime/result identities are recorded
separately. Runtime activation and product platform coverage are not inferred from
these documentation pages.

## Derivation versus evidence

The `4L` return bound follows directly from the three finite phases in source:
`(L completions + L withdrawals) + L close submissions + L new submissions`.
The three- and four-event tests discriminate that formula from the obsolete `L`
meaning. They do not derive the formula by fitting an observed maximum.

Likewise, three candidates per outer iteration derives `3L` selection/membership
attempts. Each selection gets at most two excluded jobs, deriving `6L` copied
references. Child primitive work is then charged using the actual declared
catalog, execution, retention and Native/file owners. Attempted and failed work
is retained even when the scheduling root has already returned or a worker has
not completed its physical receiving.

The reference explicitly distinguishes kernel/index work and logging/finalizer
latency from counted API entries. It neither asserts a wall-time guarantee nor
introduces a hidden total-workspace ceiling. A resource unknown can still retain
its original capacity under the already selected receiving policy.
