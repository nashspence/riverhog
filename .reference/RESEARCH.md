# Research and source audit

## Primary platform/runtime evidence

1. [Linux close(2)](https://man7.org/linux/man-pages/man2/close.2.html) describes
   early descriptor-slot reuse, errors after release, and why retry can close a
   different resource. It also documents divergence from newer POSIX EINTR
   semantics. The design does not assume all POSIX systems share Linux behavior:
   any nonzero close result remains unknown. Receiving is not an fsync guarantee.
2. [Apple's archived close(2) manual](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/close.2.html)
   describes deletion of the per-process reference, zero success and possible
   EINTR/EIO. This is archived API documentation, not qualification of a current
   Darwin release. No success is inferred from an error or subsequent slot probe.
3. [CPython 3.12.3 fileio.c](https://github.com/python/cpython/blob/v3.12.3/Modules/_io/fileio.c)
   makes the file object's fd negative before native close, returns close errors,
   and rejects directories. Thus FileIO object identity is useful but `.closed`
   alone is not a general native-success receipt, and it cannot supply this
   directory backend. Assigning a newly constructed object after acquisition also
   does not provide the requested pre-acquisition caller retention.
4. [Python signal execution](https://docs.python.org/3.12/library/signal.html#execution-of-python-signal-handlers)
   says Python handlers run later in the interpreter, not inside the low-level
   signal handler. This supports committing the native result on the retained
   object before delivering a Python exception. The reference calls no Python
   callbacks between the syscall and its bookkeeping. Its explicit test signal
   is checked only after state is recorded; forced native cancellation is outside
   the profile.
5. [CPython thread/GIL API](https://docs.python.org/3.12/c-api/init.html#thread-state-and-the-global-interpreter-lock)
   supplies the allow-threads boundary used around blocking syscalls. Ownership
   state changes are serialized under the GIL, and in-flight effects remain busy.
   The reference explicitly rejects free-threaded builds rather than treating the
   GIL assumptions as universal.
6. [Linux open(2)](https://man7.org/linux/man-pages/man2/open.2.html) distinguishes
   the descriptor table slot from an open file description, documents O_CLOEXEC
   and O_NOFOLLOW, and explains relative directory lookup. Original handle
   duplication is not reopening a matching pathname. O_NOFOLLOW protects the
   final component only; existing safe-root authority is retained separately.
7. [PEP 475](https://peps.python.org/pep-0475/) treats close/dup2 differently from
   normally retried EINTR syscalls. A generic Python retry helper is consequently
   the wrong receiving abstraction. The implementation uses a single native close
   and does not infer successful closure from that PEP's higher-level API advice.

The Open Group 2024 close/open pages were requested but returned HTTP 403 in this
research environment. Their text was not treated as independently verified
normative proof. Neither POSIX-version detection nor an assumed universal EINTR
meaning is used by the selected algorithm.

## Exact Riverhog owners

All repository paths below are at
[`0e45d35a64d18dbde0f88b89d3c55d60b4977d28`](https://github.com/nashspence/riverhog/commit/0e45d35a64d18dbde0f88b89d3c55d60b4977d28).

- `packages/workspace-reclamation/src/workspace_reclamation/__init__.py`:
  `reclaim_retired_files` keeps directory/data integers in local frames and closes
  in finally. `_declaration` bounds successful opens/closes but cannot make a
  caller-retained receiver out of a frame lost through an exception.
- `riverhog/src/riverhog_core/retained_workspace_handles.py`:
  `_close_descriptor` and `RetainedWorkspaceHandles.close` retain/retry a callable
  and integer, rather than the original resource plus its native outcome.
- `canonical_discovery_retirement.py`: `DiscoveryRetirementTurn._open`, `_acquire`,
  `_advance` and `close` compose parent custody; the metadata helper call has no
  retained child data owner slot. Its `_closed` assignment alone is insufficient.
- `canonical_discovery_parent_reclamation.py`: `DiscoveryParentRetirement.close`
  leaves old integers in slots until os.close returns normally; a lost reply can
  therefore retry against a recycled slot.
- `canonical_discovery_retired_metadata.py`: `_close_namespace_handles` and
  `close` contain related raw directory/data handles. SQLite connection ownership
  remains independent and must not be certified by a file-only receiver.
- `journal_validation_retirement.py` and `canonical_discovery_child_reclamation.py`:
  relative namespace opens register raw integers after acquisition.
- `journal_validation_workspace_reference.py`: `ValidationWorkspaceReference`
  retains the same FileIO object for close retries, and duplicates held custody;
  use that lifetime principle, not a pathname/inode surrogate or a generic
  assertion that the stream's closed flag proves every native failure settled.
- `packages/riverhog-provenance/src/riverhog_provenance/{journal,corpus}_reclamation.py`:
  the neutral helper is shared by Native as well as Core. The repair must stay in
  a neutral package and thread retained receiving through both boundaries.

Repository policy and build examples were read in `AGENTS.md`, `README.md`, the
workspace-reclamation tests/pyproject, the existing client native hatch build,
and the growth-contract implementation. Source evidence is not a new run of
integration's two maintained DiscoveryRetirementTurn reproductions.

## Deductions and selected policy

The native owner is a design deduction from the Python handoff gap, not an OS
feature named "receiving". The positive criterion is intentionally narrower than
some OS release guarantees: zero native close result, retained before Python
reply; otherwise fail closed. This keeps physical uncertainty charged without
assuming a raw integer can be retried. Object lifetime evidence is process-local;
archive, retired namespace and durable resource-budget authority remain owned by
the current application. Tests demonstrate the reference boundary, not all those
owners merely because the reference imports no implementation from them.
