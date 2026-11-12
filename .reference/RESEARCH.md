# Primary-source research and deductions

## Exact repository anchors

Audited source: `c5157d69712185aa8d4ad185e2e6d465dfbd7179`.
The [owning issue](https://github.com/nashspence/riverhog/issues/988) and its
[reproduction](https://github.com/nashspence/riverhog/issues/988#issuecomment-6810070637)
identify the public gap. The issue's unpublished source was not inspected.

| Source at the audited commit | Finding relevant to this cut |
| --- | --- |
| [journal_store.py](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/packages/riverhog-provenance/src/riverhog_provenance/journal_store.py) | Exclusive raw creator close precedes store allocation, `_open`, and callback; directory sync has another raw origin. Final-reader release preserves a callback until success, with `_closed` assigned afterwards. |
| [journal_session.py](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/packages/riverhog-provenance/src/riverhog_provenance/journal_session.py) | Session callback forwards returned storage after creation. Move original construction retention before this boundary, not merely earlier within the callback. |
| [upload_journal_validation_owner.py](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/riverhog/src/riverhog_core/services/upload_journal_validation_owner.py) | Slot/lease cleanup depends on returned session; actual `advance` is used in both old-gap and reference-bridge witnesses. |
| [corpus_workspace.py](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/packages/riverhog-provenance/src/riverhog_provenance/corpus_workspace.py) | Guard/connection precede callback. Guard validates UID/mode/nlink/path identity. Its lifetime is not the journal creator's lifetime. Close clears callback before invoking it. |
| [sqlite_admission](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/packages/sqlite-admission/src/sqlite_admission/__init__.py) | Existing original connection/cursor/factory receiving is independent. The bridge uses a dedicated instance, not a replacement SQLite mechanism. |
| [test_sqlite_connection_admission.py](https://github.com/nashspence/riverhog/blob/c5157d69712185aa8d4ad185e2e6d465dfbd7179/tests/unit/test_sqlite_connection_admission.py) | Maintained Native retained creation is part of required platform behavior. A blanket POSIX skip is not the repair. |
| [prior #987 reference](https://github.com/nashspence/riverhog/blob/c961ff4390b4e4d8581249c47acd999bdc2aa37e/.reference/CLOSE_DESIGN.md) | Original native result recording, unknown-state retention and dependency receiving are reused, not reapproved as a general architecture. |

## External sources consulted

1. [Microsoft `_sopen_s`, `_wsopen_s`](https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/sopen-s-wsopen-s?view=msvc-170).
   Documents wide-path CRT creation, error output of -1, sharing flags and the
   O_CREAT|O_EXCL no-replacement behavior. The selected binary mode avoids text
   translation; CRT permissions are not an ACL/private-namespace guarantee.
2. [Microsoft `_close`](https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/close?view=msvc-170).
   Zero indicates successful close; the CRT descriptor and underlying OS handle
   are both closed. Invalid descriptors invoke invalid-parameter handling. Thus a
   retained CRT outcome is appropriate here; a separate CloseHandle would be wrong.
3. [Microsoft invalid-parameter handlers](https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/set-invalid-parameter-handler-set-thread-local-invalid-parameter-handler?view=msvc-170).
   Defines a thread-local handler and return of the preceding handler. The reference
   restores it around the close call rather than modifying process-global policy.
4. [Microsoft `_open_osfhandle`](https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/open-osfhandle?view=msvc-170).
   Describes ownership transfer to the CRT. No such transfer/adoption is selected;
   avoiding a second HANDLE-to-descriptor transition keeps this cut smaller.
5. [CPython 3.12.3 `posixmodule.c`](https://github.com/python/cpython/blob/v3.12.3/Modules/posixmodule.c).
   The supported Windows os-level API uses the CRT `_wopen`/close profile. The new
   `_wsopen_s` boundary makes the output/error contract explicit while preserving
   exclusive regular-file creation, binary behavior and the same CRT family.
6. [CPython 3.12.3 Windows configuration](https://github.com/python/cpython/blob/v3.12.3/PC/pyconfig.h).
   Confirms MSVC/shared-Python platform conventions. The extension requires the
   dynamic CRT and GIL-enabled build; static/foreign CRT and free-threaded builds
   are not silently qualified by this evidence.
7. [Linux close(2)](https://man7.org/linux/man-pages/man2/close.2.html).
   Discusses descriptor reuse after errors and close versus active I/O/durability.
   This supports the no-integer-retry and separate borrower/join/sync obligations.
8. [Apple close(2)](https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/close.2.html).
   Documents the close return/error boundary. The selected Darwin policy is still
   conservative: zero is positive, nonzero is unknown and cannot be retried by slot.
9. [Python sqlite3](https://docs.python.org/3.12/library/sqlite3.html) and the repository's
   independent connection owner above. File receiving is not evidence of database,
   cursor or final-reader closure; the existing owned scope supplies that proof.

## Explicit deductions, not extra OS promises

A retained Python callback over an integer cannot distinguish a lost close reply
from a different resource subsequently using that integer. Recording the native
result on the same pre-retained object removes the Python-return gap; treating
errors as unknown avoids guessing from current namespace/descriptor state.

The analogous constructor gap occurs before any returned object exists. Therefore
an earlier callback alone is insufficient: the caller needs passive original
receiving state before the first effect, with the independently retained connection
scope already attached. This deduction drives the required two-phase interface.

No source above promises universal positive receiving after an indeterminate close,
a private Windows ACL from CRT mode bits, directory fsync on Windows, immunity to
hostile descriptor manipulation in the same process, or that normal close flushes
all data. Those are not claims of this packet. Positive close and successful Native
initialization/validation/durability remain separate facts.
