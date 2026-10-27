# Research and source audit

Research date: 2026-10-27. Only primary Microsoft, SQLite, CPython and Riverhog
sources support the technical observations below. The proposed algorithm and
trade-offs are design deductions, not quotations or claims of executed Windows
qualification. Source IDs are local navigation for this reference, not a new
product rule registry.

## Primary sources and what they establish

**S1 — SQLite atomic commit.** [Atomic Commit](https://sqlite.org/atomiccommit.html),
especially hardware assumptions and section 7.6: transaction recovery assumes
working storage flushes; persistent rollback avoids journal-name deletion by
invalidating its header. This supports transactional logical names, not a claim
that a file-only publication algorithm can skip durability. Do not manually remove
a potentially hot journal. The document describes engine behavior, not proof
about this proposed application adapter.

**S2 — SQLite durability and capacity settings.** [PRAGMA documentation](https://sqlite.org/pragma.html):
PERSIST retains its journal, EXTRA strengthens rollback-mode synchronization,
page/cache/journal limits have distinct meanings, and incremental auto-vacuum is
explicit maintenance. Read back actual settings. FULL/EXTRA wording must be
interpreted with the chosen VFS; EXTRA does not manufacture a Windows directory
fsync. Use actual receipt bytes in receipt cost, not a fictitious constant encoding.

**S3 — Windows VFS source.** [SQLite 3.45.1 `os_win.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.45.1/src/os_win.c),
`winSync`, `winDelete`, `winOpen`: syncing uses `FlushFileBuffers`, directory-sync
argument handling differs from POSIX, and locking-enabled versus no-lock VFSes
are distinct. Therefore specify the normal Windows locking VFS, record the actual
runtime and verify its source. This audit does not endorse arbitrary custom VFSes.

**S4 — Commit ordering source.** [SQLite 3.45.1 `pager.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.45.1/src/pager.c),
`zeroJournalHdr` and pager invariants: database synchronization precedes journal
invalidation, and header invalidation is synced when synchronization is enabled.
The proposed small selection transaction relies on the engine, not atomic
multi-sector application writes or an fsync-like function name alone.

**S5 — Windows flush and metadata.** [FlushFileBuffers](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers)
and [File Caching](https://learn.microsoft.com/en-us/windows/win32/fileio/file-caching):
file flushing and write-through control cached writes/metadata; volume flushing
requires administrator privileges. The design uses unprivileged file/database
operations and an existing durable parent. These pages are not treated as a
complete contract for arbitrary newly-created directory chains or durable unlink.

**S6 — Windows open/share/security controls.** [CreateFileW](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew):
creation disposition, access/sharing, handle inheritance and open-reparse-point
flags are explicit. A share-denied permanent owner handle is useful across
transactions. A database identity guard must still allow SQLite's own opens.
Write-through metadata language is not extrapolated into a full replacement for
the POSIX mkdir/link/directory-fsync protocol.

**S7 — Identity and namespace.** [GetFileInformationByHandle](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfileinformationbyhandle)
and [GetFileInformationByHandleEx](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getfileinformationbyhandleex)
expose identity/attribute information for handles. [Reparse points](https://learn.microsoft.com/en-us/windows/win32/fileio/reparse-points)
include directories, not just familiar symlinks. Pin and verify ancestry as well
as final files; reject links/aliases instead of relying solely on string resolution.

**S8 — Private state and known-folder placement.** [Security descriptor strings](https://learn.microsoft.com/en-us/windows/win32/secauthz/security-descriptor-strings)
route to native security descriptors; POSIX chmod bits do not specify the desired
Windows DACL. [SHGetKnownFolderPath](https://learn.microsoft.com/en-us/windows/win32/api/shlobj_core/nf-shlobj_core-shgetknownfolderpath)
provides the current user's known-folder path. Its result still needs the backend's
local-volume, security and no-reparse checks. No literal user/deployment path is
embedded in this reference.

**S9 — Rejected rename shortcut.** [MoveFileExW](https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-movefileexw)
describes move flags, including write-through and cross-volume copy/delete.
It does not by itself supply this store's whole initialization, no-clobber,
reclamation and accounting algorithm. The chosen database backend avoids depending
on a newly inferred whole protocol from this single call.

**S10 — Engine bounds and integrity assumptions.** [SQLite limits](https://sqlite.org/limits.html)
include a per-row/BLOB size limit; chunking is required to avoid treating that as a
member-journal limit. [How to corrupt SQLite](https://sqlite.org/howtocorrupt.html)
explains hazards such as broken locking/flushes, hot-journal deletion, moving live
databases and multiple links. A database dependency does not remove ownership,
privacy, correct error handling or realistic media assumptions.

**S11 — Toolchain pin is not the observed binary.** [CPython v3.12.3 externals](https://raw.githubusercontent.com/python/cpython/v3.12.3/PCbuild/get_externals.bat)
names SQLite 3.45.1.0 in its upstream Windows build recipe. Riverhog pins Python
3.12.3, but mise/runtime packaging must still be observed. Record
`sqlite_version()`, `sqlite_source_id()`, compile options, enabled VFS and installed
artifact identity in native evidence. No downloaded latest SQLite DLL is proposed.

## Audited Riverhog source (exact base)

All paths below are relative to the unchanged reference tree, at
`8c94eb3e5e349147d46b8f10fc25e161cb7106be`.

| Path / symbol | Finding relevant to this handoff |
| --- | --- |
| `packages/riverhog-client/src/riverhog_client/_prepared_primary_posix.py:PrimaryFiles` | Constructor rejects non-POSIX; flock, dir-fd no-follow, link/replace and directory fsync are real dependencies. |
| `_prepared_primary_store.py:PrimaryStore`, `_member`, `_recover`, `reclaim` in the same package | One selected-member transition, root counters and receipt-plus-decision reclaim ordering are shared behavior worth retaining. Physical member layout is currently directory-based. |
| `_prepared_primary_documents.py:PreparedPrimary`, `PrimaryCustody` | The original manifest is now sealed; complete receipts are validated before compact proof creation. This supersedes the older unsealed implementation examined during #981's follow-up. |
| `_prepared_primary_store.py:load_controls`, `accept_custody` | Receipt paths already avoid pending-preimage verification. Windows must preserve this implemented ruling rather than reuse strict `load` everywhere. |
| `_prepared_primary_inventory.py:ordered_primary_inventory` | Direct sqlite3 connection to `files.root / selection.sqlite3`, journaling/sync OFF for disposable sort scratch. A logical-storage port must adapt this seam explicitly; those settings are not durable-store defaults. |
| `_prepared_primary_closing.py` | Inventory framing, durable cursors, finish-ready and terminal custody draining already have their owner. |
| `_prepared_primary_policy.py:PrimaryStatePolicy`; store `PrimaryStoreLimits` | Existing budgets are logical state/preparation/control/scratch amounts; they do not reserve SQL pages or a rollback journal. |
| `producer.py:IncrementalCollectionProducer`; `processing/writer.py` | Maintained producer/writer behavior depends on the mandatory shared store, so a skipped Windows store is an actual consumer gap. |
| `some-implementations/riverhog/applications/a-riverhog-cli/src/a_riverhog_cli/directory_upload.py:prepare_primary_upload` | Initial allocation identity and `.primary` directory placement are coupled; `_state_path` makes nested XDG directories. Windows needs explicit file placement, not a hidden new-parent durability assumption. |
| `some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/landing.py` | Store lifecycle belongs to the claim owner; it must retain its custody/claim-disposal conditions. |
| `some-implementations/stove0/packages/target-support/src/stove0_target_support/execution.py`; `persistent.py` | Target `.primary` placement and directory-oriented retention/deletion must use the selected backend's lifecycle, not guessed physical files. |
| `packages/state-schema/src/state_schema/__init__.py:StateSchema` | Explicit upgrade/current revision contract. `_verify_database` includes whole-database quick/foreign-key checks; distinguish explicit audit from bounded receipt activation. Its convenience `sqlite_engine` forces WAL/30-second busy timeout and must not be reused unchanged. |
| `packages/riverhog-client/pyproject.toml` | Does not currently declare state-schema; database/migration packaging is real integration work. |
| `scripts/ci_qualification.py`; `.github/workflows/ci.yml` | Native selection includes store/document/POSIX tests; existing matrix includes Windows 2025. Shared behavior must execute there, while genuinely POSIX-specific syscall tests stay distinct. |

The issue's report of 158 passes/139 skips, including 135 POSIX-backend skips,
is prior integration evidence, not a new run here. Likewise its Native working-set
samples remain #981/#982 issue evidence, not proof that a durable byte grant caps
process memory. No latest all-green CI or delivery claim is inferred from them.

## Alternatives and rationale (design deductions)

A direct Win32 file port keeps the smallest code footprint but leaves the exact
namespace/flush/deletion proof that triggered this issue. A custom append-only
container would require a new crash-safe index, commit protocol and compactor.
ESENT would supply a Windows transaction engine but add a new native integration
surface not already used by this client. SQLite is already used for disposable
ordering, is accessible through the locked Python environment, and gives one
transaction owner for byte selection and progress. The proposed durable usage
has different settings, schema, ownership and qualification from that scratch use.

The cost is real: schema dependency, indexed chunk overhead, explicit page/journal
admission, and a bounded physical-compaction/disposal lifecycle. The proposal
accepts and declares those costs instead of claiming directory emulation is free.
It does not assert SQLite is universally fastest or that Linux evidence qualifies
Windows. The native witness suite decides conformance of the implemented backend.
