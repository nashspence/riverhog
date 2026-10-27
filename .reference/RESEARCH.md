# Research and source audit

All conclusions below are scoped to the handoff, not a new repository contract.
Documentation was consulted on 2026-10-27. The agent's measurements remain
[issue-reported evidence](https://github.com/nashspence/riverhog/issues/982#issuecomment-6455046191).
No Windows run is claimed for this reference.

## Primary external sources

| Source | What it establishes; what it does not |
| --- | --- |
| [SQLite cache_size](https://www.sqlite.org/pragma.html#pragma_cache_size) and [cache_spill](https://www.sqlite.org/pragma.html#pragma_cache_spill) | Cache size is suggested; spill controls dirty pages written mid-transaction. Neither gives a hard private cache allocation. |
| [journal_size_limit](https://www.sqlite.org/pragma.html#pragma_journal_size_limit) | Retention after transaction/reset, not a peak-journal admission mechanism. PERSIST still needs a separate write-extent gate. |
| [hard_heap_limit64](https://www.sqlite.org/c3ref/hard_heap_limit64.html) | Aggregate library/process heap ceiling with allocation failures; documented exceptions include disabled accounting, alternative cache and supplied cache pool. It is not per connection and does not cap Python or RSS. |
| [OS file methods](https://www.sqlite.org/c3ref/io_methods.html) and [file-control opcodes](https://www.sqlite.org/c3ref/c_fcntl_begin_atomic_write.html) | Public interception/delegation boundary, including xTruncate and allocating hints. Policing xWrite alone is insufficient. |
| [Incremental BLOB writing](https://www.sqlite.org/c3ref/blob_write.html) | Fixed extent, no length increase. Together with exact schema and source, supports the narrow control-write derivation, not structural SQL. |
| [SQLite file format](https://www.sqlite.org/fileformat2.html) | B-tree/index/freelist/overflow/pointer-map page roles; overflow payload is usable-size minus four; rollback page records include page number, page and checksum. |
| [Atomic commit](https://www.sqlite.org/atomiccommit.html) | Journal-before-database ordering, sector peers, cache spill, persistent journals and recovery dependencies. Hardware/flush assumptions still apply; a VFS wrapper must preserve them. |
| [Transaction errors](https://www.sqlite.org/lang_transaction.html) | FULL/IOERR/NOMEM may cause statement or transaction rollback. Inspect/reconcile state; do not infer success/failure solely from the returned exception. |
| [Application-defined cache](https://www.sqlite.org/c3ref/pcache_methods2.html) | Global cache replacement has initialization and allocation semantics. This design deliberately does not install one or replace the locked engine. |
| [SQLite implementation limits](https://www.sqlite.org/limits.html) | Engine/connection limits are separate from application admission; chunking must not turn the BLOB limit into a total journal/collection ceiling. |
| [CPython 3.12.3 build properties](https://raw.githubusercontent.com/python/cpython/v3.12.3/PCbuild/python.props) and [SQLite build](https://raw.githubusercontent.com/python/cpython/v3.12.3/PCbuild/sqlite3.vcxproj) | Upstream Windows build names SQLite 3.45.1.0 and exports its API. This is provenance context, not attestation of the actual qualification DLL: record and compare the loaded source ID/hash. |
| [SQLite 3.45.1 pager](https://raw.githubusercontent.com/sqlite/sqlite/version-3.45.1/src/pager.c) and [B-tree](https://raw.githubusercontent.com/sqlite/sqlite/version-3.45.1/src/btree.c) | `sqlite3BtreePutData`/`accessPayload` preserve the fixed record; pager invariants, sector journaling and header/record handling motivate the formula. Reconcile those functions with the exact loaded engine before qualifying the control profile. |

### Design deductions, not external promises

The Qio prefix invariant comes from checking all attempted delegations before the
native call; it is not a SQLite built-in feature. The fixed-control formula is our
conservative derivation under the stated profile. The resident-page inequality
uses the explicitly required heap-backed default cache. No theorem is claimed
that a 9-, 20-, or 68-page observed transaction bounds every future B-tree shape.

We selected a resource VFS gate rather than: guessing a larger number; pretending
cache_size/journal_size_limit enforce peaks; changing to WAL; allocating a second
engine/database; installing a global custom cache; or rebuilding whole containers
per member. It introduces one explicit native enforcement boundary. Its cost and
qualification are part of the handoff, not hidden integration discretion.

A universal structural *success* guarantee under an arbitrarily small configured
quota is deliberately not selected. Budget rejection is observable and preserves
pending obligations; fixed controls have separately backed progress extents. This
is the proposed change to the earlier resource/liveness statement that needs the
owning issue's adoption. No private producer authority is transferred by publication.

## Exact Riverhog source map

Paths below refer to `f41bae6e1456d4ae16952de293320cbe4a6ce460`.

| Path / symbols | Audit finding and integration role |
| --- | --- |
| `packages/riverhog-client/src/riverhog_client/_prepared_primary_sqlite.py`: `SqlitePrimaryLimits`, `_connect`, `_engine_factory`, `_transaction`, `sync`, `_append`, `_select`, `recover_step`, `remove_step` | D ceiling/retention/cache tuning exist, but no enforcing Q gate. Creation/factory opens and auto rollback are within scope. `sync` currently changes an integer's variable-size encoding. |
| Same directory, `_prepared_primary_sqlite_controls.py`: `prepare`, `put`, `remove` | Preallocated 9-byte-header BLOBs distinguish pending/usable slots. Incremental exact updates are the nonstructural progress profile. |
| `_prepared_primary_sqlite_journal.py`: `_warm`, `prepare`, `ensure`, `require` | Successful actual warming and allocated-file checks exist. The observed floor must not be relabeled a proof of other shapes. |
| `_prepared_primary_sqlite_schema.py` and `_prepared_primary_migrations/` | Current table/index/FK structure and explicit StateSchema owner. Resource metadata/barrier change belongs here, not server schema. |
| `_prepared_primary_sqlite_ordering.py`: `ordered_rows`, `cleanup_step` | Indexed derived rows, retained grant and one-row cleanup. Index changes and string-valued cursors are structural, not fixed control writes. |
| `_prepared_primary_sqlite_space.py`: `space_usage`, `vacuum_step` | Point page/free/allocated observations. Vacuum N=1 does not imply one page written; it can move pages/update parents/pointer maps. |
| `_prepared_primary_windows_native.py` | Native owner/identity/ACL and physical file observations remain authoritative for local container admission. |
| `_prepared_primary_store.py`, `_prepared_primary_documents.py` | Original sealed manifest, exact custody/decision and original-length accounting stay unchanged in meaning. |
| `tests/support/sqlite_vfs_faults.py`: `FaultVfs`, `TransactionWrites` | Same-loaded-DLL test VFS and successful-delegation measurement. Reusable experimental seam, not yet pre-delegation enforcement or production native code. |
| `packages/riverhog-client/tests/test_prepared_primary_vfs.py`, `test_prepared_primary_sqlite.py`, `test_prepared_primary_sqlite_space.py`, `test_prepared_primary_windows.py` | Existing ownership/platform/SQL resource test locations; retain and extend actual native qualification. |

Newer inspected source `93a3208056a98887682db0f5c16f6e868d5dc141` differs only by
renaming `primary_state_schema`/`verify_shape` with leading underscores and their
imports/calls. It contains no new resource algorithm. The reference's parent is
the request's exact f41 checkpoint; no current-main or newer-source test claim.
