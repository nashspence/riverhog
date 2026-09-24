# Validation record

Reference suite at handoff: **81 passed, 1 skipped**. The skipped test is
`test_postgresql_concurrent_handoff`, which requires a disposable PostgreSQL
server and driver unavailable in this environment. Test evidence in the tar
contains the final pytest output and JUnit XML.

| Concern | Executed coverage |
| --- | --- |
| Acceptance | Durable before payload; no early jobs; rollback of all destinations; input bounds; unknown/self destination rejection |
| Authorization | Both creation and archive-management permission; missing/unattributed/mismatched key; expiry/revocation at acceptance and handoff; grant removal; key deletion; app change; current tag scope |
| Idempotency | Sorted/deduplicated destinations; changed source/cache/destinations/body/tags/context; changed key; omission versus explicit empty list; changed server defaults |
| Handoff | Job + requested event + receipt atomicity; partial multi-destination restart; caller attribution; existing ordinary job observation without resubmission |
| Cancellation | Pre-publication durable discard; repeated cancellation; no canceled-upload resume; ordinary post-publication cancellation boundary |
| Failure | Config removal/remap; safe terminal failure; savepoint rollback after job insertion; transient event-stage failure/backoff; unrelated gateway receipt; missing observed job |
| Job observation | All eight ordinary lifecycle states remain unchanged on duplicate finalization; no duplicate events; operator observation after originating-key revocation |
| Real process crashes | SIGKILL before/after acceptance; before/after publication; after job insert; after event insert; before handoff commit; after handoff commit/lost acknowledgement |
| Concurrency | Four independent Python subprocess sweepers over the same file-backed SQLite database, producing one job and requested event |
| Static | Python syntax compilation for all reference Python files; PostgreSQL DDL compilation of proposed tables and constraints |

## What these tests do not establish

The harness uses SQLite with foreign keys, synchronous FULL, and explicit BEGIN
IMMEDIATE writer serialization. This exercises actual transaction rollback,
commit persistence, subprocess death, reopen/retry behavior, and state-machine
semantics. It does not execute Riverhog's production tables, token authentication,
HTTP dependencies, ordinary copy executor, provider adapters, or PostgreSQL locking.
The supplied Riverhog bridge was syntax-checked and mapped against source, not
imported/executed in a full installed server. The tests are not mocked commit
calls, but they are still tests of a reference prototype.

No power-loss/disk-corruption, multi-host/HA, archive-byte transfer, retrieval-cache,
provider segmentation, migration upgrade, client/CLI, release artifact, or generated
contract qualification was performed. No repository-wide lint/unit/dist-smoke/build
or GitHub Actions result is claimed. No remote branch, issue relationship, issue
comment, or pull request was created.

Test dependencies actually used: Python 3.13.5, SQLAlchemy 2.0.50, pytest 9.0.2,
SQLite 3.46.1. Network cloning and installation of a PostgreSQL runtime failed;
these are recorded limitations rather than inferred successful checks.
