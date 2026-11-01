# Research and exact-source audit

## Owning evidence

The [request](https://github.com/nashspence/riverhog/issues/964#issuecomment-6551784418)
and [planner observation](https://github.com/nashspence/riverhog/issues/964#issuecomment-6551438560)
identify one artifact emitting 1/16/17/129/1,000 raw volumes. They report one
registration transaction and one returned summary per emitted volume; the largest
SQLite metadata-only registration took 0.197 s. Those are issue-reported results,
not rerun here and not payload, encryption, HTTP, custody or 1,000-member evidence.

The subsequent [qualified-source checkpoint](https://github.com/nashspence/riverhog/issues/964#issuecomment-6552163450)
records completion of the preceding efb9a615 source's required checks while
keeping this next implementation paused. That evidence is not validation of the
new reference. The original P15 packet and G10/G11 family entries were inspected
at `e35bddf8f1bb72ca3f0046b597d5966cd5a08312`. They require exact semantics and
bounded requests/transactions, not simply a smaller returned iterator.

## Source anchors

All links below pin `efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055`.

| Maintained source | What owns the relevant behavior |
| --- | --- |
| [incremental_plan.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/incremental_plan.py) | `advance_incremental_volume_plan` retains all raw plans for one member; checkpoint has only completed-member progress and bounded pending pack. |
| [raw_volume.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/raw_volume.py) | `plan_raw_volumes`, RawVolumePlan construction and `raw_volume_plan_bytes` determine exact segment ID, offset, length and canonical plan. |
| [pack_volume.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/pack_volume.py) | `plan_pack_volume` and `pack_volume_plan_bytes` own pack ordering, unit boundaries, index and plan hashes; do not duplicate them in a new planner. |
| [collection_plan.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/collection_plan.py) | CollectionVolumePolicy pins count, raw/part alignment and per-volume construction limits. |
| [collection_uploads.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/services/collection_uploads.py) | `register_artifacts` updates members, counters, full emission, coverage and checkpoint in one transaction; `_persist_plan_batch` builds rows and `batch.volumes` builds the response. |
| Same service | `get_artifact`, `list_artifacts`, `iter_artifacts`, `list_volumes`, `acquire_work`, raw-digest append and direct upload paths require publication predicates, not only API filtering. |
| Same service | `complete`, `_seal_open_collection_upload`, `_record_payload_custody_progress`, `_ready_for_finalization`, cancellation/orphan methods own closure, payload verification and disposition. |
| [catalog_models.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/catalog_models.py) | Existing upload/member/object/coverage identities, indexes, counters and ORM relationships; add operational generation/frontier state here or its owning model module. |
| [catalog_db.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_core/catalog_db.py) | Current application StateSchema, SQLAlchemy sessions and SQLite WAL configuration. The reference model is not this connection setup. |
| [app.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_api/app.py) | `_process_archive_maintenance` and `_run_archive_upload_reaper` provide existing execution scheduling; add bounded work, not another scheduler. |
| [router](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_api/routers/collections.py), [schemas](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/riverhog/src/riverhog_api/schemas/collections.py) | Current artifacts POST calls registration synchronously; output has `state=open` and unbounded volumes. Current-only response change must be explicit. |
| [client.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/packages/riverhog-client/src/riverhog_client/client.py), [producer.py](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/packages/riverhog-client/src/riverhog_client/producer.py) | The client expects immediate open registration. Producer then submits raw digests/replays primary and uses nonempty inline volumes to request work. Preserve original prepared authority while adapting this handshake. |
| [registration tests](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/tests/unit/test_collection_registration_work.py), [planner tests](https://github.com/nashspence/riverhog/blob/efb9a6155b61cf498dbf56fdd6eaec2e3ffb8055/tests/unit/test_incremental_plan.py) | Exact plan/checkpoint parity, per-request prior-plan loading, coverage rollback and pending-pack closure. Existing retry response intentionally has no new inline volumes; the proposed pinned outcome replaces that transport convention explicitly. |

## External primary-source checks

1. [RFC 9110, 15.3.3](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.3.3)
   makes 202 an incomplete processing outcome and recommends status-monitor
   information. **Design deduction:** use a discriminated pending operation,
   never convert 202 into the existing successful-registration promise.
2. [SQLite transaction control](https://www.sqlite.org/lang_transaction.html)
   and [isolation](https://www.sqlite.org/isolation.html) document one writer,
   snapshot behavior and BEGIN IMMEDIATE/BUSY handling. **Design deduction:**
   serializing each bounded cursor/row commit is sufficient for the model;
   PostgreSQL-style FOR UPDATE alone is not a portable SQLite owner fence.
3. [PostgreSQL transaction isolation](https://www.postgresql.org/docs/17/transaction-iso.html)
   documents statement snapshots at READ COMMITTED and that sequence increments
   do not roll back. **Design deduction:** pin accepted frontiers consistently;
   volume sequences come from retained canonical planner state, not database
   sequence allocation that would change IDs after rollback.
4. [PostgreSQL explicit locking](https://www.postgresql.org/docs/current/explicit-locking.html)
   documents row-lock exclusion and transaction lifetime. **Design deduction:**
   hold upload/operation locks only for bounded transitions; add cursor/epoch
   predicates, not locks spanning network response or the whole large artifact.

None of these sources by itself proves Riverhog semantics. The staged visibility
marker, verification pass, response cut and client persistence are the proposed
owner-local design. They require explicit consumption and maintained-source tests.
