# Focused implementation and evidence

This is a proposed proof plan for #981, not executed acceptance evidence.
The [handoff](HANDOFF.md) is the sole entrypoint. Apply the current issue decisions
and repository qualification rail to the exact integrated source.

## First useful slice

Use `tests.unit.test_incremental_collection_producer._bounded_service_api` with
the actual `ApiClient`, service, database and Native author/validator. Fix one
member's bytes, opaque artifact ID and supplied `ObservationResult`.

Retain the prepared primary under a real application-style durable directory.
Interrupt immediately after successful
`upload_collection_upload_session_provenance_journal` and before
`bind_collection_upload_session_artifact_provenance`. Stop/join the old producer,
close its store, and instantiate new producer/store objects from the same
durable allocation. Append the member again. Keep the original journal bytes
outside the resumed producer only as the test oracle, not as a recovery cache.

Prove all of the following in the same maintained witness:

- Exactly one staged sealed journal and one selected primary; journal octets,
  journal ID and every binding coordinate equal the original preparation.
- The resumed path never invokes a second observer or Native author.
- The member obtains a verified receipt whose `primary` equals the prepared
  binding, with correct member and completion requirement.
- Finish the normal history/corpus/finalization path and verify complete output,
  required history and custody. A table-count assertion alone is insufficient.
- Prepared bytes survive until the receipt is durably retained; cleanup then
  succeeds, and a later changed-input retry still conflicts.

First demonstrate that the current unmodified path creates the second journal
under the same injection, then that the integrated repair eliminates it. Preserve
both evidence sets. The issue already reports a deterministic diagnostic, but it
does not prove the exact causal history of the failed FTP CI attempt.

## Failure and conflict matrix

Each case uses exact expected bytes/bindings and real durable state. In-memory
mocks may locate an injection point; they do not establish restart durability.

| Boundary/case | Required observation |
| --- | --- |
| Before/during candidate write, short write, fsync or manifest publication | No remote primary mutation before the durable barrier. Retry selects one valid record or reports the local failure. |
| Durable preparation, before registration or journal create | Cold restart uses the same record; no new observation/Native identities. |
| Journal create commit with response lost | Same ID, length, digest and role reconcile; one staged record. |
| First/middle/final chunk committed, response lost | Exact chunk/offset replay; no skipped bytes, duplicate corpus entry or changed anchor. |
| Seal committed or validation pending, response lost | Existing seal protocol resumes; no replacement journal. |
| Successful stage, bind not sent | The first useful slice above. |
| Bind committed, response lost | Full accepted-binding equality resolves uncertainty. |
| Materialization decision committed, response lost | Same decision replays and exact acknowledgement is retained; binding success alone does not release custody. |
| Custody exists before decision acceptance; cold read-only resume | Payload safe-release may proceed, but exact decision/preimages stay retained. Replay the decision and durably settle its acknowledgement before completion or compaction. |
| Complete with a final open pack | Close only after all decision acknowledgements, without requiring all early custody receipts; prove no completion/custody deadlock. |
| Crash during local closing/finish-ready barrier | Freeze the exact owner inventory, reject new members, resume the bounded cursor, and include prepared-but-unregistered members before complete. Polls do not restart the closure scan. |
| Payload sealed, history/completion pending | Internal reader may close; source and primary intent remain protected. |
| Custody reply lost or repeated | Read back exact receipt and commit the local receipt before release. |
| Crash before/after receipt fsync and each byte unlink/directory fsync | Reopen either pending complete bytes or a verified compact record with optional leftover bytes; cleanup is repeatable. |
| Finalized before local custody retention; finalization response lost | Existing point GET returns the original retained receipt after upload deletion; exact-primary check and cleanup work without staging, payload reread or export permissions. |
| Crash before/after receipt projection, cursor commit, publication or upload deletion | Atomic copy/cursor and complete coverage preserve every original receipt, byte-for-byte; retries cannot replace one or publish incomplete coverage. |
| Catalog rebuild/reindex/upgrade and collection disposal | Preserve retained operational receipt bytes through rebuilding and interrupted upgrade; dispose only under existing collection-owner authority. Never fabricate an absent receipt. |
| Create-only producer, revoked key, changed principal and scoped deletion reader | Preserve the existing read authorization boundary for both upload and finalized receipt paths; no catalog/provenance permission widening. |
| Two identical writers/processes | One live owner/winning primary; loser is busy or reuses the exact durable winner. |
| Two conflicting writers/processes | Conflict before loser staging; winner remains intact. |
| Timeout/cancel with old worker still alive | No expiry-based takeover or lock-file unlink; unrelated construction still progresses. |
| Same bytes, different opaque artifact IDs | Separate members and prepared primaries, preserving existing semantics. |
| Changed member length/digest, source context, attribution/version, completion requirement | Explicit conflict, no second journal or mutation that replaces accepted identity. |
| Changed supplied observation graph/IDs/catalog or observation mode/declaration | Explicit conflict. Lazy/automatic retry uses the frozen first result without running its factory. |
| Changed output ID, causal reference coordinate/order, hint or omission decision | Explicit conflict before stage. Include absent-versus-empty cases. |
| Accepted primary/receipt has same member but any changed binding coordinate | Reject; retain pending evidence and source. This must exercise `resume_artifact_custody` too. |
| Valid receipt self-hash but wrong collection/member/completion/primary | Reject; a self-hash is not sufficient custody. |
| Missing store/header, missing/corrupt pending bytes, unknown format | Fail closed; no initialization, adoption or reauthoring fallback. |
| Ambiguous initial session-create acknowledgement / old unrecorded orphan | Retain state and report reconciliation boundary; do not claim automatic recovery from this separate ambiguity. |
| Capacity/disk full during preparation or receipt retention | No new stage without durable preparation; no eviction. A failed receipt write retains pending data. Reclamation can make progress independently. |
| Allocation retired / crash during terminal cleanup | Cannot reopen as new; bounded cleanup resumes without losing unresolved custody. |

Use fresh processes for the crash/ownership subset, not only fresh Python objects.
Qualify the actual filesystem and supported platforms' locking/fsync behavior.
Fault injection at a Python call is not evidence of power-loss durability by itself.

## Owner-specific proofs

| Maintained owner/tests | Required extension |
| --- | --- |
| `tests/unit/test_incremental_collection_producer.py`; `packages/riverhog-client/tests/test_canonical_production.py` | Actual-service primary window, exact input/binding/receipt checks, public wrapper forwarding and no-reread custody path. |
| `tests/unit/test_cli_upload.py` | Persisted allocation plus explicit lazy native observer: cold retry reuses the original result, selection changes conflict, original IDs survive. |
| `some-implementations/riverhog/ingress/ftp/tests/test_ftp_spool_custody.py` | Inject into actual shared stage/bind path; retain payload/intent across process death; exact receipts precede final claim deletion. |
| `some-implementations/stove0/packages/target-support/tests/` | Derived output restart, retained completion/history obligations, fresh current capability with unchanged semantic inputs, pending-intent retention and receipt-backed source release. |
| Existing workspace/interface/protected-contract tests | Shared owner boundaries, maintained caller parity, no Native/archive identity changes or weaker corpus rejection. |
| Upload-service/catalog schema and supported database tests | Retained exact receipts, bounded copy/cursor, finalized point readback, current authorization and preservation across rebuild/upgrade. |

## Growth evidence

Vary dimensions independently: retained/completed backlog `N`, pending count,
per-member input/Native sizes, journal chunk count, replay attempts, active workers,
and cleanup position. Include first/middle/late/terminal/empty selections, equal
payloads with distinct IDs, long context/causal data, cold processes and cache eviction.
Use representative small and large backlogs (for example 1/17/1,025/8,193) and the
issue's approved larger-workload cadence; these examples create no new gate.

Instrument actual opened/read/written/deleted files and bytes, readdir/index visits,
hash/Native/observer calls, HTTP requests and bytes, peak live buffers/authoring
memory, and admitted/retained/reclaimed storage. Count rejected/interrupted work.
The mechanism must show:

- No second author call for one prepared member across any replay sequence.
- Point resume/receipt work independent of the number of unrelated completed
  members, except an explicitly declared backend seek cost.
- Linear durable-byte preparation and cumulative replay work in bytes actually
  traversed/attempted; account for the current helper's accepted-prefix reads.
- No whole-construction JSON rewrite or complete pending scan after each append.
- Large pending-byte reclamation after exact receipts and decision acceptance,
  retained compact metadata accounted separately, one server receipt copy per
  member, and complete linear owner-driven terminal cleanup.
- Finalized receipt lookup performs indexed member reads without archive/corpus
  scans, Merkle reconstruction or changed authorization. Closure of decisions
  before completion is one resumable pass, not one complete scan per poll/retry.
- Responsive status/coordination and independent-construction progress during a
  slow publication, full disk, held writer or deferred cleanup.

Do not infer growth from method-call counts or a small timing-only test. Report the
current whole-member Native authoring memory honestly. If streaming/admission or
an owner enumeration change is needed, qualify it with the maintained operation,
not an isolated model that bypasses those costs.

## Delivery qualification

Reconcile the exact integrated commit and #981/#964 decisions first. Run lint and
the affected owner tests, then rerun the failed ingress-custody lifecycle on that
source. Preserve the existing lifecycle deadline and strict corpus check; do not
turn repeated failure into a longer timeout. Exercise actual CLI, FTP and derived
placement, complete outputs/custody, interruption, cold restart and cleanup.

Use the repository's locked toolchain and existing qualification definitions.
Complete the required local/built/backend and exact final-source/tested-merge
GitHub suites under the then-current rail. `make linux-qualification` is the
existing exhaustive local entrypoint when required; there is no new reference
test command to make authoritative. Retain failed attempts as well as final
results. Keep correctness, measured growth conformance and elapsed-time
observations separate. No numerical timing target or waiver is proposed here.

Record a short finite consumption result in #981: exact integrated source,
adopted/reproduced/partially used/superseded/rejected disposition, actual evidence,
and any remaining concrete qualification. Reference publication itself does not
complete #981, cure the failed CI run, or authorize release/provider operations.
