# Sink-gated uploads and extent/growth evidence

Start with the [2026-10-14 issue decision](https://github.com/nashspence/riverhog/issues/949#issuecomment-6172547034), then this guide.
That issue record owns the accepted direction and acceptance. Earlier conversation
was intent calibration. This is non-authoritative reference input under #903, not
a selected wire design, production implementation, or permission to merge this branch.

Audited main: `e0036f80410a83cc5eaa9d63c17650ffae72e2bd`.
Reference revision parent: `705cd8c79e48c190fedece4105f2f6e9345994c7`.
`source_audit_v3.json` records 19 selected source blobs. Some changed since the last
audit: do not reuse the earlier claim that all relevant source is byte-identical.

## Payload rule, without adding a media domain

Riverhog does not own a deferred-delivery payload spool. A destination's known
inability to accept the applicable payload stops upstream consumption/production.
No local file, database, temporary volume, RAM backlog, or retrieval-cache diversion
may let producer uploads continue ahead of an unavailable selected destination.
This includes ciphertext as well as plaintext. Small durable ownership, continuation
and reconciliation records are different from artifact payload bytes.

Transient in-flight receive, encryption, framing and bounded replay-unit memory
remains allowed. Account for simultaneous copies and concurrency before reading
the body, not just after the upload service begins processing it. Stop subsequent
reads/units when a wait becomes known. Preserve source replay responsibility and
existing custody receipts; do not acknowledge a local buffer as stored content.

Admission is not a future availability guarantee. Race handling must distinguish
known deferral, uncertain outcome, and definite failure at the actual effect.
Bytes already in flight may need bounded settlement/reconciliation. Do not claim
zero bytes can reach a socket, or that a timeout stops an executing worker. Resume
from the necessary bounded unit/exact receipt, not from the beginning of an archive.
Waiting affects the upload but must not monopolize shared coordination.

Existing explicitly selected retrieval-cache/copy roles are unchanged. They are not
a producer-upload spool and this work adds no automatic staging fallback. Without a
supported transfer path, apply explicit pending/capacity/unsupported-path semantics;
do not solve physical topology by inventing a Riverhog storage tier. Provider resource
mapping, devices, mounts, operator actions and notifications remain adapter-owned.

## Extent contract, with separate axes and authorities

These variables describe the required accounting dimensions, not new protocol fields.
Integration chooses actual representations/configuration at the existing owners.

| Dimension | Required invariant / owner |
| --- | --- |
| Local deferred payload | Exactly zero bytes of Riverhog-managed backlog for unavailable-destination delivery. |
| Transient payload | Aggregate reservations <= M bytes and C concurrent executions, including application ingress, crypto and copies. Each carrier/unit <= U and receive chunk <= R; bounds must hold before reads. Kernel/server/proxy overhead is separately measured/configured. |
| Caller operational records | Q admitted unresolved work/continuation/debt records, each <= K encoded bytes, including ambiguous effects and terminal-parent cleanup. Explicit admission pressure before external effects. Repeated waits replace bounded state, not append attempts. |
| Adapter ownership/replay | For S admitted scopes, W retained exact ownership identities per scope, P pins per identity: records <= S*W; pins <= S*W*P; scalar and identifier encodings bounded too. Different constructions need equivalent declared bounds. |
| Retained safety evidence | Compact retirement/validity evidence survives exact-record reclamation. No stale prepare/renewal resurrection; no scope churn bypass. Pin/effect settlement, not a timer, permits reclamation. |
| Per-step work | At most B candidates processed/materialized/changed and a finite external-contact/control allowance. Discovery must not scan all historical work merely to return B rows. |
| Notifications/diagnostics | State/retry diagnostics remain bounded. Adapter event/log history needs its own cardinality/byte/lag/recovery policy. No new Riverhog media-event protocol. |
| Necessary archive state | Real archive data and necessary per-object/segment/catalog evidence may grow with logical content. Q, W, U and B are not lifetime archive-size limits. |
| Physical DB storage | Logical row/encoded-byte bounds do not include page slack, indexes, WAL/journal or retained snapshots. Select checkpoint/compaction and storage-overload behavior before claiming disk bounds. |

With bounded record/pin encodings, new operational state is bounded by the admitted
Q, S, W and P capacities, not by outage duration or number of historical polls.
Indefinitely unavailable resources may keep safety debt indefinitely; when capacity
is exhausted, stop admitting NEW debt. Preserve already admitted renewal, status,
release and reconciliation paths. Capacity must not erase uncertainty or revive old
identities. Management progress needs reserved capacity/separate selection where needed.

A finite set of scopes is not an absolute bound unless S itself is admitted/configured.
Likewise, a bounded number of arbitrary strings/counters is not a byte bound. Select
identifier lengths, encoded envelopes, clock/revision/counter domains and non-wrapping
exhaustion behavior. Public extents go in actual schema/config/operation declarations;
private operational internals do not require new public fields. Use existing extent
projection/tests, not a duplicate production complexity registry.

## Work-growth obligations

Let D be unique transferred bytes, D_retry actually reprocessed/retransmitted bytes,
N the units/necessary metadata processed, and T control attempts (including polls,
renewals, reconciliation and duplicate requests). The productive payload/iteration
target is O(D + D_retry + N + T), not a claim that indefinite waiting costs O(D).
Repeated confirmed retries should not reread confirmed bytes. An unavailable gate
costs a bounded control attempt, not O(object size) body buffering/hashing.

Index lookup, ordering and maintenance costs are separate. For a conventional indexed
queue with Q retained rows, a due selection can cost O(log Q + B), and B individual
updates O(B log Q). State these costs rather than calling the entire operation O(1).
Use measured candidate/statement/byte counts to test declarations, not wall-clock
speed as a proxy for algorithmic complexity. Cursors alone do not prove bounded
selection; keyset/index plans and adverse data distributions need evidence.

A minimum per-item dispatch interval Delta bounds repeated checks over an interval H
by Q*(1 + floor(H/Delta)) for the fixed admitted population, with new admission and
external client request load accounted separately. Actual implementations may use
backoff/jitter or events; events are hints, not storage receipts. No polling loop may
sleep inside shared coordination or repeatedly rebuild a growing history. Lease-renewal
and retry intervals must be coherent with configured grant/control allowances.

Ordinary traversal must not repeatedly reprocess the entire prefix for each unit.
Adverse retry/restart tests must account for attempted bytes and unavoidable replay
separately; infinite failures do not imply a finite total work bound. Saturation and
blocked low identities must not silently discard work or indefinitely starve unrelated
eligible work. A conditional fairness claim must state its assumptions.

## What the executable additions establish

`backpressure_model.py` has a lazy source factory, a counted bounded reader, shared
transient reservations, an exact part identity, and a destination fixture.
Known waiting or capacity pressure never constructs/reads the source. Readiness loss
after preflight and inside consumption is exercised. Lost committed replies reconcile
while offline without requesting payload again. Only metadata survives Upload.snapshot.
The fixture reserves two chunks for overlapping old/new chunk references; its counters
are reservations, NOT measurements of Python RSS or real crypto/server overhead.

Its 23 tests include boundary rejection, concurrency/byte saturation, cancellation,
short/invalid input, zero-read waits, mid-unit loss and linear growth at 64/128/256 units.
The source owns fixture bytes; the destination retains counts/digests rather than an
archive. This is neither encryption nor provider durability validation.

`work_budget_model.py` uses standard-library SQLite to demonstrate finite typed
metadata, admission before effects, terminal debt retention, independent management
at saturation, indexed due selection, bounded retirement, and database close/reopen.
Its single configured scope uses a retirement window. This is ONE construction; it
is not an instruction to put SQLite or ordinal windows into Riverhog.

Its 23 tests cover capacity+1, bounded UTF-8/encoded fields, pins, low-ordinal/gap
backpressure, fixed-clock dispatch limits, 1,000-item churn/late replay and indexed
selection with 32/512/2048 retained rows. The query-plan/VM-step test detects the
modeled full-scan regression; SQLite VM counts do not count every B-tree page operation.
A negative witness shows that fixed-size returned pages can still hide quadratic scans.
The capacity model's transactions reserve state atomically in one SQLite owner;
no multi-process/production claim protocol, worker fencing or process-kill test is supplied.

The original 77 semantic/lease tests remain byte-for-byte unchanged. Those original
fixtures contain deliberately unbounded dictionaries/event histories and an O(S*W)
resource scan. They remain semantic examples, NOT production growth specifications.
The new models are supporting witnesses, not proof that the entire feature meets all
accepted extents. Translate these acceptance cases into the maintained implementation.

## Current-source characterization

`audit_body_gate.py` extracts the exact pinned upload function (decorators removed,
annotations postponed; body unchanged) and executes it with counting dependency
doubles. The observed order is authorization, get-unit metadata, read 32 body bytes,
then encounter storage unavailability. This demonstrates a gap in audited main,
not a passing application-read gate and not proof of disk spooling.

Relevant current sources:
- [Upload route](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/riverhog/src/riverhog_api/routers/collections.py#L849-L886): body is read before the service call.
- [Upload service](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/riverhog/src/riverhog_core/services/collection_uploads.py#L1916-L2028): destination writes are invoked after receiving content.
- [Raw upload buffers](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/riverhog/src/riverhog_core/raw_upload.py#L524-L601) and [age preparation](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/riverhog/src/riverhog_core/streaming_age.py#L141-L247): replayable ciphertext and later processing budgets are not an ingress admission gate.
- [Extent principles](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/scripts/extent_contract.py#L15-L85), [developer growth/control guidance](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/AGENTS.md#L57-L90), and [shared control budget](https://github.com/nashspence/riverhog/blob/e0036f80410a83cc5eaa9d63c17650ffae72e2bd/packages/http-api-contracts/src/http_api_contracts/control.py): integrate through these owners.

## Transport research and real validation

Primary sources consulted on 2026-10-14:
- [RFC 9110 section 10.1.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.1.1): a sender need not wait indefinitely for 100 Continue. That header alone cannot implement admission.
- [Uvicorn server behavior](https://uvicorn.dev/server-behavior/): application receive and transport flow control differ; early rejection can avoid application body consumption, not every in-flight byte.
- [NGINX request buffering](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_request_buffering): buffering can consume a body before upstream forwarding; disabled buffering has protocol/configuration qualifications.

Supported deployment tests must verify middleware/server/proxy settings do not spool
bodies behind the application's back. Exercise real HTTP, premature bodies, disconnection,
storage loss, request concurrency, aggregate memory, temp-file/DB/cache writes and
cancellation. A source-spy/model test cannot establish those properties. No specific
proxy configuration, transport endpoint or new error literal is selected here.

## Reproduction and handoff

```sh
python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v
python3 -m compileall -q .reference/949
python3 .reference/949/audit_body_gate.py --repo /path/to/checkout-with-audited-commit
```

`validation_v3.json` records actual results and timing, including the negative source
characterization. Timings are observations, not gates or matching performance targets.
Product package/full-repository/real-I/O validation limitations are in HANDOFF.md.

Only .reference files change. Start implementation on then-current authoritative main,
using the issue decisions and current owning contracts. Preserve the previous handoff
in history. The new exact published SHA in #949 supersedes the previous preferred
reference input, not its provenance or the issue's integration authority.
