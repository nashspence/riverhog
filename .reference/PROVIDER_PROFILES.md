# Provider settlement profiles and exact operational contract cut

This supplements QUIESCENCE_DESIGN.md; it does not assert external-provider
qualification. Primary sources are linked in RESEARCH.md.

## Evidence types

A minimal current-only operational envelope is:

```
call_id + exact_request_sha256 + target_incarnation + dispatch_owner
    + execution: closed | unknown
    + outcome: no_effect | session | part | completed | cleanup_progress | unknown
    + exact existing result / bounded reconciliation evidence
```

The envelope does not alter existing plan, encrypted object, part, sealed receipt
or archive-root identities. It is private operational evidence. Validate all
identity fields and the original result using existing validators. It is not a
bearer permission. A public `closed=true` provided by an upload caller is invalid;
only the trusted effect-owning implementation can attest its qualified semantics.

A separate observation contains observed sessions/parts/completed object and
continuation, but **never** execution closure by implication. Recovering a result
can preserve custody without authorizing terminal cleanup. Result/report commit
must be retryable without replaying the provider effect.

## Scope of the required change

Propagate operation correlation through the internal archive-object port,
validated StorageAdapterPort and HTTP/ASGI support. Keep synchronous begin/part/
complete implementations, but classify returned outcomes precisely and expose
UNKNOWN on loss/ambiguity. One service-side armed intent covers one fully tracked
adapter invocation; any internal attempt fan-out must be included in that envelope
or use individually recorded child intents. A normal method success is closure only
when the qualified implementation proves all delegated attempts ended.

Select single-attempt mutation dispatch for the present S3 profile rather than
adding a provider-owned transaction service. Configure the locked SDK appropriately
(`total_max_attempts=1` is the documented Botocore setting), inspect intermediate
HTTP adapter transport retries/redirects too, and qualify actual wire attempt
counts. No generic retry middleware may reissue an ARMED mutation after timeout.
Read-only queries can retry within declared resources. Safe exact cleanup retries
are separate from forward payload retries.

Add a read-only bounded open-write observation when recovering lost begin tokens
requires it. It returns exact-key sessions plus explicit pagination, not the first
match silently treated as the sole session. It is not a replacement begin. Preserve
provider configuration/storage incarnation and compare complete original request
identity where the provider can establish it; absence of identity metadata is not
permission to adopt a different session.

Replace terminal use of unbounded prefix deletion with a bounded cleanup-step
contract. A step is scoped to a durably selected terminal request and exact prefix/
incarnation, with a count/byte/provider-call grant, retained exact page intent,
per-item outcomes, continuation and explicit exhaustion. Update validators,
adapter implementations, support clients/routers and conformance tests together;
no legacy unbounded alias is a valid fallback for this terminal path.

This is a scoped operational provider-contract change requiring issue consumption.
It does NOT claim to add an S3 fencing token, server-side idempotency key, privileged
volume operation, new authority over unrelated keys, or another durable database.

## Maintained filesystem adapter

The implementation uses an exclusive process-owned instance lock and per-object
read/write gates, and persists exact write/session state. A normally returned local
method with joined children can close its own call. A dead caller is NOT proof that
a separate adapter HTTP handler has stopped. Process-incarnation proof must refer
to the actual adapter executor, not just the upload API process.

A future recovery proof based on acquiring the filesystem owner's exclusive root
lock must also fence queued old-incarnation requests, identify every effect, and
qualify OS/filesystem pending I/O and original-state recovery. The present design
does not certify that stronger proof from `flock` alone. Without it, a lost reply
is UNKNOWN, even if the private write directory can be inspected. Use the existing
filesystem private-state owner for bounded observation/disposal, not arbitrary
recursive source-directory deletion. No Windows/POSIX portability claim is made.

## Maintained S3 family

The maintained begin searches for one active upload at the key and otherwise calls
CreateMultipartUpload. That is a mutation, not a safe terminal recovery query.
S3 creates a provider upload ID; its documented request has no client-supplied
idempotency field for a whole begin operation. The public API documentation reviewed
here does not supply a whole-prefix, incarnation-fenced execution barrier.

With single-attempt dispatch and a complete successful response, retain the exact
session/part/result before declaring the effect settled. On a lost response, retain
ARMED/UNKNOWN. A negative list/HEAD cannot exclude a request delayed before effect.
A positive result must be matched to original authority and may establish custody,
but is not by itself proof that all uncertain attempts have stopped.

`CompleteMultipartUpload` needs its full response parsed: an initial HTTP success
header can precede a body-level error. Preserve existing conditional create-only
completion and exact object reconciliation. Do not follow provider advice to start
a new multipart upload after a conditional conflict while terminal admission is
closed. No delete/recreate manoeuvre may erase a late seal or change ciphertext.

Abort is destructive cleanup, not a universal cancellation barrier. AWS explicitly
allows in-flight part requests to finish after an abort and recommends checking
ListParts/removing leftovers. Thus first drain all known forward calls; then abort
known incomplete sessions and verify their parts/session removal. A NoSuchUpload
answer must not be confused with proof of no completed object. Unknown completion
still protects against cancellation. Repeated abort may be necessary, but loops
belong in bounded resumable execution, not one coordinator call.

For provider cleanup, explicitly track object versions/delete markers and multipart
sessions; deleting a prefix's objects does not enumerate unfinished multipart
uploads. AWS list calls have page limits and continuation markers; they are not
total-work bounds. Inspect DeleteObjects per-key errors even when its overall HTTP
response succeeds. Under a proven no-writer scope, a bounded first-live-page scan
and deletion avoids an ever-growing completed-prefix rescan. Unsupported version
listing, immutable retention policy or denied deletion remains blocked/unknown;
it is not empty-success. Do not silently extend qualification from AWS general-
purpose buckets to every S3-compatible target or directory bucket.

**Unavoidable unresolved case:** an old CreateMultipartUpload request may still be
in flight while all current observations are empty. This profile reports pending
uncertainty and retains its original descriptor/catalog owner. An operator may
supply actual provider/executor closure evidence through the owning recovery path;
waiting longer or declaring process death is not that evidence. No automatic
finite-time completion is promised for this case.

## Optional retrieval cache and other writers

Mirroring is not one atomic provider effect. Its cache admission can mutate before
archive begin, segment calls fork futures, and completion may finish cache first.
Even its completed-object lookup can perform a cache admission. Each child must
have its existing owner and tracked effect before dispatch. Only genuinely pure
observation may run outside admission; split side-effectful lookup behavior.

Preserve cache-independent archive success, but require a durable handoff of cache
cleanup to the existing abandoning/cache owner before the upload may stop owning
it. Lost cache admission replies cannot disappear with the archive session row.
A direct fake provider that mutates synchronously is only a model profile; it is
not evidence that real multipart or mirrored-provider calls have the same closure.

Finalizer small-object writes and any other writer of the selected archive prefix
must participate or be proven unable to overlap. A dedicated write RPC is not the
only effect boundary. Source path lists, known checkpoint lists and a last-minute
upload-state check are incomplete admission inventories.
