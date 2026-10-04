# #949 integration-agent handoff: generic asynchronous storage

**Start with the [2026-10-04 issue decision record](https://github.com/nashspence/riverhog/issues/949#issuecomment-5981858530).**
That record consolidates the maintainer-authorized direction and owns acceptance.
Earlier conversation was intent calibration, not blanket authorization. This directory
is non-authoritative, executable reference input under [#903](https://github.com/nashspence/riverhog/issues/903),
not a selected transport design or an implementation to merge mechanically.

## Boundary and scope

Riverhog coordinates **opaque objects and external effects**, not removable media.
Generic concepts are nonterminal waiting, outcome reconciliation, exact consumption
ownership, finite renewal/release, incremental object progress, and truthful completion.
No media inventory, mount/device identity, attach/eject commands, operator state, HA
configuration, or topology graph belongs in Riverhog's protocol or orchestration.

Adapters own resource mapping, actual availability, worker/I/O safety, provider effects,
and any operator interaction. A separate adapter-owned event endpoint can use the
existing generic relay. A delivered notification is neither a readiness observation
nor an effect receipt. No direct writes into Riverhog's private database are needed.
This revision implements no adapter, public endpoint, schema, database, or device access.

## What changed in this reference

The initial `semantic_model.py` and `test_semantic_model.py` are byte-for-byte retained.
They witness waiting at effect boundaries, outcome ambiguity, independent consumption,
cleanup debt, and incremental read progression. Their forever-retained dictionaries are
**not** a production retention policy.

`lease_model.py` and `test_lease_model.py` add a separate constructive experiment:
finite renewable ownership, idempotent renewal, retirement without resurrection,
bounded replay records, active-stream protection, and post-expiry cleanup. Its
ordinal/window algorithm is **one possible witness**, not an issue decision that
Riverhog must use ordinals, these Python classes, or these endpoint names.

`source_audit.json` records 15 source/test Git blob comparisons between the mapped
predecessor and audited main. They match. This demonstrates source continuity, not
runtime compatibility or passing product tests. Current dependencies, release tooling,
and generated-artifact rules still need reconciliation by the integration agent.

## Reproduce the evidence

The model suite needs only the Python standard library (Python 3.10+ syntax):

```sh
python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v
python3 -m py_compile .reference/949/semantic_model.py \
  .reference/949/test_semantic_model.py .reference/949/lease_model.py \
  .reference/949/test_lease_model.py
```

Executed on Python 3.13.5: **77 tests passed** (36 initial, 41 additional).
They include the initial 180 replay/release schedules, 120 renewal/release/expiry/
collection/open orders and 24 prepare/release/renewal/collection orders: **324
specified serial orderings** with snapshot/reload boundaries. A separate churn
witness retires 1,000 consumers and rejects all 1,000 late preparations while retaining
at most one exact record in that sequential workload. Counts are not a concurrency,
crash-consistency, performance, or exhaustive state-space proof.

## The finite-ownership experiment

### Grant validity is not resource readiness

A `Consumption` binds the adapter incarnation, already authenticated caller namespace,
never-reused ordinal, and exact object/revision. The fixture's ordinal allocator is
assumed durable; the single-claim caller model is not that allocator. `prepare` returns
a finite grant even while the underlying resource is unavailable. Exact preparation
replay returns current grant state without extending the expiry. A caller can renew
while legitimately waiting, so a long operator delay is not a job-failure deadline.

`renew` conditionally advances a grant revision to an exact absolute deadline, bounded
relative to the adapter's authoritative clock. Replaying the last accepted renewal
returns its existing grant, not another `now + duration` extension. Competing/stale
renewals require reconciliation. Only the last renewal tuple is retained, not every
renewal ever issued. Older exact responses may be unavailable; current authoritative
state is still queryable. Expired/released ownership cannot be renewed back into life.
A still-live job may reacquire with a fresh identity, retaining any old cleanup debt.

Caller, adapter and transport failures are different. A lost response does not prove
no effect occurred. The caller snapshots renewal intent before dispatch and retains it
across a lost reply. The reference does not prescribe a new persistent public
`ambiguous` state; an integration may reconcile using existing exact identities and
receipts. Do not copy the original model's exception classifier as a production retry
policy for all unrelated provider errors.

### Bounded replay evidence without a resurrection hole

The witness keeps a persistent `retired_through` watermark per configured caller scope.
All old ordinals at or below it remain inadmissible, even after the exact records have
been removed and arbitrarily delayed requests arrive. Above it, only a fixed window
of ordinals can have records. Collection advances only across a contiguous prefix of
settled records; it never skips an unobserved preparation, active lease, unresolved
stream pin, or required cleanup effect.

This addresses a specific failure of timer-only tombstone deletion: after forgetting
an old release, an unchanged delayed preparation could look like a new request. The
suite contains that negative counterexample, alongside the watermark rejection witness.
A different bounded construction is acceptable if its remaining validity/fencing rule
also prevents resurrection after reclamation. A retention duration without that rule
is not sufficient evidence.

For `S` configured scopes, window `W`, and pin capacity `P`, exact record cardinality
is at most `S * W`, pin cardinality at most `S * W * P`, plus one retirement watermark
per scope. One collection call retires at most `min(limit, W)` records. These are
cardinality bounds, not claims of constant byte-size for every symbolic clock/counter.
The ordinal domain has explicit exhaustion checks; actual wire encodings and all
other scalar bounds must be selected during integration.

A compact `retired` result proves that ownership in the retired range cannot have
remaining admitted work. It does **not** recreate an exact historical grant, assert
that preparation once happened, or attest object deletion. An `unknown` result above
the watermark proves none of those things; an explicit release fence is still needed
for a potentially delayed preparation.

### Expiry, release and streams are separate facts

Expiry/release prevents new stream admission and renewal under that ownership. It does
not close already admitted streams, erase pending cleanup effects, or authorize a
physical eject. Pins survive snapshot/reload. `confirm_stream_closed` represents
adapter-side evidence that the exact stream/worker can no longer do I/O; the model
**assumes** that evidence. It implements neither real worker fencing nor a timeout
that magically establishes it. Delayed opens after closure are rejected; a delayed
close of an old stream cannot release a newer pin.

A cleanup effect attached to an expired/released preparation remains unsettled until
its modeled provider effect is complete. Release may durably close admission yet still
return a nonterminal wait. The caller retains cleanup debt after every terminal job
outcome and even after a lost release reply. Reclamation cannot remove pinned or
unsettled records. A restart does not infer resource availability or worker cessation.

The model's clock cannot regress below the persisted observed clock; regression fails
closed. Large forward jumps can end grant admission but cannot remove stream pins.
This does not solve real clock skew, failover, rollback, simultaneous adapter processes,
or paused workers. Their authority and fencing must be specified and tested in the
real implementation. Read-ownership expiry is not retrieval-delivery expiry, a provider
restore-window expiry, or a new expiry rule for existing durable write sessions.

### Costs and assumptions the model deliberately exposes

- A long-lived low ordinal or an allocation gap can prevent prefix retirement and
  cause window backpressure. This trades liveness/throughput for a strict record bound;
  it is not a recommended global serialization policy. Production needs fair scheduling
  and an explicit way to settle abandoned/gap allocations without erasing uncertainty.
- Scopes are finite, configured and authenticated outside this model. Never let an
  untrusted request choose another caller's scope. Automatic unbounded scope creation,
  key-rotation-as-new-identity, allocator rollback and ordinal reuse defeat the premise.
- Expired pinned work stays pinned until real cessation/fencing is established. Capacity
  may remain exhausted rather than silently discarding that safety evidence. A real
  recovery path is required; finite leases alone do not solve orphaned physical I/O.
- A monotone compact watermark is retained as durable safety evidence. Exact per-request
  tombstones are reclaimable, but all anti-replay information cannot simply be deleted.
  Disaster recovery or namespace replacement must not restore an older admissibility
  domain while stale messages/workers remain possible.
- State transitions and JSON snapshots are serial atomic assumptions. No real transactions,
  HTTP framing, cancellation propagation, encryption, hashing, outbox delivery, OS flush,
  provider deletion or transport authentication is exercised here.

## Mapping to integration work

The issue owns the acceptance checklist. This is a source-oriented work map, not a
new scope declaration:

| Integration seam | Reference evidence and remaining real proof |
| --- | --- |
| Protocol and HTTP boundary | Retain strict response/error validation. Choose exact known-wait semantics and reconciliation at begin/segment/complete, small-object writes and applicable cleanup/abort/delete effects. Existing preflight is not sufficient; do not blindly convert every error into an endless retry. |
| Read identity through ports and callers | Carry one durable identity through preparation, status, renewal, stream admission and release. The current bridge discards collection context; adding only a wire field cannot create caller ownership. Preserve incarnation, exact object/revision, and independent authorization. |
| Durable state and terminal paths | Allocate identity and cleanup debt before dispatch. Include cache-hit shortcuts, failed preparation replies, job cancellation/failure/expiry, verified cache handoff, startup reconciliation and catalog retirement. A cleanup worker must remain able to progress after the parent is no longer scheduled. |
| Lease and replay safety | Translate the new witnesses into real database/HTTP/concurrent-worker tests. Resolve clock authority, namespace/allocator recovery, stale renewals, bounded storage, admission pressure, pin fencing, and expired-generation reacquisition without requiring this particular watermark algorithm. |
| Retrieval and archive-copy scheduling | Consume/release bounded object windows incrementally. The initial one-resource fixture assumes a verified ciphertext cache; it does not prove arbitrary object sizes fit any cache, or zero-staging transfer through incompatible topology. Maintain capacity accounting and useful concurrency. |
| Completion and cleanup | Distinguish admission closure, outstanding cleanup and complete provider-level effects. Preserve deletion/abort postconditions and archive durability/placement obligations. No local queue/tombstone alone completes an external effect; conversely this does not promise forensic secure erasure. |
| Supplied adapters and release evidence | Show that immediate and restore-required adapters still satisfy the selected generic contract. Reconcile the then-current baseline/migrations/generated contracts and run required product validation on the integration SHA. No production compatibility shim or additional product dependency is supplied here. |

## Research consulted for this revision

Primary sources retrieved 2026-10-04; these motivate distinctions, not the exact model:

- [RFC 9110, section 9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2):
  retry safety depends on effect semantics when a response is not received. An HTTP
  status alone is not durable workflow or reconciliation policy.
- [AWS Builders' Library: idempotent APIs](https://aws.amazon.com/builders-library/making-retries-safe-with-idempotent-APIs/):
  caller request identities, changed intent and late-arriving requests matter. The
  watermark construction here is our independent model, not an AWS-prescribed API.
- [etcd v3.6 Lease API](https://etcd.io/docs/v3.6/learning/api/#lease-api): finite
  server-granted leases and keepalives illustrate ownership expiry. Expiring leased
  metadata does not by itself establish that unrelated physical I/O has stopped.

The initial guide retains primary sources on cloud restores and generic event/webhook
handling. No etcd dependency, cloud account, HA deployment or physical-media requirement
is introduced by consulting these sources.

## Validation limits and publication lineage

Local model tests and Python compilation passed. An additional attempt to collect the
existing storage-protocol/support package tests stopped with missing `rfc8785`; an
isolated install of locked `rfc8785==0.1.4` failed because pypi.org could not resolve.
No product test pass, full repository lint/unit/dist-smoke/build, or real concurrent
recovery/conformance result is claimed. The Git clone gateway supplied a verified full
checkout; this is no longer an API-only source review.

The revision is appended to mapped predecessor
`224b3239c8c82be0dbb6730675ca6c09f23e782f`, without force-pushing or rebasing away its
history. Its underlying mapped source base is
`6f4099700834e411d7b7b342c43c077fcc83e55e` (also recorded in `source_audit.json`). Current main re-audit:
`29d16e99d52bda0f308495fd7be519c1e321b735`. The branch intentionally is not an integration
checkout of current main. Start implementation from then-current authoritative main.

The new exact published reference SHA and any observed CI status are recorded in #949.
The prior original `78415d14ae5e5556a4834267cf93053bb6af66f3` provenance is preserved by
#950's authorized mapping/recovery record. This revision supersedes the earlier reference
as the preferred input, not the historical provenance or issue authority.
