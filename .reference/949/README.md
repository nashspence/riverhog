# #949 semantic reference experiment

Non-authoritative input under [#903](https://github.com/nashspence/riverhog/issues/903).
Scope, observations submitted for triage, decisions, and acceptance remain in
[#949](https://github.com/nashspence/riverhog/issues/949). This directory is an
executable experiment and its source map, not a new public contract or integration plan.

Audited source: `main` at `ec544b60dde572ac3bc2f31b199ab5b3727e4548` (2026-10-02).
Only `.reference/` is added. No production modules, dependency declarations,
state baselines, generated contract closure, release refs, or workflows are changed.
There is no removable-media adapter, device access, or Home Assistant deployment here.

## Run

From a checkout of this reference commit, with Python 3.10 or newer:

```sh
python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v
python3 -m py_compile .reference/949/semantic_model.py .reference/949/test_semantic_model.py
```

The experiment has no dependencies beyond the standard library and imports no
Riverhog implementation. It was run with Python 3.13.5: 36 tests passed, including
180 distinct interleavings with a snapshot/reload after every transition.
These are **model tests, not Riverhog integration or conformance results**.

## Reading the experiment

`semantic_model.py` separates durable caller demand, observed resource availability,
active stream pins, write continuation, and cleanup debt. Its methods stand for
atomic committed effects. JSON round trips stand for restart boundaries. They do
not implement a database, filesystem durability, RPC authorization, or real I/O.

`ReadDemand.consumer` is a fixture for an exact caller-persisted consumption identity,
not a user/principal name. It is distinct from the adapter incarnation and exact
object revision. A caller saves that identity and a cleanup obligation **before**
calling prepare. Replay cannot create a second demand; reuse for a different object
is rejected. Release can fence an unseen/delayed prepare. A fresh consumption needs
a fresh identity. The experiment retains terminal fences indefinitely; production
retention and abandoned-demand recovery are deliberately not solved by that choice.

The fixture write token represents an already-established adapter-owned WriteSession.
It is not a proposed caller-issued token or a replacement for the existing exact
WriteStartRequest/segment/completion validation. Counts and digest labels are symbolic;
no actual payload, digest computation, encryption, or completion attestation is tested.

`Deferred` distinguishes a known nonterminal wait from an ambiguous lost response and
from failure. It is not a selected HTTP status, public error literal, or endpoint.
The model deliberately permits waits at segment, completion, and cleanup boundaries,
not only before the first byte. No ETA, deadline, or failure budget is inferred from
an operator wait. Actual admission/backpressure and polling policy remain integration work.

`ConsumerModel` retains cleanup debt independently of success, failure, or cancellation.
A lost release reply cannot discard that debt. Read release prevents new opens but does
not release an existing stream pin. `quiesce` atomically closes modeled admission;
new demand after that point waits for reactivation. It does **not** mean a real drive
has been flushed, unmounted, powered down, or is physically safe to eject.

Resource labels and object-to-resource mappings are private adversarial fixtures.
They must not become Riverhog object metadata, public routing instructions, or
real deployment identity. The event list models transition deduplication only;
it is neither a CloudEvents endpoint nor a transactional outbox implementation.

## Source-to-witness map

All repository links below are pinned to the audited commit, not moving `main`.

| Audited seam | Executable witness / reason for including it |
| --- | --- |
| [Protocol: write session and exact start](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/protocol.py#L219-L261) | Existing durable continuation is useful; `test_waiting_write_is_persisted_before_accepting_bytes` exercises waiting without replacing that identity. |
| [Protocol: preparation and readiness](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/protocol.py#L603-L642) | The current request identifies an object set, not a consumer. Overlap, lost prepare replies, release-before-prepare, and the 180-interleaving test isolate the ownership problem. |
| [HTTP operation/error contract](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/http_binding.py#L393-L630) | The error vocabulary and per-operation acceptance are closed. Readiness-loss and completion-wait tests show why adding only a preflight check is insufficient. |
| [Archive-copy exception path](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/services/archive_copy_jobs.py#L566-L615) and [failure recording](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/services/archive_copy_jobs.py#L1931-L1953) | Exceptions can terminally fail a copy. `test_known_wait_ambiguous_effect_and_failure_are_distinct` captures the semantic distinction needed by callers; it does not modify their current behavior. |
| [Retrieval preparation/polling](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/services/retrieval.py#L1760-L1873) and [verified cache handoff](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/services/retrieval.py#L2035-L2096) | The inspected restored-object path releases cache admission, not an adapter read preparation. The terminal-outcome/lost-release tests model a durable cleanup obligation rather than process-local `finally` alone. |
| [Archive-copy batch readiness and cleanup](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/services/archive_copy_jobs.py#L948-L1032) | One test demonstrates the all-ready barrier with one available resource; another demonstrates one-object consumption and release. This is not a zero-staging source-to-destination copy proof. |
| [Delete and verify](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/riverhog/src/riverhog_core/stores/storage_adapter_archive_store.py#L108-L140) | Offline-delete/abort witnesses intentionally avoid asserting that a local tombstone proves provider reclamation. They do not settle the public deletion semantics. |
| [Incarnation fence](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/client.py#L458-L476) | Tests reject prepare, release, and write continuations against a different incarnation; replacing a registration must not consume old cleanup debt. |

## Using these witnesses in authorized integration

The smallest wire design has not been selected here. A session-bound write readiness
operation and a precisely specified deferral result are alternatives to evaluate;
known waits must remain distinguishable at the actual effect boundary either way.
A retry hint alone cannot define persistence, cancellation, or mutation completion.

Any accepted read-identity design needs end-to-end transport through the protocol,
HTTP schemas/binding/client, archive-store ports/bridge, retrieval progress, archive-copy
progress, and their durable state owners. The existing bridge discards `collection_id`
when translating preparation; adding a field only to the wire model will not create
a stable caller identity. IDs must not grant authorization. Existing incarnation,
object/revision, identity assertion, and completion fences must remain in force.

For aggregate readiness, singleton scheduling is a smaller experiment than adding
an unbounded per-object status map. Actual scheduling still needs fairness and bounded
work. One-slot copying between two removable targets needs ciphertext staging or an
explicit supported topology; the model's successful singleton witness assumes an
available verified ciphertext cache. It does not prove an arbitrarily small cache
can restore arbitrarily large objects or remove existing whole-object cache admission.

For cleanup, integrate a durable release obligation with all applicable paths:
verified cache commit, success, cancellation, failure, expiry, lost replies, and
restart reconciliation. Recheck both retrieval and archive-copy behavior. Abandoned
caller recovery, finite replay-fence retention, read expiry/renewal, and stale
in-flight effects need a selected policy and real concurrency tests before freezing.
Do not transplant the model's forever-retained dictionaries as a production database.

Writes admitted into a local spool must not acquire a completed archive receipt
before the configured durability/placement obligation is satisfied. Media availability,
unknown capacity, or cleanup waits must not be disguised as successful completion.
The symbolic abort/delete examples are conservative test inputs, not permission to
change deletion meaning or broaden #949's accepted scope.

## Operator-event boundary and external research

The [generic lifecycle envelope](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/lifecycle-events/src/lifecycle_events/models.py#L24-L54)
and [relay](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/relay.py#L190-L255)
already support a separately configured event source. The relay advances its page cursor
after delivery, so an interrupted page can redeliver earlier events. A future adapter
can own its own state/outbox and native event endpoint; it should not write into
Riverhog's private database or extend Riverhog's closed event union by convention.
Notification delivery is not evidence that media were attached or flushed. Revalidate
current demand before acting on a stale notification; physical readiness remains adapter-owned.

Primary sources consulted on 2026-10-02 (supporting context, not Riverhog requirements):

- [RFC 9110, 10.2.3 and 15.6.4](https://www.rfc-editor.org/rfc/rfc9110.html#section-15.6.4):
  HTTP defines 503 and optional Retry-After, not Riverhog durable-work semantics.
- [AWS: restoring archived objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/restoring-objects.html):
  Glacier restore can make a temporary readable copy. This helps explain object-level
  restore state, but does not supply independent consumer ownership or media-release semantics.
- [CloudEvents 1.0.2, event identity](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#id):
  duplicate deliveries can retain source and ID. Deduplicate notifications by that pair;
  do not confuse a new event ID with a new storage demand. The model event list is not
  a full CloudEvents serialization or delivery test.
- [Home Assistant webhook data/security](https://www.home-assistant.io/docs/automation/trigger/#webhook-trigger):
  webhook IDs are secrets and local-only access is available. The guide documents
  `application/json`, while the Riverhog relay emits `application/cloudevents+json`.
  Actual deployed HA media-type parsing must be tested; direct compatibility is not
  asserted here. Keep notifications separate from destructive storage authority.

Keep the adapter's catalog/control plane available when payload media are offline;
`head` must not invent absence. The [placement selectors and inert assertions](https://github.com/nashspence/riverhog/blob/ec544b60dde572ac3bc2f31b199ab5b3727e4548/packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/protocol.py#L61-L105)
are not a volume-routing extension point. Normal media rotation should not replace
one logical target's incarnation. Independent recovery must not rely solely on a
lost local catalog: preserve a recoverable inventory and archive materialization path.
These are integration constraints to evaluate, not a supplied storage layout.
