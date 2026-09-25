# Storage-incarnation reference for #908

**Non-authoritative experiment.** Read [../HANDOFF.md](../HANDOFF.md) first.
The [accepted #908 decision](https://github.com/nashspence/riverhog/issues/908#issuecomment-5826495012)
controls. The mechanisms below are proposals, not additional accepted scope.
No production module, schema baseline, generated contract, or configuration is changed.

## What this branch demonstrates

`model.py` contains a real Linux filesystem identity witness and a small SQLite-backed
registration/resolution service. `test_model.py` exercises them with real temporary
roots, durable catalog files, independent processes, and injected rebinding races.
SQLite is an executable model of relational invariants, **not a proposed second Riverhog
database**. No product or adapter may import this reference implementation.

The critical test keeps the configured name and URL identical while changing backend A
to backend B. The existing tuple hash remains identical, but verification fails, neither
historical ownership nor pending work changes, and no object read reaches B successfully.
An explicit administrative replacement can give the name to B for new work; it still
cannot give B ownership of A's copies or work. A late A receipt cannot acquire B's identity.

Run from the repository root on Linux with Python 3.13:

```sh
python -W error::ResourceWarning -m unittest discover \
  -s .reference/storage-incarnation -p 'test_*.py' -v
python -m py_compile .reference/storage-incarnation/model.py \
  .reference/storage-incarnation/test_model.py
```

## Three separate identities

A configured **name** is an operator-facing alias. An adapter **URL** is a route, not
ownership. Software implementation IDs, versions, credentials, TLS certificates, bucket
names, filesystem paths, and hashes of those values are not durable storage-incarnation
proofs. Changing software, credentials, route, or name should not create a new owner when
the same storage authority remains attached.

This experiment uses a random, canonical UUID4 persisted **inside the storage authority**
as the identity of one logical storage incarnation. Provisioning generates it; ordinary
configuration cannot supply a UUID to stamp onto a different backend. Riverhog records the
observed identity only through explicit enrollment and thereafter compares it to the
expected owner. The candidate production descriptor field is `storage_incarnation_id`;
the exact wire representation and placement of operation preconditions remain integration
choices. The model calls its discovery operation `describe`, not the existing descriptor API.

Identity proof here means an authenticated, trusted adapter attesting to durable metadata
in the same authority on which it performs the requested operation. It is **not** a hardware
fingerprint, remote attestation, or cryptographic proof that a malicious operator cannot
copy a marker. An independent clone carrying the same marker is indistinguishable by this
mechanism alone. Independently owned copies need fresh identities; a controlled restoration
may preserve identity only when it preserves the same logical authority, not when it creates
another independently countable archive copy. Such restoration is not implemented here.

TLS/authentication establishes who supplies the evidence; the backend witness establishes
which incarnation it serves; existing archive receipt, revision, digest, and encryption
checks establish object integrity. None replaces the other two. The private marker is
non-secret metadata; credentials and deployment details are not included in these fixtures.

## Filesystem witness: implemented behavior and assumptions

`provision(root)` is an explicit administrative operation, never called by discovery,
read, runtime startup, or automatic recovery. It requires an existing root whose intended
mount and exclusive administrative ownership have already been established. An empty
mountpoint is not, by itself, evidence that the intended volume is mounted.

Provisioners serialize with `flock`. A new marker is written to an exclusive temporary
file, flushed and fsynced, then published with a no-replace hard link and directory fsync.
The temporary entry is removed and the directory synced again. Reprovisioning preserves
valid marker bytes and re-syncs the directory. A competing provisioner adopts only the
already published valid identity. An fsync failure does not report success; crash debris
is refused rather than interpreted as permission to invent a new owner. This relies on a
Linux local filesystem honoring the relevant lock/link/fsync semantics; network filesystems
and actual power-loss behavior have not been qualified. Python documents these primitives
in its [OS interface](https://docs.python.org/3/library/os.html#os.fsync) and
[link interface](https://docs.python.org/3/library/os.html#os.link).

Missing, truncated, malformed, noncanonical, unknown-format, symlink, or nonregular markers
are not repaired during discovery. An unmarked nonempty root cannot be provisioned.
Normal application operations must never replace the marker. Its private namespace must
be outside object listing, prefix deletion, retention/lifecycle rules, and cache eviction.
The reference reader rejects the marker and uses only safe flat fixture keys; it does not
implement the production object namespace or its deletion paths.

Discovery reads the marker afresh. Every reference read takes an expected incarnation,
opens the root once, checks the marker, and opens the object relative to **that same root
file descriptor**. Renaming/replacing the root path after the check therefore cannot route
the operation to the replacement. A stable endpoint switching backends between discovery
and the read is also rejected at the backend operation boundary. The marker and root
contents must remain under a trusted adapter/administrator; hostile marker rewrites or
arbitrary concurrent in-place replacement of the root's contents are outside this proof.

## Catalog ownership and rebinding: implemented behavior

The model separates immutable `incarnations` from mutable `bindings`. `archive_copies.owner`
and `pending_work.owner` are non-null foreign keys to incarnations, never to names or URLs.
Database triggers reject ownership rewrites. An operator alias may change without updating
any historical owner. Old incarnation rows remain after removal or explicit name replacement.

Enrollment requires an expected identity and a live adapter observation that matches it.
First enrollment is an explicit trust decision, not a side effect of finding a configured
name. A production administrative interface must authenticate/authorize this decision;
this model has no HTTP/admin authorization layer. Normal restart should obtain the expected
identity from durable registration, not adopt whatever a newly routed endpoint advertises.

Rebinding clears the old local admission first, probes the adapter, then compares the saved
binding snapshot in a transaction before publishing a new binding generation. A failed
probe cannot leave a previously verified client usable through that attempted rebind.
Changing a name's owner requires an additional explicit compare-and-swap on the prior owner.
Merely changing the configured expected ID is not enough. The reference conservatively
allows at most one configured alias per incarnation, preventing duplicate placement counts;
multiple endpoint aliases would require separate, explicit production selection semantics.

Configuration removal disables the alias and advances its generation; it does not delete
copies, work, or incarnation rows. Reopening a catalog does not recreate verified clients
from saved URLs. Re-adding the original backend, including under a new name/URL, requires
fresh matching evidence. Historical resolution starts from the immutable owner, looks for
a currently configured and locally verified binding for that owner, refreshes the evidence,
and passes the expected owner to the adapter operation. There is no fallback to the old name.

Copy recording takes a mandatory incarnation from a receipt **already validated against
the admitted operation**. Pending work similarly takes an explicit admitted incarnation.
Neither derives ownership from whichever backend currently occupies the configured name.
The reference defers a late receipt when its owner is no longer bound; production reconciliation
must preserve that receipt/work and reconcile against its original owner, not discard it or
rewrite it to the replacement. The experiment does not implement upload transactions,
archive byte verification, receipt authenticity, or catalog/object mutation reconciliation.

Administrative `bound`/`disabled` is independent from observations such as unverified,
unavailable, or mismatch, and from read/write capability. An unreachable bound store remains
bound administratively. A healthy different incarnation can still serve its own holdings.
The model's filesystem capability booleans are illustrative; production adapters must make
truthful observations, and per-operation errors remain authoritative. This is not an
implementation of readiness policy, retirement, or service-wide degraded operation.

Binding generations fence catalog updates and stale local registrations. This is not a
complete distributed lease/revocation system: an already admitted read may finish on its
original owner during a concurrent administrative removal. The integration agent must
specify admission, draining, cancellation, and mutation fencing for in-flight workflows.
No such race is allowed to change the expected owner or redirect effects to a replacement.

## Cloud-adapter research: do not mistake a marker for an effect fence

A provider-backed marker can be useful, but its creation and lifetime must be qualified.
AWS documents [conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html):
`If-None-Match: *` prevents overwriting an existing current object; a competing writer may
receive 412, and concurrent deletion can produce 409. In a versioned bucket, a current delete
marker may allow creation again. Therefore use a provider-proven conditional create, reread
the winning marker after a create race, and never turn a missing marker for enrolled storage
into automatic reprovisioning. Do not substitute a HEAD-then-unconditional-PUT sequence.
Keep marker storage immediately readable and protected from lifecycle/deletion rules.

AWS's [expected bucket owner condition](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucket-owner-condition.html)
checks the owning account, not this application-level incarnation. Consequently, it is a
useful routing safeguard but does not alone identify a bucket recreated in the same account.
AWS documents [atomic updates to a single key](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel).
**A GET of the marker followed by an operation on another key is not an atomic two-key
incarnation precondition.** Bucket/root recreation between those operations remains a proof
obligation. The local root-fd demonstration must not be claimed as a generic S3 solution.

Before claiming conformance, each remote adapter needs an enforceable co-binding of the
expected incarnation, continuation/locator, and backend effect: for example a provider-stable
resource handle with adequate lifecycle guarantees, or a qualified incarnation-scoped object
layout and ownership/serialization mechanism. Namespaced keys alone do not prove reads,
writes, completions, or deletes safe under every recreation race. Establish the provider's
actual guarantees and reject unsupported cases rather than advertising a fictional fence.

No AWS, Backblaze, or S3-compatible provider was exercised. AWS semantics are not evidence
of Backblaze behavior. Marker bootstrap, permissions, versioning, concurrent recreation,
continuation replay, and destructive operations require provider-specific qualification.
This branch intentionally provides no cloud implementation that would conceal that gap.

## Audited source anchors and integration seams

All source observations below are pinned to
`main@06a5153f166c1611e94d4a3637e8f934348628ed`, not to a moving branch.

| Audited source | Observation and integration seam |
| --- | --- |
| [placement_choices.py](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/riverhog/src/riverhog_core/placement_choices.py) | `archive_binding_sha256` hashes configured name and URL. Keep any configuration fence concept separate from verified ownership. |
| [archive_store_registry.py](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/riverhog/src/riverhog_core/archive_store_registry.py) | Name-keyed capabilities have no incarnation. Production needs owner-aware resolution, admission, and expected-owner propagation. |
| [deps.py, startup construction](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/riverhog/src/riverhog_api/deps.py#L114-L210) | Startup checks every configured adapter, constructs name-keyed bindings, and checks restore-required cache availability. Reconcile durable registration before admitting verified capabilities; degrade only according to required capabilities. |
| [protocol.py, descriptor](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/protocol.py#L175-L220) | Descriptor exposes software identity/read mode, not durable backend identity. Extend the focused protocol and adapters together; do not confuse inert object identity assertions with routing. |
| [filesystem adapter](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/some-implementations/riverhog/storage/filesystem/src/a_riverhog_filesystem_store/adapter.py#L460-L520) | Root lock/private state exist; descriptor lacks incarnation. Integrate witness persistence with existing ownership and private metadata, and apply the effect fence to every operation, not just reads. |
| [catalog retirement relation](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/riverhog/src/riverhog_core/catalog_models.py#L536-L553) | Retirement and its archive-copy FK use `(collection_id, store)`. Move ownership keys and all dependent relationships to incarnation IDs in Riverhog's own catalog baseline. |
| [Backblaze construction](https://github.com/nashspence/riverhog/blob/06a5153f166c1611e94d4a3637e8f934348628ed/some-implementations/riverhog/storage/backblaze/src/a_riverhog_b2_store/app.py#L78-L131) | Configures bucket/root prefix and uses HEAD bucket for readiness. That is not an incarnation proof. Qualify provider-specific persistence and effect semantics. |

Follow-on integration pointers, **not an exhaustive service audit**: upload and copy-intent
call sites in `services/collection_uploads.py` and `services/archive_copy_jobs.py`; all
completed-copy/object records; retrieval plans and continuation handles; retirement/deletion
plans and challenges; mutable publication work; retrieval-cache associations and leases.
For each, persist the admitted incarnation, require it on resume/effect/completion, and
forbid name fallback. A cache incarnation change invalidates cache-local/rebuildable state;
it cannot transfer or erase archive authority. Keep disabled/unavailable holdings visible.

The production baseline must use Riverhog's existing PostgreSQL owner and linear schema
history, without a compatibility/migration track for this pre-v1 cut. Keep reusable wire
contracts in their focused packages and domain registration in Riverhog services/ports;
do not import supplied adapter implementations into the server. Regenerate and validate
external contracts only as part of accepted integration. Field naming/configuration shape
belongs with the subsequent #900 cut, not this experiment. Permanent loss and destructive
historical purge belong to #909 and are not inferred from removal or unreachability.
