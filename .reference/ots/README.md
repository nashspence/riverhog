# OTS proof lifecycle — reference only for #870

This is concrete external reference input under #903, not accepted application code
or a public API. See `../HANDOFF.md` for provenance, base and validation boundaries.
The catalog traversal experiment already linked from #870 is deliberately untouched.

## Run in isolation

From the repository root, with Python 3.12 or later:

```sh
python3 -m venv /tmp/riverhog-ots-reference
/tmp/riverhog-ots-reference/bin/pip install -r .reference/ots/requirements.txt
/tmp/riverhog-ots-reference/bin/python -m pytest \
  -c .reference/ots/pyproject.toml .reference/ots -q
```

The tests use no network, credentials, timers, sleeps, public calendars or Bitcoin
daemon. They use real upstream OTS serialization/operations and a separately assembled
Bitcoin genesis vector, as well as explicitly synthetic calendar and chain ports.
The branch-only workflow runs this command outside the repository workspace. Nothing
here adds a workspace member, public package, executable, application, or release entry.

## Contract boundary

`prepare(statement_digest, nonce, urls, now=...)` accepts exactly 32 **already-computed
SHA-256 digest bytes** supplied by the witness application. It does not construct,
canonicalize, domain-separate, or claim authority over a Riverhog witness statement.
It does not hash the digest again as if it were the original statement file.
The detached `.ots` file records this caller digest with `OpSHA256` as its file-hash
algorithm. The original statement bytes and their meaning remain caller-owned.

The caller supplies 32 cryptographically random nonce bytes in production. Persist
that nonce with the job **before any network effect**. The calendar receives
`SHA256(statement_digest || nonce)`, with the append/hash path retained in the proof.
A restart or replay never invents a new nonce. The deterministic tests deliberately
use fixed fake nonces. Randomness generation is not an implicit effect in this core.

`step(job, calendar, now=..., allow=..., retry=...)` performs at most one calendar
exchange and returns an immutable proposed replacement job. It does not sleep, read
a clock, create a transaction, use Riverhog APIs, or fetch a proof-supplied URL without
an exact caller allowlist match. `now` and persisted deadlines are UTC epoch seconds,
not process-local monotonic timestamps. Clock rollback delays eligible work; the caller
must supply a sensible clock. Backoff is capped exponential with no hidden randomness;
production jitter/rate limits belong to the scheduler.

The calendar port takes `submit` or `upgrade` and returns serialized **Timestamp
subtree bytes**, not a detached proof or an assertion of success. An upgrade asks for
the commitment at the exact pending proof node, which can differ from both the
statement digest and the initial submission commitment. All proof operations and
Bitcoin attestation verification use `opentimestamps==0.4.5`; this is not a new OTS
wire-format or cryptographic implementation.

## Lifecycle and retention

| Situation | Proposed state / behavior |
| --- | --- |
| Newly enqueued | Nonce, exact digest, submission URLs and due work are durable before submission. |
| Submission accepted | Merge into the blinded root; save standard detached proof bytes and discover allowed pending subtrees. |
| 404/not yet mature, outage, timeout, 429/5xx | Keep the old proof, increment persisted attempt count and schedule a capped retry. |
| Malformed, truncated, trailing, oversized or over-budget response | Keep the old proof and retry with a bounded diagnostic code; no partial merge is committed. |
| Permanent transport rejection | Keep evidence and task; `due=None` disables that task. Operator-approved repair/re-enabling is caller-owned. |
| Pending URL outside allowlist | Keep the attestation in the proof but do not fetch it. Expanding the allowlist discovers it on a later step. |
| Another calendar is unavailable | At most one due task runs per call; persistent due ordering prevents it from monopolizing other due work. |
| Bitcoin attestation appears | Presence can be inspected with `has_bitcoin`; it is NOT validity. Keep pending retries until the application chooses to retire/suspend them. |
| Bitcoin node unavailable or moving chain | Verification reports unavailable, not invalid. Proof evidence is unchanged. |
| Cryptographic path does not match active header | That attestation is invalid relative to the observed active chain; another independent path can still be valid. |
| Restart or lost response before local commit | Resume persisted deadlines/evidence; a calendar operation may repeat using the same commitment. No exactly-once external effect is promised. |
| Corrupt/unsupported local state | Reject it before I/O; do not silently discard evidence, regenerate identity or reset progress. |

Existing pending attestations remain in matured proofs. A forged Bitcoin marker must
not cancel useful retries, and confirmation policy must not be baked into proof
maturation. Successful lookups that add no new evidence still back off. There is no
unbounded retry loop in one call and no finite attempt count that deletes the proof.
Removing a URL from the allowlist immediately blocks its work without deleting it;
application schedulers should consider allowlist eligibility, not repeatedly spin on
`Job.next_due` for administratively blocked work. No background scheduler is included.

## Persistence boundary

`dump_job` / `load_job` are a versioned **reference-only** state representation with
strict field sets, duplicate-key rejection, bounded input, and digest/work binding
checks. They are not a proposed shared statement or public durable-state contract.

`sqlite_example.py` is an application-owned demonstration, not a new shared database.
Initialization is explicit. Startup refuses missing or unsupported databases.
`enqueue` deduplicates by the caller digest, preserving the first job's nonce and
progress. `save(expected_revision, proposed_job)` uses one transaction for the new
proof, retry progress and append-only historical proof revision. An older worker
cannot overwrite a newer result. Network calls must be outside that transaction:

```python
revision, job = store.load(statement_digest)
proposed = step(job, calendar, now=utc_epoch_seconds, allow=allowed_calendars)
if proposed != job:
    store.save(revision, proposed)  # StaleWorker => discard result and reload
```

Integration must adapt this to the application's own migration history and atomically
commit catalog progress with a durable witness-work intent. Do not adopt this schema
or serialized job wholesale as `riverhog-client` state. There is no collection deletion
operation here: loss of catalog authorization is not destruction and must not delete
historical witness evidence. Scheduling retirement, operator repair, disk quotas,
backup/restore, multi-host leases, and historical evidence export remain application
responsibilities. Every selected job and proof is bounded; total database growth is
not a global storage quota.

## Bitcoin verification and confirmation policy

`verify(proof, statement_digest, bitcoin)` reparses/recomputes the proof and produces
per-attestation `valid`, `invalid` or `unavailable` observations, pending/unsupported
counts, and a digest identifying the exact proof bytes checked. It can validate one
path even when another path is bad. Unsupported attestations are preserved but never
promoted to Bitcoin evidence. At most eight distinct heights are queried by default;
others are explicitly deferred as unavailable/budget, not silently counted as valid.

`BitcoinCore(rpc)` is a read-only adapter around a caller-supplied authenticated RPC
function. For each distinct height it obtains a synchronized mainnet tip, looks up the
active block hash by height, reads the raw 80-byte header, rechecks the active hash and
tip, and checks the header's double-SHA256 hash. A changed observation is retriable,
not a cryptographic verdict. OTS verifies its computed commitment against the header's
raw Merkle root in Bitcoin serialization byte order, not explorer/display hex order.
The recorded time is the header timestamp, **not an exact statement creation time**.

Consensus validation, accumulated work and active-mainnet authority are delegated to
a **trusted fully validating Bitcoin Core node**. Neither a matching header hash nor
a fake `Bitcoin` test implementation independently establishes those facts. There is
no hand-written SPV/consensus verifier or third-party explorer fallback. A production
adapter must bind authentication and network identity; the five sampled RPC calls are
not an atomic historical snapshot and cannot eliminate every possible ABA reorg race.
Reverify when using a result, after restarts, and when chain freshness matters.

`confirmation_policy(verification, minimum=None)` is a separate pure decision. For a
valid anchor, confirmations are `tip_height - block_height + 1`; a tip block has one.
A valid proof may fail a six-confirmation application policy while remaining
cryptographically valid. A high confirmation count can never make an invalid path
valid. Results are not cached as permanent success. Policy thresholds and retirement
are owned by session 3 integration.

## Bounds and trust notes

The reference caps detached proofs/calendar responses at 64 KiB, state at 200,000
bytes, semantic trees at 1,024 nodes / 128 attestations / depth 64, and queued work at
128 items. A read-budgeted upstream context rejects excessive parser work before a
large tree can be constructed; upstream operation message limits also apply. A port
must enforce the response bound *while reading*, not allocate an unbounded response
and rely on the core's subsequent rejection. Adversarial process memory/CPU isolation
and comprehensive fuzzing are not qualified by the focused deterministic tests.

No HTTP implementation is supplied: the production calendar adapter must explicitly
bound timeouts and response bytes (`limit + 1` read to detect overflow), forbid
redirects, constrain DNS/addresses according to deployment policy, preserve the exact
configured base URL, and map only known operational failures to `CalendarError`.
The upstream convenience RemoteCalendar is not used; its returned mutable objects,
unrestricted transport defaults, and EOF handling are not this core's boundary.
The Bitcoin RPC adapter must likewise bound transport and map outages/not-found into
`BitcoinUnavailable`. Programming errors are not swallowed as ordinary retries.

## Research and compatibility evidence

Reviewed primary sources on 2026-09-24:

- OpenTimestamps Python library and PyPI 0.4.5 release:
  https://github.com/opentimestamps/python-opentimestamps and
  https://pypi.org/project/opentimestamps/0.4.5/.
  Core timestamp source blob `5dd19dc9f9d2018fb29b2c903bdd8af5d2849fd2`,
  notary blob `c747d57e7db7d004b8d94e4d9a6319a4bb89ff3d`,
  operation blob `29f2e86da3d98dcb89147a22c5a4a064e09484b6`,
  serialization blob `78c7b7c471013fd0ddf967a7a4a6f2390030a296`,
  calendar blob `c5686d63b0c16989fcdf7a9d2d6c8d45bcb22b5c`.
  The inspected upstream default-branch sources are not asserted to be release-tag
  files; the executable tests qualify the separately pinned PyPI dependencies.
- Bitcoin Core documented RPC semantics:
  https://bitcoincore.org/en/doc/30.0.0/rpc/blockchain/getblockhash/,
  https://bitcoincore.org/en/doc/30.0.0/rpc/blockchain/getblockheader/,
  https://bitcoincore.org/en/doc/30.0.0/rpc/blockchain/getblockchaininfo/.
- Genesis constants and transaction construction:
  https://github.com/bitcoin/bitcoin/blob/master/src/kernel/chainparams.cpp,
  audited blob `147f6d01d9126d80c9b0ec66355c357a9c37c072`.
  `genesis-vector.json` is independently assembled from those public protocol facts,
  not an upstream library roundtrip used as the sole proof of correctness.

First-party reference material follows the repository's Apache-2.0 default.
OpenTimestamps retains LGPL-3.0-or-later licensing; python-bitcoinlib and
pycryptodomex retain their own terms. Nothing is vendored. The pinned test environment
is not an application publication lock or a release-level license qualification.

## Intentionally left for session 3

Shared/domain-separated witness statement ownership and bytes; source/collection/root
identity binding; catalog ingestion and application transactions; Minisign; public
client API; Riverhog core; production HTTP/RPC/CLI/service/configuration; secret
management; install/build/release/licensing and publication coordinates; catalog
departure integration; scheduler/operator lifecycle and release qualification.
