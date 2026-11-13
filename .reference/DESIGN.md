# Bounded app-key usage recording

This is a proposed owner change for [#990](https://github.com/nashspence/riverhog/issues/990),
prepared under [#903](https://github.com/nashspence/riverhog/issues/903). It is not an
accepted production policy. The integration owner must record acceptance or an
alternative in #990 before delivering a replacement source on PR #979.

## Proposed policy decision

**An app key's `last_used_at` is a lossy observation of successful authentication,
not a prerequisite for authorization.** A successful authentication may leave it
null or unchanged when the catalog writer cannot be acquired without waiting.
An omitted observation is dropped; it has no deferred delivery, retry, replay,
freshness, or completeness guarantee. Later authentication makes a new attempt
for that later observation. A null or old timestamp cannot establish inactivity.
It is not a usage ledger, quota input, security audit trail, or authority token.
The observation concerns credential validation; request completion and the
authorization or outcome of later operations remain separate facts.

For a particular stored key, successful observations never move the timestamp
backward. The application summary still selects the maximum recorded value
among its current keys; deleting a key can change that aggregate. The response
field names, types, nullability, and sorting interfaces remain the same. Their
source-owned API descriptions now state these semantics.

Only classified database contention is an allowed omission. Invalid credentials
still yield no principal. Authoritative-read errors and unexpected observation
errors propagate; they are not converted into successful authentication. This
design does not conceal corruption, I/O faults, connection failures, programming
errors, or cancellation behind a generic best-effort exception handler.

If the owner instead requires durable recording for every successful
authentication, this candidate is not suitable: the owning issue must define
that stronger durability/admission contract and its resource ownership. No
separate durable queue is implied or implemented here.

## Exact source and change boundary

The audited and experiment base is
`f7bf8b2b6644d80bf2edae4414c3d18574e511c8`, the `codex/964-session-owner`
source explicitly confirmed by #990 and the head of draft PR #979 when inspected.
This is the issue-established source for this handoff, not a claim that PR #979
has passed integration qualification or that current `main` has been tested.

The candidate edits the app-key service, its API field descriptions, and a
focused app-key test module. `.reference/` contains the handoff and evidence.
The archive implementation, publication fixtures, database schema, key-mutation
generation logic, permission checks, quotas, and growth/control contracts are
unchanged. Product paths contain proposed edits directly; they are not hidden
behind a runtime switch or represented only by a patch file.

## Authentication and observation ownership

1. The app-key service hashes the presented token and opens a new
   `state_schema.read_snapshot`. SQLite starts a real read transaction;
   PostgreSQL uses a read-only repeatable-read transaction.
2. It loads the current key through the unique token-digest index, verifies the
   digest, checks revocation/expiry, and loads that key's current grants in the
   same snapshot. It builds the existing principal and authorization-view
   identity and rechecks expiry after grant work. It retains no principal cache.
3. It closes that snapshot before asking for a writer. No read-to-write upgrade
   of the authorization snapshot occurs.
4. The same synchronous request owns one independent observation transaction.
   On SQLite it uses the catalog's existing `sqlite_immediate_writer` and
   `sqlite_defer_if_busy` options. The engine temporarily sets `busy_timeout=0`
   for `BEGIN IMMEDIATE` and restores the connection's previous timeout in a
   `finally` block. Writer admission is attempted once.
5. On PostgreSQL it requests the update's relation lock using
   `LOCK TABLE app_keys IN ROW EXCLUSIVE MODE NOWAIT`, then locks the exact key
   row with `FOR UPDATE NOWAIT`. The relation probe matters because row NOWAIT
   alone does not bound its prerequisite relation-lock wait. These are finite
   admission probes, not a retry policy.
6. A conditional primary-key update changes only `last_used_at`. Its predicates
   require the same key ID, presented-token digest, observed access generation,
   current active status, and a null or earlier stored timestamp. It never
   inserts a missing key, changes grants, or increments the authorization
   generation. A changed, deleted, expired, or already-newer row is a no-op.
7. The observation transaction commits or rolls back and closes before the
   request returns. Its surrounding handler suppresses only `OperationalError`
   recognized by the existing `catalog_write_contention` classifier: numeric
   SQLite BUSY/LOCKED families, including extended codes, and the maintained
   PostgreSQL contention SQLSTATEs. The handler does not enclose the
   authoritative snapshot and does not catch arbitrary exceptions.

There are no new executors, detached tasks, timers, buffers, queues, or shutdown
participants. Session ownership stays with the app-key service and the existing
database context managers. A request interruption is not swallowed and does not
imply that another owner's work has stopped.

## Authorization remains current and fenced

Every call resolves the supplied token against current committed state. Invalid,
revoked, expired, deleted, and rotated-out tokens remain denied. Current grants
and their authorization-view identity are derived from the same snapshot.
Credential rotation retains its existing distinction from grant-generation
changes; the observation's digest predicate prevents an old credential's
observation from updating a replacement credential.

An authentication snapshot is not permission for future work. A mutation can
commit after it closes. The existing `require_current_principal`, retained key
validity/generation fences, and operation-specific scope checks remain the
authorities for later work. An observation no-op or BUSY outcome neither renews
the principal nor overrides those fences. In particular, the observer does not
resurrect a deleted key or overwrite an intervening grant change.

## Work and timing bounds

The incremental observation has at most one writer-admission attempt and one
conditional point update. PostgreSQL adds one relation probe and one row probe.
There is no archive/member traversal, grant rescan, retry backlog, or work
proportional to unrelated keys. It retains a constant number of scalar values
per call. Point lookups and index maintenance have their ordinary indexed
database cost; constant statement count is not constant CPU or I/O time.

Whole authentication still reads the current grant set. Existing
`normalize_access` deduplicates and sorts grants; with G grants, that can require
O(G log G) comparisons, O(G) retained grant entries, and work over the actual
grant bytes for normalization and identity hashing. This handoff does not claim
to make the entire authorization path constant work or redefine its existing
growth requirements.

**The bound is zero waiting for writer admission and a finite amount of work,
not a universal wall-clock deadline.** Pool checkout, connection setup, snapshot
reads, filesystem/network I/O, rollback, close, and commit remain ordinary
synchronous operations. The SQLite evidence uses the maintained WAL catalog.
No new numerical latency target or global pool/database timeout is introduced.
Reported elapsed times are observations under recorded conditions.

One immediate attempt per successful authentication deliberately avoids a
sampling configuration or per-key throttle cache. It preserves simple ownership
and fresh authorization. Timestamps can generate normal indexed writes when the
writer is available; the maintained small publication is the bounded recovery
calibration, not proof of 1,000-member or production throughput.

## Archive-worker overlap and qualification

The archive worker may continue to own SQLite's writer while an authenticated
status request proceeds. It is not joined before each status request, and no
fixture bypasses authentication or omits the product observer. The original
archive owner continues to own execution, cancellation, resource custody,
receiving, and actual join. A Future result, request timeout, or successful
authentication is not evidence of worker receipt.

The held-writer experiment keeps an actual original thread and SQLite connection
alive across authentication, independently probes that its writer lock remains
held, then explicitly releases and joins that same thread before disposing the
factory and removing its workspace. The maintained `make profile` driver owns
and receives its original helper before removing publication scratch.

The evidence includes the actual 16-member authenticated publication geometry
requested by #990, plus the existing archive-only shared-root rebuild and the
two retained-history geometries cited in its comments. Their exact-member,
shared-history, ciphertext integrity, custody, and recovery assertions are
preserved. See [VALIDATION.md](VALIDATION.md) for actual outcomes and source
identities; a proposed check is not a passing result.

## Integration boundary

First reconcile the proposed policy and owner diff against the then-current
#990 decisions and PR #979 source, and run the held-writer witness on that
replacement source. Integrate intentionally through the existing rail. Full
exact-final-source and tested-merge CI, remaining owner acceptance, platform and
PostgreSQL qualification, and any long-arm work stay with their existing owners.
This handoff neither qualifies a 1,000-member arm nor authorizes provider
qualification, merge, release, or runtime activation.
