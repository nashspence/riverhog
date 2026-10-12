# Bounded-work revision: source and external checks

This is design research for #964, not a new policy authority or runtime approval.
Inspected source: `152d095038df0c0d0ed642cf265d47b11c65a754`. The reference parent
is `263abca4d8b09f390d90f2b977b10c1557961bc1`.

## What changed after the earlier handoff

The earlier full-query read transaction made consistency simple but tied MVCC
resource lifetime to unbounded computation. Merely fetching fewer rows does not
shorten that transaction. A global optimistic fence with all-work retry avoids
the old transaction but can starve under continued mutations. The selected
persistent-root design trades write/retention/GC machinery for bounded reader
resources and preservation of completed work. This is a recommendation with
explicit semantic changes, not an inference that present source implements it.

Source-owned extent principles are in `scripts/extent_contract.py:17-38`: no
undocumented semantic-total maximum; bounded steps; explicit capacity results;
operator-configurable environmental limits. Issue #766 supplies the original
classification/behavior obligations; #964 owns the new operation growth design.

`packages/state-schema/src/state_schema/__init__.py:386` provides `read_snapshot`,
with PostgreSQL REPEATABLE READ and close in finally. It is not intrinsically
wrong; using it for a consumer-controlled or full-query lifetime is the defect.
`services/search.py:iter_artifacts` currently yields while holding that context.
The revised design changes the lifetime/retained input, not only the fetch size.

`canonical_discovery_search.py:_read_identity` currently traverses selected
collection headers and preserves a latest-view conflict contract.
`canonical_discovery_index.py:574` publishes a completed generation by switching
an active pointer without a catalog event. The new design therefore replaces the
old freshness semantics explicitly and requires an atomic searchable-directory
update at that owner; catalog-sync's event watermark alone cannot supply it.

`catalog_events.py:publish_catalog_event` already serializes a publication watermark.
The new directory must reconcile lock order with that owner, not invent a trigger
that takes the locks in reverse order. `riverhog_protocol/collection_tags.py` has
an existing copy-on-write authenticated radix tag set: useful algorithmic precedent,
not permission to overload its public codec with catalog-directory values.

The request contract caps clauses, values and page count but not logical members,
source histories or query result total. `riverhog_api/schemas/search.py` declares
`DiscoverySupportOut.pointer: str` without a small byte carrier maximum. A page
of 200 matches is not a byte bound. The revision therefore specifies exact
segmented support transport, and streamed source evaluation instead of adding a
hidden assertion/value/support maximum. Native semantics remain the oracle.

`artifact_access.py:require_current_artifact_capability` verifies active claim,
key, expiry and fence independently of immutable scope. `Principal` permits a null
authorization-view identity. The proposed pin/work state cannot treat either
historical content or a null view ID as current permission. Production grant/tag/
capability and HTTP proof remains required; the model does not implement it.

## Primary technical references checked for this revision

- SQLite, Write-Ahead Logging, especially concurrency/checkpoint starvation:
  https://www.sqlite.org/wal.html
  A long reader can prevent checkpoint progress/reset; shorter readers help but
  concurrent overlapping readers still require checkpoint scheduling. The design
  does not promise WAL size is magically bounded by one small fetch.
- PostgreSQL 18, Routine Vacuuming:
  https://www.postgresql.org/docs/18/routine-vacuuming.html
  Old snapshots/transactions can retain tuple visibility and interfere with
  reclamation. Durable application-level immutable versions move that retention
  into explicit storage policy; they do not make retention free.
- SQLite, Row Values, scrolling window queries:
  https://www.sqlite.org/rowvalue.html
  OFFSET does work proportional to skipped rows. Actual full-key plan witnesses
  remain necessary for Riverhog's SQL shapes and tie-heavy cases; the earlier
  native negative controls remain relevant.

These sources motivate resource/access-plan choices. They do not establish
Riverhog's contract, prove the new algorithms or authorize a semantic change.

## Model evidence and what it does not show

The new tests run real SQLite WAL and actual transaction boundaries. A negative
control holds a read snapshot and prevents truncation; the proposed model permits
concurrent writes and successful truncation between immutable node reads while
an old logical query view remains active. Another test exercises sparse 63-bit
keys where node work exceeds the dense-fixture 2N+w counts, guarding against an
incorrect universal linear coefficient.

The model's private hash encoding, simplified authority, small leaf payloads,
synthetic result rows and SQL state names are NOT Riverhog implementations.
It models admission/pins/work/CAS/GC; it does not implement G26 semantics,
streaming canonical JSON extraction, PostgreSQL locks, real descriptor-publication
coverage, actual resource defaults, HTTP 202/read-token contracts or runtime gates.
The KMP test proves only the literal chunk matcher on synthetic strings. No
end-to-end support transport or mainline growth conformance is claimed.
