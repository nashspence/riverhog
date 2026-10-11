# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: #964. Convention: #903.
Branch: reference/external/964-large-collection-scaling
Previous reference / direct parent: cee4c00f2fb1a5133bee8150f0399d89c9d2c3d9
Original reference runtime base: main@8425e672c60285a549ca4d5aaba15ecbd99e5bf3
Original base tree: d18b813a489955b085094765f98282ea2e2276d0
G22 and G26 inspected source: main@152d095038df0c0d0ed642cf265d47b11c65a754
Inspected source tree: 991e994457b487a7dca016b7fc0409485cef25c6
Approved policy: #964 comment 6111028968; prior G22 publication: 6111500038.

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, in this conversation
Published by: connected GitHub integration; commit metadata is publication metadata.

## Integration agent: both requested designs are now supplied

Read [g26/DESIGN.md](g26/DESIGN.md), [g26/RESEARCH.md](g26/RESEARCH.md), and
[g26/README.md](g26/README.md) for the new source-factored discovery design.
Read [g22/DESIGN.md](g22/DESIGN.md) for the unchanged exact-prefix validation design.
The old statement that G26 has not been researched is superseded; this is a design
submission with **specific derived-index identity approval** (#964 comment 6112668478),
not an implemented state change or approval of unrelated frozen identities.

G26 selects a normalized fact ledger (one raw assertion plus birth/retraction
coordinates), exact snapshot headers, shared roots/imports/documentary dependencies
and fixed typed scope rules. It avoids materializing member-by-history or prefix-
by-assertion products. Correlated imported late metadata requires BOTH imported
history and causal applicability for the SAME member. Public scope semantics,
current permissions, source binding proofs and archive authority stay intact.

The proposal deliberately changes the DERIVED discovery extraction/dataset identity
so sealing hashes compact canonical source facts instead of every expanded logical
membership. New generation/read identities invalidate old cursors. Archive/member/
history/journal bytes and commitments do not change. **That exact derived-index
identity change is now approved** by the maintainer;
rebaseline its own new deterministic identity with current-source freeze tests,
not by silently updating an expectation. The implementation is an explicitly
approved **pre-v1 hard cut with no schema migrations or backward compatibility**:
new current-only database schema and extractor, fail-closed incompatible old
schemas, new-only archive-backed index rebuilding, no dual readers/writers,
old cursor translation, rolling transition or legacy extraction runtime.
Preserve archives, custody receipts and non-rebuildable operational authority.

Common unary/shared-snapshot queries use exact posting predicates and shared reverse
minimum-support propagation instead of one complete traversal per member. Imported
late-claim guards retain exact same-member correlation with a separately stated
G28 query bound. No universal output-linear query, all-operation linearity, or
under-600-second runtime guarantee is claimed.

## Concrete material and implementation map

[g26/factored_index.py](g26/factored_index.py) is an executable SQLite relational
model of compact sealing, interval leaves, guards, deduplicated traversal and common
query propagation. [g26/native_compiler.py](g26/native_compiler.py) lowers actual
native journal/history fixtures for differential scope comparison. It deliberately
uses small in-memory/per-prefix views and is NOT a production source-normal reader
or bounded-memory implementation. DESIGN.md specifies the production source facts,
virtual indexed joins, staging protocol, complete manifests, query paths, new-only current-schema hard cut and exact file-level owner map
that replace those test conveniences.

The two G26 test files, [profile_g26.py](g26/profile_g26.py),
[counters.json](g26/counters.json) and [evidence.json](g26/evidence.json) make the
semantic and structural observations reproducible. Do not copy the model into main
as a new public rule engine, or claim that successful model counters establish the
SQLAlchemy/PostgreSQL production builder. Production wiring and required witnesses
are explicitly part of the integration job.

The parked discovery experiment described in comment 6102286067 may supply useful
snapshot-link/capture/fence tests. Its actual private patch must be reconciled by
its owner; this producer did not inspect that local file. Keeping its expanded
logical hash would preserve the identified scaling cost and is not the selected
G26 design.

G22 helper modules, integration patch, tests and raw evidence are unchanged.
Its original expanded prototype tree remains b4ac503c05f9c05ff0c39cb4072ba149dee28d2f.
Do not relabel its 187 provenance plus 21 client passes as a combined G22/G26 run.
G22's online/correction profiles and current validation boundaries remain as stated.

## Validation actually performed for G26

34 new native/model tests passed: shared-factor work, exact same-member guards,
12 seeded fixedpoint controls, diamond/cycle deduplication, minimum witness order,
immutable/retry/reopen behavior, growing prefix headers, native bound/retained and
retracted scopes, and genuine source-binding proof imports with/without causal links.
15 selected unchanged native source tests passed separately; one HTTP case was
excluded for missing installed distribution metadata. A prior broader selection
failed six HTTP/rebuild cases during setup for that environment reason and emitted
an exception-cleanup warning; it is recorded as FAILED, not an aggregate pass.

Model counters at N=16/32/64/128/256/512 members and shared assertions show 5N+4
physical/hash records instead of materializing N*N logical memberships; common
minimum-support propagation settles N+1 nodes and N edges. The 128-prefix control
stores 128 raw assertions and 6N+1 total model records. These are exact model work
counts, NOT full production, valid wire workload, or media timing measurements.

Environment: native Python 3.13.5 / SQLite 3.46.1, not the locked workspace; exact
upstream rfc8785.py v0.1.4 implementation loaded externally with an export shim.
No replacement canonicalizer, external dependency copy or warning relaxation is
included in the published reference. Code/source hashes and exact limitations
are in g26/evidence.json. Documentation and Git whitespace checks accompany the
publication; the remote tree must match the prepared reference files.

Not run: production source-normal extraction or PostgreSQL query plans, complete
HTTP/rebuild/security/cursor integration, actual new-schema hard cut and guarded
archive-backed rebuild, full locked gates,
final-image 128-file/large-file/recovery qualification, CI/CodeQL, or provider/release
operations. There is no proof of complete v1 growth conformance in this reference.

## Authority, history and sequence

The original reference runtime tree remains pinned to 8425e672, not rebased. Use a
separate checkout of inspected 152d0950 to reproduce native fixtures; reconcile
against exact post-#963 source before actual integration. The source inventory's **original** audited view and digest remain in a prior
reference commit; the **current** decision-reconciled proposal updates G08/G26
and has a new verified SHA-256. G22 files and earlier superlinear evidence remain
unmodified.
Read [v1_inventory/DECISIONS.md](v1_inventory/DECISIONS.md) for approved directions;
G22 and G26 designs are supplied and the **G26 derived-identity hard-cut decision
is now approved** (#964 comment 6112668478). Any former request for separate
migration/compatibility approval in older reference files is superseded.
That does not transform research recommendations into approved final declarations.

#961 owns onboarding, #963 evidence, #964 growth design/fixes and #469 v1 accounting.
Keep #960's explicit timing-shortfall disposition, 600-second maintained profile,
all functional/security/recovery obligations and existing qualification cadence.
No composite passes with unresolved or incompatible delegate costs. Prefer cheap
adversarial growth tests plus real boundary witnesses; do not remove required proof
or hide source work outside the measured/declared lifecycle.

Only .reference material changes. The exact newly published commit recorded on
#964 is the durable handoff identity; retain all earlier reference ancestors.
Issue decisions and approved owner contracts control the normal integration rail.
No change to main, release/provider authority, or protected settings is made here.
Native Development/dependency mutation is unavailable through the publisher;
Markdown references are not claimed to establish those native relationships.
