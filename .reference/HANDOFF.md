# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue #964; convention #903.
Branch: reference/external/964-large-collection-scaling
Direct reference parent: 4621be70fe2229ef7de169862ff1ec5274dec00f
Audited main: 152d095038df0c0d0ed642cf265d47b11c65a754
Audited tree: 991e994457b487a7dca016b7fc0409485cef25c6
Historical runtime base remains 8425e672c60285a549ca4d5aaba15ecbd99e5bf3.
Use a separate exact audited-source checkout. Reconcile post-#963 before acceptance.

Producer: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requester: Riverhog maintainer
Publisher: connected GitHub integration; Git metadata is publication metadata.

## Current entry points

Read [g22/DESIGN.md](g22/DESIGN.md) and [g26/DESIGN.md](g26/DESIGN.md), then
[g22_g26/README.md](g22_g26/README.md) for reproducible diagnostic evidence.
The preceding G27/G28 retained-input design remains at
[g27_g28/DESIGN.md](g27_g28/DESIGN.md).

Earlier recommendations are calibration, not a requirement to retain unsuitable
lifecycles. The common recommendation is bounded work over exact retained inputs,
durable progress, live authorization and fully accounted cleanup. This revision
replaces G22's whole-epoch physical transaction/savepoint model and G26's complete-
assertion staging, flat-hash restart allowance and competing query lifecycle.

G22 keeps certificate-first exact-prefix validation and honest correction/online
costs. It adds bounded phase/adjacency/undo progress, durable source/checkpoint
bindings, explicit summary handles, and exact SHA-256 continuation. No journal,
archive, correction, prefix or semantic-acceptance meaning is changed. The small
new SHA implementation is a diagnostic, not an approved production crypto backend.

G26 keeps source-normal intervals, exact imported-history AND guards and scope
semantics. It specifies segmented paths/values/postings, independently verified
source completion, a resumable ordered Merkle dataset commitment, source-publication
coordination and retained-generation reclamation. The Merkle commitment refines
the earlier flat compact digest proposal; it needs explicit owner identity review,
not a silent change under the old digest. It never replaces archive/journal hashes.
G27/G28 alone owns query invocation, retained views and public support transport.

## Actual verification and limitations

65 new diagnostic tests passed with warnings as errors; see
[g22_g26/evidence.json](g22_g26/evidence.json) for exact commands, environment,
source hashes and verification records. Tests exercise SHA restart/native parity,
bounded Kahn work with high-degree/deep/sparse/diamond graphs, transaction rollback,
stale workers, segmented extent and retry/terminal negatives, deterministic Merkle
carry/fold and SQLite resource release. A native AST guard/generic JSON schema
negative control is NOT a complete valid-journal or production endpoint proof.

At 128/512/2048 synthetic edges, Kahn visits exactly that many edges; graph and
Merkle steps use at most 17 logical transitions per quantum while reopening actual
connections. Larger posting/chunk diagnostics cross the old 20,000/10,000 limits.
These are algorithm/component observations, not SQL machine bounds, media timing,
PostgreSQL qualification or a complete native semantic validator/index integration.

The historical G22 patch/native test evidence, G26 factored-model/native evidence,
and G27/G28 tests remain at their own source identities. They were not rerun or
combined into the new test claim. Do not mechanically apply the old G22 patch or
copy the old G26 compiler's per-prefix in-memory views. Their limits are explicit.

Still required: production resumable semantic phases/undo and reviewed SHA backend,
large valid native journal/profile fixtures, G26 native source-normal/commitment
encoding and both-history-extent/query parity, PostgreSQL locks/plans, real job kill
and power-loss/storage-integrity tests, source handles, publication/pin/GC races,
full locked/built/recovery/security/local and independent exact-SHA remote gates.
Numerical resource defaults, timing targets and composite conformance are not invented.

## Authority and retained history

Only .reference material changes; no mainline, issue-body/state or release/provider
operation is changed. The issue and normal contract/integration rail control adoption.
Keep #961 onboarding, #963 shared evidence, #964 growth work and #469 qualification
ownership, plus #960's report-only profile and recorded shortfall disposition.
Preserve source custody, live grants/claims/fences, archive-only recovery and
non-rebuildable operational state. Retain Alembic, the single current v1_0001
bootstrap and synthetic future forward-revision/interruption tests; no abandoned-
pre-v1 layout migration, dual readers or implicit destructive reset.

The earlier HANDOFF-G22-G26.md is historical. The exact new commit recorded in #964
is the durable #903 handoff identity; branch history is preserved by fast-forward.
Native Development/dependency mutation is not exposed by this publisher; existing
relationships are left intact and Markdown links do not establish new native links.
