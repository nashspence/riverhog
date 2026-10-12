# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue #964; convention #903.
Branch: reference/external/964-large-collection-scaling
Direct reference parent: 263abca4d8b09f390d90f2b977b10c1557961bc1
Audited main: 152d095038df0c0d0ed642cf265d47b11c65a754
Audited tree: 991e994457b487a7dca016b7fc0409485cef25c6
Historical branch runtime base remains 8425e672c60285a549ca4d5aaba15ecbd99e5bf3.
Use a separate audited-main checkout; this reference does not rebase main source.

Producer: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requester: Riverhog maintainer
Publisher: connected GitHub integration; Git metadata is publication metadata.

## Current entry point: G27/G28 bounded-work default

Read [g27_g28/DESIGN.md](g27_g28/DESIGN.md),
[g27_g28/ALGORITHMS.md](g27_g28/ALGORITHMS.md), and
[g27_g28/README.md](g27_g28/README.md).

At the maintainer's request, prior conversational design recommendations are
revisited as calibration. The selected default is bounded work over retained
immutable inputs: short atomic root admission, copy-on-write searchable directory,
leased exact source versions, resumable operators, bounded checkpoints, current
authorization, explicitly budgeted retention and incremental cleanup.
No full-query/export MVCC transaction or global-content-fence whole-query retry.

The design explicitly recommends a current-only pre-v1 revision to discovery's
snapshot/read/token semantics, pending/cancellation/expiry and segmented exact
support transport. These are not hidden cache changes, broad permissions to
weaken custody, or already accepted/deployed public contracts. Mutable browse,
exact discovery and sealed ordinal authority keep distinct progression semantics.
The owning issue and normal reviewed contract/integration rail control adoption.

## Executed evidence and integration limits

47 new stdlib/SQLite model tests passed with warnings as errors. The model covers
root pinning, bounded transaction work, continuous-content progress, WAL checkpoint
negative/positive controls, restart/CAS/fencing, capacity resume, current authority
failure, sparse keys, segmented literal matching and bounded reclamation.
At 128/512/2048 members it takes 311/1077/4147 directory-node visits with at most
17 per quantum; 19/64/244 content changes cause zero complete-query restarts.
These counts describe consecutive-key synthetic fixtures, not arbitrary key maps,
production queries, PostgreSQL behavior, throughput or release qualification.

See [g27_g28/bounded/evidence.json](g27_g28/bounded/evidence.json) for commands,
source/reference hashes and gaps. The inventory verifier passes 64 families,
86 projects and 182 source anchors; G27/G28 proposal summaries and their canonical
digest were reconciled without creating another registry.

Not performed: production directory publication/retention/lock integration,
streaming G26 query/scope compilation, summary mutation coverage, new HTTP/client/
CLI/token/support transport, PostgreSQL concurrency, connection-pool lifetime,
full locked gates, Linux/image/recovery/lifecycle/security or exact-SHA CI/CodeQL
qualification. Neither complete v1 growth conformance nor a timing target pass
is claimed. Earlier native/model evidence is retained at its original identity,
not counted as newly executed tests. The old ProjectionStore lifecycle is superseded.

## Boundaries and history

The original G22/G26 handoff remains at [HANDOFF-G22-G26.md](HANDOFF-G22-G26.md).
Their source/code/evidence and approved G26 identity/current-baseline boundaries
remain intact. Preserve Alembic, one current v1_0001 baseline, explicit bootstrap
and synthetic future forward-revision/interruption tests; no abandoned-pre-v1
migration or compatibility path. Preserve archive authority, both history extents,
current grants/claims/fences and non-rebuildable operational state.

#961 owns onboarding; #963 owns shared evidence; #964 owns growth design/fixes;
#469 consumes approved declarations/executed qualification. Keep #960's report-only
600-second profile and recorded shortfall disposition. No new detailed policy
ledger or inventory is added to AGENTS.md. No ordinary validation is waived.

Only .reference files change, by fast-forward retaining all published history.
The exact new SHA recorded in #964 is the durable handoff identity. Native
Development/dependency mutation is not exposed here; existing relationships
remain intact and Markdown links are not claimed to create native relationships.
