# Integration guide for #958 reference material

Read `HANDOFF.md` first. The issue controls scope and decisions; this file is
application guidance for the concrete input, not a parallel authority document.

## Apply in reviewable slices

1. Review `patches/01-payload-read-domains.patch`. It imports the existing 1 MiB
   volume-document bound into recovery; adds a shared 16 MiB encoded pack-index
   budget to `riverhog-archive-contracts`; checks it in the planner and both
   recovery index paths; and permits adjacent zero-length tar members without
   permitting overlapping nonempty extents. No archive bytes or format labels
   are rewritten. The six new tests live in `tests/unit/test_archive_payload_domains.py`.
2. Run `verify.py` in a fresh detached worktree as described below. Inspect the
   captures using `inspect_fixture.py`, rather than treating compression as an
   opaque new Riverhog format. Replay uses fixed bytes and expected identities;
   it does not call a candidate fixture writer.
3. After settling remaining pre-v1 hard cuts, move/adapt the relevant replay
   tests and captures into the component-owned or repository-wide test locations.
   Replace these *pre-v1* captures deliberately with the accepted baseline;
   this branch does not make f5d09bf an indefinitely supported format.
4. Review `patches/02-v1-readability-policy.patch` independently. It changes only
   the existing `archive` and `recovery` compatibility strings, avoiding a new
   parallel release inventory. It is not applied by the verifier. The patch
   does not decide post-major support and does not substitute for state-owner
   declarations, enforced compatibility or release qualification.

Reference checkout verification, with dependencies already installed:

```sh
python .reference/verify.py --worktree /new/empty/path/riverhog-958-check
```

The verifier uses the current interpreter. In a configured development checkout,
run it through the repository's locked `uv` environment. It never installs
packages, contacts providers, changes an existing worktree, or applies patch 02.
The destination must not exist. Remove the detached worktree explicitly after
inspection with `git worktree remove /new/empty/path/riverhog-958-check`.

For accepted integration, extract/apply only reviewed source/test changes onto
then-current authoritative main through the normal rail. `.reference/` is
handoff material, not a directory to merge wholesale into main. Root README,
architecture, workflows, and lock files are intentionally untouched.

## The 16 MiB choice

This reference deliberately raises the reader budget instead of repacking valid
50,000-member output. The observed 11,297,822-byte index fits within 16 MiB, while
both writer and reader have one explicit encoded-size cap. The count limit and
byte limit remain separate constraints. This is a concrete proposed policy,
not a claim that all theoretical parameter combinations were exhaustively
proved to fit or that a new public constant is already freeze-approved.

Raising a count limit later does not silently raise the byte budget. Do not
shrink historical reader acceptance when tightening future construction policy.
If integration prefers splitting instead, retain a reader domain sufficient for
all *accepted v1* bytes. Pre-v1 hard cuts remain permitted; the issue's
historical-readability language must not accidentally preserve obsolete pre-v1
formats. Include the new constant in regenerated native contract evidence.

## Replay coverage and remaining evidence

| Finding | Concrete material here | What remains before issue closure |
| --- | --- | --- |
| F01 | Shared descriptor bound; encrypted 256-part payload regression; over-limit parser rejection | Locked installed independent-age and service qualification, plus broader boundary vectors |
| F02 | Shared index bound; real 50,000-member extraction; inclusive/oversize index tests; overlap rejection | Final resource-budget review and integrated construction/retrieval qualification |
| Newly exposed zero-length rejection | Reader compares next header against previous extent end, allowing equality; real rendered zero-byte member regression | Keep the explicit negative overlap test when adapting the patch |
| F03 | Captured filesystem small/segmented committed representation; no-startup materializer, normal-read and missing-ledger tests; fixed S3 mapping/metadata/incarnation envelope | Component-owned state declarations and transition/dispatch ownership; AWS/B2 qualification, exact revision and other representative state cases |
| F04 | Fixed root/sequence, binding-tree, record-set, source-proof, pack/segment, description/tag bytes; service and recovery provenance readers | Final immutable corpus, atlas linkage, format-family inventory and actual cross-release/installed proofs |
| F05 | Separate compatibility-prose patch | Maintainer post-major decision; ordinary end-to-end service and migration-cost gates |
| F06 | Classification guidance below; no speculative wire rename/removal | Source-owned terminology and accountable dispositions for persisted fields |

The semantic capture is a derived archive with inherited source history. It
contains 44 decoded logical objects and exact historical source-root preimages.
The service-reader test is not proof of full HTTP retrieval, archive-copy jobs,
catalog reconstruction, or an installed service upgrade. The filesystem capture
has two committed logical objects, not a complete encrypted collection. The S3
fixture is not AWS or B2 live qualification. Keep these evidence distinctions.

## Durable-state ownership integration seam

Extend the existing `release.toml` state owners and `scripts/state_contract.py`
projection path; do not create a second hand-maintained authority catalog.
A component-owned `python-contract` declaration can describe a mixed physical
representation while tests execute its real decoder/materializer. Naming a
state owner alone is not compatibility evidence.

For filesystem storage, account for path hashing and persisted logical-path
records; current revision selection; object metadata discriminators; payload
placement; completed-object segment ledger schema and integrity domains; and
storage-incarnation continuity. Version dispatch must be based on representation
discriminators, not the storage-incarnation UUID. A current-pointer or ledger
failure must not become inferred empty state or implicit cleanup.

For S3 support, account for root-prefix/key mapping, private stored digest and
placement markers, inert caller assertions, exact provider revisions when
required, and the incarnation marker outside the logical object prefix. Avoid
duplicating ownership inconsistently across the AWS/B2 wrappers and their shared
representation layer. A portable logical export and preserving the same backing
authority are different preservation sets.

Each migration disposition should state separately: metadata bytes read,
payload bytes read, bytes rewritten, provider-side bytes copied, adopter-network
bytes transferred, required free space, interruption/restart behavior, and what
identities must remain unchanged. A provider-side full-object copy is not a
metadata-only update merely because its purpose is changing metadata.

## Proposed vocabulary and preservation boundaries

| Concept | Distinction needed for integration |
| --- | --- |
| Artifact identity / byte digest | Opaque member identity is distinct from the SHA-256 of file bytes; equal bytes need not imply one member identity. |
| Collection / archive / copy | Collection membership, immutable root-bound representation, and a placement with copy-adjacent metadata are separate concepts. |
| Archive generation | Cohort identity carried across a sealed authority graph; its 64-hex representation alone does not make it a content digest or format version. |
| Root identity / stored identity | Hash of exact canonical root plaintext versus exact stored ciphertext identity; randomized encryption and embedded stored hashes have different propagation effects. |
| Recovery descriptor | Plaintext bootstrap/key-selection and stored-root integrity information; not a secret, trusted-origin signature, or independent authenticity authority. |
| Relative path / logical object path / provider path | Archive-relative path, adapter-addressed namespace path, and private provider/on-disk representation are three layers. |
| Payload volume / descriptor / terminal | Bytes, their bounded description, and authenticated sequence termination have different budgets and identities. |
| Archive part / adapter write segment | Durable plaintext/ciphertext verification extent versus transfer partition. The construction policy may change while old extent interpretation remains supported. |
| Semantic provenance / custody structure | Journals' meaning is distinct from bounded storage pages, bindings, proofs and their recovery closure. Do not invent a second provenance language. |
| Storage incarnation / representation version | Backing-authority identity is not a decoder-version discriminator. |
| Immutable archive / complete selected-copy recovery | The latter also needs the declared description/tag state. Missing required tag authority must not become an invented empty tag set. |
| Private / disposable | A private completed-object ledger can still be indispensable durable state. |

Retain `parts` as durable verification structure unless a justified pre-v1
redesign replaces that job. Classify `age_state` by its encryption/range-reading
job, not merely its origin in upload code. Reconsider `plan_sha256` only after
tracing copy/reconciliation consumers; retaining an opaque carried digest does
not automatically freeze the planner. The reference intentionally makes no
speculative removal or rename of these fields.

Separate document-format discriminators from stored-object reconciliation
labels, and account for their mapping. Exact-field parsing plus a fixed storage
profile is a closed format, not a ready-made extension-negotiation mechanism.
Preserve historical interpreters when introducing new supported formats; do
not require older software to accept future objects.

Lock the interpretation and supported access to accepted bytes: canonical
encodings, paths, identities, commitment preimages, sequence and terminal rules,
selected provenance closure, embedded proofs, adjacent metadata, and supported
adapter materialization. Do not freeze future pack planning, valid part-boundary
choices, randomized encryption output, buffering, concurrency, staging, SDKs,
rebuildable caches, or source-code layout. Active resumable sessions may have
separate continuity obligations; they are not automatically disposable either.

## Final gates

Run the repository's current full lint, unit, distribution, build and exhaustive
Linux qualification rails. Require actual external `age`/batchpass runs and the
installed offline recovery/materialization proofs owned with #546. Retain
source-owned freeze/atlas discovery; regenerate evidence only after the intended
hard cuts are accepted. Inspect the exact pushed integration SHA's GitHub checks.

No comparison test here proves a hypothetical v2 coexistence policy. Decide
that policy in #958, then add the corresponding dispatch and embedded-proof
retention tests. Publication of this reference must not close #958 or trigger
live storage cleanup/provider qualification.
