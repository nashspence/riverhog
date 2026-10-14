# NON-AUTHORITATIVE EXTERNAL REFERENCE — #949 revision 3

Owning issue: https://github.com/nashspence/riverhog/issues/949
Convention: https://github.com/nashspence/riverhog/issues/903
Latest decision record: https://github.com/nashspence/riverhog/issues/949#issuecomment-6172547034
Reference branch: `reference/external/949-removable-media-foundations`
Date: 2026-10-14

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro (user-visible identity)
Configuration: no separate effort/mode setting exposed for this handoff
Requested by: repository maintainer, following extent/backpressure clarification
Published through: the connected GitHub API on the requester's behalf

## Authority and purpose

The latest explicit request authorizes researching/updating the issue and reference.
Preceding conversation is intent calibration, not blanket authority. The issue
decision record consolidates the accepted direction; these models are concrete
reference input, not a selected wire design or permission to integrate.

Riverhog remains provider-neutral and does not spool artifact payload while a
destination waits. This revision supplies executable source-admission and
extent/growth witnesses, corrects ambiguous staging wording, and separates bounded
metadata from payload. Real media, topology and operator interaction stay private
to adapters/deployments. No production code, schema, dependency declaration,
migration, generated contract, workflow, release ref or pull request is changed.

Authority remains with #949, subsequent maintainer decisions and the normal
integration rail. Reconcile against then-current main; do not merge mechanically.
Publication/testing does not complete #949 or establish a release requirement
beyond the issue decision. Numerical fixture capacities/SQLite/ordinal windows
are not prescribed production mechanisms.

## Lineage and audited source

Sole revision parent: `705cd8c79e48c190fedece4105f2f6e9345994c7`.
Underlying mapped source base: `6f4099700834e411d7b7b342c43c077fcc83e55e`.
Current-main re-audit: `e0036f80410a83cc5eaa9d63c17650ffae72e2bd`.
Audited main tree: `b109c466311b6bc54cfd0cf5cb08a2da55cffff1`.

This revision appends to the existing reference, preserving prior history rather
than rebasing, merging main or force-pushing. The new exact published SHA recorded
in #949 supersedes the predecessor as preferred reference input, not historical
provenance. The original `78415d14ae5e5556a4834267cf93053bb6af66f3` and its
`224b3239c8c82be0dbb6730675ca6c09f23e782f` mapping remain governed by #950.

A full pinned clone was obtained through the read-only Git clone gateway. Download
SHA-256: `90a5458996283ea6c1ba6f48c64a0939aedd48aa3fa1327be52c682870581f93`.
The digest and audited HEAD/tree were verified. The production checkout on this
reference remains older than re-audited main. `source_audit_v3.json` records
19 selected comparisons (12 equal, 7 changed), not whole-product compatibility.

## Contents

Entry points: `.reference/949/INTEGRATION.md` and `EXTENT_AND_BACKPRESSURE.md`.
New body-admission and SQLite work-budget models each have 23 tests. The original
four Python semantic/lease files and their 77 tests remain byte-for-byte unchanged.
`audit_body_gate.py` executes one exact pinned function with dependency doubles;
`source_audit_v3.json` and `validation_v3.json` preserve source/result evidence.
The preceding source audit remains intact.

## Validation actually performed

- Read issue scope/decisions, #903 convention and current repository guidance;
  inspected source, extent/control declarations and relevant tests at audited main.
- Consulted official HTTP, Uvicorn and NGINX sources linked in the technical guide.
- Ran the reference unittest discovery with Python 3.13.5 / SQLite 3.46.1:
  **123 tests passed, zero failures/errors/skips**, elapsed **1.121309 seconds**.
- Transfer work at 64/128/256 units: 16384/32768/65536 source bytes;
  counts of source reads/receipt lookups/admissions/effects scale linearly.
  Peak modeled reservation stayed 128 bytes, not a process-RSS measurement.
- Due selection processed four rows with 170 SQLite VM steps at 32/512/2048
  retained rows. This does not measure every B-tree comparison/page operation.
- New 1,000-consumer retirement/churn test retains at most one exact record in
  that sequential workload and rejects retired replay.
- Pinned route-body characterization observed 32 body bytes consumed before a
  storage-unavailable outcome. This confirms a CURRENT GAP, not gate conformance.
- Compiled the reference Python files. Whitespace/conflict-marker and staged diff
  checks, original-file identity and root-tree publication checks are recorded
  in the owning issue after verification.

Timings are observations, not machine-independent guarantees or performance gates.
No matching measured performance baseline is asserted.

## Not validated / limitations

The new witnesses are small constructions, not production algorithms. Payload
handling is a counted/hash fixture, not encryption/provider durability; the budget
models reservations rather than full-process allocation. SQLite transactions and
close/reopen are not distributed concurrency, paused-worker fencing, process-kill,
real storage recovery or indefinite-WAL-growth proofs.

A focused current storage-protocol package-test collection was attempted with
local source paths. It exited 2 because `rfc8785` is unavailable; no product test
body pass is claimed (reported collection elapsed: 2.62 seconds).
Full repository lint/unit/dist-smoke/build/linux-qualification were not run.
Live HTTP/ASGI/proxy behavior, real aggregate memory, ciphertext-cache effects,
adapter conformance, devices, webhooks and production growth contracts were not
validated. Actual integration-SHA validation remains necessary.

The current publishing interface has no native issue-branch Development-link
mutation. Record that limitation in #949 under #903. Actual branch tip/parent/tree,
changed-file identities and visible Actions/check/status results are publication
evidence to record after publication, not predicted by this file.
