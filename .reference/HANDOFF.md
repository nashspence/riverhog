# NON-AUTHORITATIVE EXTERNAL REFERENCE — #949 revision 2

Owning issue: https://github.com/nashspence/riverhog/issues/949
Convention: https://github.com/nashspence/riverhog/issues/903
Decision record: https://github.com/nashspence/riverhog/issues/949#issuecomment-5981858530
Reference branch: `reference/external/949-removable-media-foundations`
Date: 2026-10-04

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro (user-visible identity)
Configuration: no separate effort/mode setting exposed for this handoff
Requested by: repository maintainer, following integration-agent feedback
Published through: the connected GitHub API on the requester's behalf

## Authority and purpose

The maintainer explicitly requested this issue/reference update after identifying
prior conversation as intent calibration, not necessarily authoritative. The linked
issue comment consolidates the direction now authorized. This reference supplies
executable input; it does not itself select public types/endpoints, amend release
requirements, or authorize integration. Authority remains with #949, subsequent
maintainer decisions and the normal integration rail. Reconcile against then-current
main; do not merge this branch mechanically or use it as an alternate implementation.

Riverhog remains generic: opaque objects, external-effect outcomes, consumption
ownership, finite renewal/release and incremental progress. Physical media, device
identity, topology and operator/webhook handling remain adapter/deployment-owned.
No removable-media adapter, production contract or production implementation is added.

## Lineage and reconciliation

Revision parent: `224b3239c8c82be0dbb6730675ca6c09f23e782f`.
This is an append-only update to the existing mapped reference, not a force-push,
rebase or merge of current main. The exact published revision SHA is recorded in #949
and is the new preferred handoff identity; branch names are navigation only.

Original reference: `78415d14ae5e5556a4834267cf93053bb6af66f3`, originally based on
`ec544b60dde572ac3bc2f31b199ab5b3727e4548`. #950's maintainer-authorized history
rebaseline mapped the reference to the revision parent above and preserved the
original provenance in its recovery assets. The predecessor's handoff record remains
in history; this revision supersedes preferred input, not that historical provenance.

Audited current main: `29d16e99d52bda0f308495fd7be519c1e321b735`.
Audited main tree: `2aa3a5547b11dc21e79825755888210075657faf`.
`949/source_audit.json` records the exact mapped source base and 15 identical source/
test blob pairs between the predecessor and audited main. The branch's production
checkout intentionally remains at its old source base. Begin real implementation on
then-current authoritative main, including its changed release/dependency tooling.

## Contents

- `949/INTEGRATION.md`: current entry point, issue linkage, assumptions, source-oriented
  integration map, primary-source research and remaining real proof obligations.
- `949/lease_model.py` and `949/test_lease_model.py`: separate finite-lease/bounded-replay
  experiment, with no product imports or third-party dependencies.
- `949/source_audit.json`: machine-readable Git blob reconciliation evidence.
- `949/README.md`: revised entry point and repinned source map.
- Original `949/semantic_model.py` and `949/test_semantic_model.py`: unchanged.

Only `.reference/` files change. No main/release ref, production source/schema,
migration, generated contract, dependency declaration or workflow is changed by this
publication. No pull request is part of the handoff.

## Validation performed

- Read current #949/#903, history-mapping record and relevant repository guidance.
- Obtained the full commit-pinned checkout through the read-only Git clone gateway;
  verified archive SHA-256 `2c84f883589b25cf017bfea4fcff5435a7fd88c410126875fd898510b338c4af`.
  Restored executable file modes lost by ZIP extraction; confirmed a clean starting tree.
- Verified 15 relevant source/test Git blob pairs against audited current main.
- Ran `python3 -m unittest discover -s .reference/949 -p 'test_*.py' -v` with Python
  3.13.5: **77 tests passed**, including **324 specified serial schedules** and the
  1,000-consumer churn/late-replay witness. Initial 36 tests remain unchanged.
- Compiled all four Python files and checked whitespace/conflict markers and diff scope.
- Consulted the primary sources recorded in `949/INTEGRATION.md`.

The actual published commit, parent, changed-tree/blob verification and visible CI
status are recorded in #949 after publication. No local producer commit SHA is claimed.

## Not validated / limitations

- These are serial semantic witnesses, not real concurrency, database crash recovery,
  transport, authentication, encryption, worker fencing or physical-I/O proofs.
- The watermark window is one bounded construction, not a mandated public design.
  Durable ordinal allocation, gap settlement, authenticated namespace lifecycle,
  failover/clock authority, and real stream drain/fencing remain integration work.
  Capacity exhaustion/head-of-line blocking is exposed, not silently evicted away.
- No actual storage/provider cleanup, filesystem flush/eject, outbox/HA deployment,
  arbitrary-capacity staging or single-slot cross-target transfer was exercised.
- Existing storage-protocol/support tests were attempted but stopped at collection
  because `rfc8785` was unavailable. Installing locked `rfc8785==0.1.4` into an isolated
  directory failed due to pypi.org DNS resolution. No product-suite pass is claimed.
- Full repository lint/unit/dist-smoke/build, real adapter conformance and required
  integration-SHA CI remain outstanding. Reference model results cannot substitute.
- The GitHub connector exposes no native issue-branch Development-link mutation.
  This continuing limitation must be recorded in #949; a capable publisher can add
  the relationship without changing this reference commit or its provenance.
