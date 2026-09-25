# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: #870 — durable catalog following and external witnesses
Convention: #903 — non-authoritative external reference branches
Audited base: `main` @ `ba93b8bb3483f6a18b5158c21ddbdc8d8e0d710c`
Base tree: `eeedc0b8167a2b2d73f23af74e976745caea87e0`
Reference branch: `reference/external/870-ots-proof-lifecycle`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: no separately exposed effort configuration
Requested by: Riverhog maintainer, relaying the session 3 integration agent's request
Published by: OpenAI ChatGPT through the connected GitHub publication tools

## Purpose and contents

Contained OpenTimestamps lifecycle input for session 3. `.reference/ots/` accepts a
caller-owned SHA-256 witness statement digest, blinds calendar submission with a
persisted caller-supplied nonce, retains standard detached proof bytes, schedules
bounded pending-subtree maturation, verifies Bitcoin attestations against a trusted
validating node's active mainnet view, and evaluates optional confirmation policy
separately. An explicitly initialized SQLite example demonstrates restarts, replay,
compare-and-swap fencing, and atomic retention of proof/progress and prior proofs.

No catalog traversal experiment, witness statement contract, Riverhog core change,
client export, application CLI/service, publication coordinate, or workspace/lock
change. The one additional workflow is restricted to this reference branch and
runs its isolated tests; it is not a release or application workflow.

## Authority

This branch is externally produced reference material for the owning issue.
Its creation, testing, publication, native issue linkage, or request by an
authorized repository agent does not itself establish or extend an accepted
design, contract, release requirement, or authorization to integrate these changes.
Authority remains with #870, subsequent maintainer decisions, and the repository's
normal integration rail. Reconcile against then-current main and issue decisions;
do not apply mechanically. The exact published reference SHA recorded in #870 is
the durable handoff identity, not this branch name. No merge request or closing
keyword is used.

## Validation performed at initial publication

- Reviewed #903, accepted #870 scope, exact main/base tree, AGENTS.md, README.md,
  architecture, licensing, workspace/test settings, and CI trigger boundaries.
- Inspected upstream OTS timestamp, operation, serialization, attestation, and
  calendar implementations and official Bitcoin Core RPC documentation.
- Local Python 3.13 compilation of all reference Python sources passed.
- Independently assembled the genesis transaction/header/proof vector with
  standard-library hashing; checked the block hash and Merkle root against
  Bitcoin Core's published genesis constants.
- Focused dependency-backed tests: pending the reference branch workflow at this
  initial publication. Do not read this record as a passing-test claim. Exact
  results, including any follow-up revisions, belong in the final #870 handoff.

## Not validated / known limitations

- Local package installation and git clone could not reach external hosts in the
  producer container. No local dependency-backed test result is claimed here.
- Full repository make lint/unit/dist-smoke/build and mainline qualification were
  not run. This is not an application integration or release qualification.
- No public calendar was contacted, no witness digest was submitted externally,
  and no live Bitcoin node was contacted. Transport authentication, TLS, DNS/
  redirect policy, response/time limits, and RPC error mapping remain adapter work.
- The SQLite schema is an illustrative application-owned reference, not a migration
  proposal or an adopted durable-state/public client contract. Real crash/power-loss,
  deployment filesystems, worker leases and multi-host behavior remain unqualified.
- Bitcoin consensus/active-chain authority comes from a trusted fully validating
  mainnet node. A raw header hash or synthetic test port alone cannot establish it.
- Confirmation results are point-in-time observations, not sticky finality or exact
  document creation times. The integration application owns rechecks and retirement.
- Dependency installation/licensing and publication coordinates require normal
  integration/release review; no third-party library is vendored in this branch.
- The available publisher exposes no native issue/branch Development-link mutation.
  A textual reference is not a native link. This limitation must also be recorded in
  #870; a capable publisher may attach the branch later without changing provenance.
