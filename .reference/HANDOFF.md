# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: nashspence/riverhog#948
Convention: nashspence/riverhog#903
Reference branch: reference/external/948-extension-execution
Original audited base: main @ 239985271c9ec24942073f4005dd18da621d0251
Original base tree: 3847c6b32acf67c8e96518b30a5bfcee7b663911
Prior published reference: 6d5e3985fb9c781052594641773ba36ea1e05b30
Latest main reconciled: 88a3b3ae7d0addec10ced70b9eb04c7f96e0332f
Latest main tree: cc681ea890d956ab2fccb45fa0da61e33d12d29d
Revised: 2026-10-02

Produced and revised by: OpenAI ChatGPT
Model: GPT-6 Astra Pro (user-visible identity)
Configuration / effort: not exposed; no hidden backend configuration asserted
Requested by: repository maintainer, for an integration agent to consume
Published by: OpenAI ChatGPT via the GitHub Connector
Snapshot retrieval: GitHub (Additional Tools)

## Purpose and contents

This is a reviewed design and executable lifecycle experiment, not a production
implementation or issue completion. Start with [948/DESIGN.md](948/DESIGN.md), then
[948/INTEGRATION.md](948/INTEGRATION.md). [948/VALIDATION.md](948/VALIDATION.md) records
snapshot verification, checks actually run and remaining production evidence.
`948/lifecycle.py` and `948/test_lifecycle.py` are a single-owner SQLite fixture,
not reusable production runtime code.

This revision tightens the minimum v1 contract, covers the work-creation preview
revalidation path, preserves cancellation through restart, checks grant ownership,
and distinguishes resource-lease validity from already-proven completion.
It supersedes the prior reference as the recommended integration-agent handoff.
The new exact published SHA is recorded in #948; the branch name is navigation.

## Provenance correction

The original supplied `issue-948-reference-package.zip` identifies its producer as
OpenAI ChatGPT, GPT-6 Astra Pro (user-visible identity). The prior publication changed
that field to GPT-5.6 Sol. This revision restores the supplied producer identity;
it does not infer a hidden backend identity from either label. The original design,
integration map, model and tests matched the published files byte-for-byte. The
handoff and validation files had publication edits, so the earlier blanket statement
that all six published blobs matched the supplied package was too broad.

## Authority and revision history

This branch is externally produced reference material. Its creation, validation,
publication, native linkage or request by a maintainer/authorized agent does not
establish or extend accepted design, contracts, release requirements or permission
to integrate. Authority remains with #948, subsequent maintainer decisions and the
normal repository integration rail. Earlier conversation is intent calibration.
Do not apply the reference mechanically or land `.reference/` as product docs.

The integration agent is the intended consumer, not this material's producer.
This revision appends to the published reference history; it does not rebase,
force-push, merge current production changes, alter main/release branches, or change
generated contracts. The historical branch base remains distinct from the newer
main snapshot used for reconciliation. All 328 Stove0 files matched across snapshots.

## Validation and limits

Both full snapshots were downloaded and verified by archive/manifest SHA-256,
per-file SHA-256/Git blob and reconstructed Git tree. The original 28 model tests
passed locally. Eight new tests were added; the revised 36-test suite passed,
including ten repeat runs, and Python compilation passed. See VALIDATION.md for
precise scope. Real HTTP, NVENC, broker leases, PostgreSQL/concurrency, process
containment and repository-wide production qualification were not performed.

Full source snapshots are now available; the earlier source-download limitation is
historical, not a current explanation for unrun production tests. Reference-only
changes are not proof of production readiness. Native Development linkage remains
unestablished by this publisher: the available Connector has no corresponding write
action. That limitation and the new exact reference SHA are recorded in #948 per
#903. A Markdown URL or closing keyword is not a substitute. No PR or issue-closing
action is part of this handoff.
