# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#990](https://github.com/nashspence/riverhog/issues/990)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/990-recovery-design`

Audited base: `codex/964-session-owner` on PR #979 at
`f7bf8b2b6644d80bf2edae4414c3d18574e511c8`, the exact source established by #990.
Local experiment checkpoint: `cad153b519dd4a7a17dba210b30cd84d12d3cafa`.
The final published SHA recorded in #990 is the durable handoff identity.
Publication reconstructs the supplied tree through the connected GitHub API;
the published tree is verified identical and its parent is the audited base.
The local experiment SHA is provenance, not a claim that GitHub published it.

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration: Default collaboration mode
Requested by: repository maintainer in this ChatGPT conversation, for the integration agent
Published by: the same producer through the maintainer's repository connection

## Purpose and first step

Propose bounded, lossy `last_used_at` observation after a fresh authoritative
authentication snapshot. One writer attempt, no queue/retry/cache, unchanged
authorization and archive-worker fences. This is a policy proposal for explicit
acceptance in #990, not an inferred authorization to change production behavior.

**First verification:** run the held-writer command in [VALIDATION.md](VALIDATION.md)
on this clean reference checkout. It must authenticate the valid token while the
original SQLite writer is still held, preserve denials, then receive that exact
writer and record usage after release. Reconcile the owner diff and policy with
the current #990 decisions and PR #979 before intentional integration.

Read [DESIGN.md](DESIGN.md) for the policy, error boundary, exact ownership and
work bounds. [VALIDATION.md](VALIDATION.md) identifies actual evidence and its
limits; `.reference/experiments/` contains the reproducible witnesses.

## Source relationship and validation

Product paths contain proposed edits to the app-key service and API descriptions;
one maintained test module is added. All subsequent changes after the experiment
checkpoint are handoff/evidence files within `.reference/`. Product and test
contents match the experiment checkpoint. There is no superseded published
handoff. This is not evidence that current `main` or an integration merge was tested.

Validation performed: 83 focused cases passed with zero skips, including all
three reported CI failures; exact-base/candidate held-writer witnesses; pinned
full lint; and the real 16-member publication with all three Native reader
passes and helper cleanup confirmed. See VALIDATION.md for receipts and the
remaining end-to-end performance concern. Timings are observations, not gates.

Not validated: live PostgreSQL contention, complete exact-source/tested-merge CI,
1,000-member arms, the preserved 128-file profile, platform/installed/provider
qualification, release or activation. Writer admission is no-wait; pool checkout,
connection setup, reads and storage/commit I/O have no new wall-clock deadline.

## Authority

This branch is externally produced reference input. Its creation, testing,
publication, issue reference, or requester does not establish or extend an
accepted design, contract, release requirement, or authorization to integrate.
Authority remains with #990, subsequent maintainer decisions, and the normal
integration rail. Reconcile against current authoritative state; do not apply
mechanically. The branch URL and exact reference SHA in #990 provide the handoff
record; no native Development association or closing mechanism is requested.
