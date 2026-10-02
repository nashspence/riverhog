# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: nashspence/riverhog#948
Convention: nashspence/riverhog#903
Audited base: main @ 239985271c9ec24942073f4005dd18da621d0251
Audited base tree: 3847c6b32acf67c8e96518b30a5bfcee7b663911
Reference branch: reference/external/948-extension-execution
Prepared: 2026-10-02

Produced by: OpenAI ChatGPT
Model: GPT-5.6 Sol
Configuration / effort setting: not exposed; no hidden backend configuration asserted
Requested by: repository maintainer in the current conversation, for an integration agent
Published by: OpenAI ChatGPT via the GitHub Connector

## Purpose and contents

A source-audited proposal for durable, externally admitted component execution with
bounded Stove0 control operations. This is a design plus executable lifecycle model,
not a production implementation or a claim of issue completion.

Start with [948/DESIGN.md](948/DESIGN.md), then
[948/INTEGRATION.md](948/INTEGRATION.md). The latter has pinned source anchors,
proposed integration sequencing and the boundary between model tests and outstanding
production evidence. [948/lifecycle.py](948/lifecycle.py) and
[948/test_lifecycle.py](948/test_lifecycle.py) model critical interleavings with SQLite
and fake owner-specific permits. [948/VALIDATION.md](948/VALIDATION.md) records checks.

Only `.reference/` files are added. No production source, state baseline, workflow,
release branch, generated contract candidate, issue body, or integration branch is
changed. Earlier conversation is intent calibration, not a second source of authority.

## Authority and provenance

This branch is externally produced reference material for #948. Its creation,
testing, publication, native issue linkage, or request by an authorized repository
agent does not establish or extend an accepted design, contract, release requirement,
or authorization to integrate these changes. Authority remains with the owning
issue, subsequent maintainer decisions, and the repository's normal integration rail.
Reconcile against then-current authoritative state and issue decisions; do not apply
mechanically. The integration agent is the intended consumer, not this material's
producer and not the requester asserted by this handoff.

No pre-publication producer commit SHA is asserted. This handoff is intended to be
published on the exact audited parent named above. The actual published reference
SHA is recorded in #948 after publication; that SHA is the durable handoff identity
and the branch name is navigation. Do not force-push away published reference
history.

## Validation performed

Read #903, its process-correction comment, #948, AGENTS.md, README.md, and the pinned
source anchors listed in INTEGRATION.md. Ran 28 standalone design-model tests,
compiled both Python files, repeated the model suite, and checked reference text for
whitespace errors. A local patch-application round trip verifies all six file blobs.
Remote publication/tree verification and any GitHub check state are recorded in
#948 after publication rather than self-referentially in this commit.

## Not validated / known limitations

No production change is implemented here. No full Riverhog checkout/dependency
installation was available: direct public git clone failed because github.com could
not resolve in the execution container; source audit used the connected GitHub reader.
`make lint`, `make unit`, `make dist-smoke`, `make build`, PostgreSQL integration,
HTTP concurrency, NVENC hardware, real scheduler leases, process crash/power-loss,
and capability/publication qualification were not run. Model tests are not substitute
evidence for those checks. Known design tradeoffs and integration-only evidence are
explicit in the accompanying files.

The available GitHub publication actions expose branch/file/commit operations but
no native issue Development-link operation. Native linkage is therefore absent from
this publication and is recorded explicitly in #948 as permitted by #903. A Markdown
URL or closing keyword is not a substitute. No pull request or issue-closing action
is part of this reference publication.
