# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#989](https://github.com/nashspence/riverhog/issues/989)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Branch: `reference/external/989-asynchronous-retirement-growth`
Audited base: `codex/964-session-owner@dd09f4ab5e824935e680f26fba3d73d2484cb426`

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
No additional configuration/effort identity is attested.
Requested by: Riverhog maintainer, for the integration agent
Published through: the connected GitHub integration

## Current entrypoint

This initial reference selects three separately measured scopes: coordination,
original worker execution, and original receiving. For positive limit L,
advance returns at most 4L bookkeeping events, not confirmed file progress:
at most L physical completions, L ordinary reservation withdrawals, L close-only
resubmissions and L new submissions. File/catalog progress stays in the exact
original worker's outcome and child samples.

The source-owned declarations are in [journal_retirement_growth.py](journal_retirement_growth.py).
The opt-in [apply.py](apply.py) checks the exact audited product bytes before
applying the concrete candidate in a disposable authoritative worktree. Product
paths in this reference tree remain unchanged. The overlay keeps native file
work and blocker formatting on original workers, moves exception-frame receiving
off collection, and preserves separate resource, worker-join and grant predicates.

**First useful slice:** run [reproduce.py](reproduce.py) against the unchanged base,
then apply the candidate and execute the real 17-byte, three-byte-budget witness
in [test_phase_edges.py](test_phase_edges.py). It must distinguish the original
1/3 scheduling returns from actual bytes removed and ordinary retention release.
The separate four-event witness exercises the complete phase-derived envelope.

[test_retirement_loop.py](test_retirement_loop.py) preserves the existing catalog,
control width, epoch, cold/warm, file and admission regimes. Its event gates only
hold actual physical workers; they never replace workers with inline execution.
[async_retirement_growth.py](async_retirement_growth.py) observes existing owner
primitives and composes each root through the existing growth validator. Worker
roots do not become children of a completed scheduling call.

## Validation status at initial publication

Local syntax checks passed. No runtime test pass is yet claimed for this commit.
A branch-scoped Linux/macOS workflow uses the repository's locked runtime to run
the unchanged-source reproduction and explicit candidate overlay. The owning
issue's final handoff record will identify actual exact-SHA results, failures and
remaining limits. A workflow declaration is not evidence that it ran.

This is the initial #989 reference, not a revision of #987/#988 resource authority.
No main or integration branch is modified; normal runtime activation remains off.
The 1,000-member and 128-file lifecycle campaigns remain #964 integration work.

Authority remains with #989, current maintainer decisions and the normal
integration rail. Do not mechanically merge this reference or treat publication
as acceptance. The exact published SHA recorded in #989 is the handoff identity;
no native Development association is requested or expected.
