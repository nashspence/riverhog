# NON-AUTHORITATIVE EXTERNAL REFERENCE

Issue: [#989](https://github.com/nashspence/riverhog/issues/989).
Convention: [#903](https://github.com/nashspence/riverhog/issues/903).
Branch: `reference/external/989-async-retirement-growth`.
Audited base: `codex/964-session-owner@dd09f4ab5e824935e680f26fba3d73d2484cb426`.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro.
Configuration/effort: no additional identity attested.
Requested by: Riverhog maintainer, for the integration agent.
Published through: the connected GitHub integration.

## Start here

Read [ASYNC_RETIREMENT.md](ASYNC_RETIREMENT.md). The proposal keeps the existing
asynchronous execution and custody protocol. It replaces the obsolete inline
proof with separate scheduling, original-worker and receiving scopes. The return
count is bookkeeping: joined outcomes + withdrawn reservations + close retries
submitted + new work submitted. Its phase-derived bound is `4 * max(0, limit)`;
it is not confirmed file progress or a bound on every delegated cost.

**First integration slice:** in a separate authoritative worktree, run the
maintained 17-byte/three-byte witness. First scheduling returns one; after joining
the exact original worker, its actor is received while its ordinary charge stays
retained; the next limit-one call returns three. Validate scheduling and worker
samples separately and link them by original context/reservation/worker/actor.
Then run the preserved full matrix and the attainable-four-event witness.

## Concrete material and source relationship

[PATCH.json](PATCH.json) pins before/after hashes for nine proposed product/test
paths. [apply.py](apply.py) applies the three small existing-source edits and the
six readable replacements under `changes/` only after all hashes are checked.
It rejects drift. The branch's product paths remain byte-identical to the audited
base; changes are supplied as an executable source patch, not integrated source.

In a disposable checkout of this reference:

```sh
python .reference/apply.py
mise x python uv -- uv run --locked --all-packages --group dev python .reference/run_witness.py
```

The branch-only workflow runs the same patch experiment, scoped lint/types and
maintained owner witnesses on native Linux/macOS. Its exact commit, applied-source
manifest and actual outcome must agree. YAML alone is not evidence. Current final
results and artifact digests are recorded in #989, not guessed here.

[RESEARCH.md](RESEARCH.md) identifies primary sources and owner anchors.
[QUALIFICATION.md](QUALIFICATION.md) maps the retained scenarios and remaining
integration proof. [VALIDATION.md](VALIDATION.md) distinguishes scratch checks,
locked workflow evidence and limitations.

This initial #989 handoff does not supersede #987/#988 receiving semantics or the
broader #964 reference. A preceding tooling-only commit remains in branch history.
No integration/main source, existing workflow or dependency lock is changed.
Authority remains with #989, maintainer decisions and the normal integration rail.
Do not merge this reference mechanically. Its published SHA recorded in #989 is
the handoff identity; no native Development association is required. Record finite
adoption/reproduction/disposition and remaining qualification in #989. Publication
neither activates maintenance nor authorizes merge/release/provider work.
