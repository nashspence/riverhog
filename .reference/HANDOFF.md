# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#991](https://github.com/nashspence/riverhog/issues/991), under [#903](https://github.com/nashspence/riverhog/issues/903). Request: [6884822328](https://github.com/nashspence/riverhog/issues/991#issuecomment-6884822328).

Audited base: `codex/964-session-owner` / PR #979 at **`67b4ccd9419be86a60dece5c5c16bee43f7ce935`**, the exact checkpoint specified by the integration agent.

Produced by: OpenAI ChatGPT. Model: GPT-6 Astra Pro. Configuration: Default mode with parallel implementation, test and evidence review. Requested by: Riverhog integration agent, relayed by nashspence. Published through: connected GitHub application on the maintainer's behalf.

## Purpose and first receiving step

The two-file helper/test proposal replaces whole-snapshot decoding and per-group deep copies with one immutable row encoding per admitted declaration and fresh decoding of every complete reachable private input. Reader semantics, source/operation authority, full guards, exact seals, poisoning and release remain intact. Retained metadata remains linear in the admitted closure plus the existing pending-root state.

**First step:** reconcile `tests/support/growth_batch.py` and `tests/unit/test_growth_observer_batch.py` against the receiving source, then validate the accepted helper/test change through the normal integration rail. The focused reference selection uses both observer modules with the two actual caller fixtures explicitly deselected. Full subsequent qualification remains with the integration owner.

The exact clean implementation tested was **`7d2aebb21891bced44a703753040f91d9a351f0c`** (tree `f7db5c9f64f7a878339dea94761fe024c33ee990`). The final evidence commit changes only `.reference/` from that implementation. Its exact SHA is recorded in the owning #991 comment. Reader SHA-256 remains `a6fa11e73a3886dbf950bba34e310ab27857c5e87af6ca77bb6cad756a059f40`.

## Findings and evidence

**82 selected tests passed**, with both actual caller fixture cases excluded; Ruff, formatting, REUSE and mypy passed using pinned tools. One uninstrumented retained helper pair observed **14.281960105 → 11.883021960 seconds wall** and **14.222222806 → 11.858247503 seconds CPU**. A separate diagnostic pair preserved all **122 independent roots, 1,078 total nodes, 46 private groups, 47 fresh compilations, 560,860 unit visits, 1,923 canonical encodings and 1,922 SHA constructions**. All private input bytes/order matched. Deep copies changed from 46 to zero; no reader work was removed.

See [RESULTS.md](RESULTS.md) for the measured comparison, remaining cost, adversarial coverage, source relationship, original process/cleanup receipts, and reproduction inputs. Encoded payload counts are separate from memory; no peak-memory or allocator measurement is claimed. These are single paired observations, not a performance gate or production throughput result.

## Relationship and limits

This starts directly from the audited receiving checkpoint and is a new focused follow-up to helper reference `5e21cc9c75c86ec08ab4b1c755d98c36bd4e5195`, whose helper is byte-identical to the baseline. It changes that helper's private projection representation. It does not supersede the already consumed lookup reference `0e9bef9445213aadadb7218fc6e268cc324f35f3` or numeric reference `086dfaabe204d056afe8f2e4845fc42d8af6da65`; both accepted changes and their tests are unchanged in the audited base. Prior reference history is preserved.

Only the helper and focused owner tests contain executable edits. The reader, canonical implementation, actual caller/product paths and dependencies remain unchanged. The replay creates no new product operation, archive fixture, actual caller turn, journal advance or publication; its Native pause scope is empty. Broad unit/full CI/merge qualification, the incomplete census, held 1,000/10,000/100,000 campaigns, Native publication cost and parallelism are outside this reference. No whole-path cost clearance is inferred.

## Authority

This is externally produced reference material for #991. Its creation, testing, publication, request or issue link does not establish or extend an accepted design, contract, release requirement or authorization to integrate. Authority remains with the owning issue, subsequent maintainer decisions and the normal integration rail. Reconcile against then-current source and decisions. The exact final reference commit linked in #991 is the durable handoff identity; the branch name is navigation. No native Development association is requested or expected.
