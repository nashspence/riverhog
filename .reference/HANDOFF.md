# NON-AUTHORITATIVE EXTERNAL REFERENCE

**Bounded Core stepping** — response to [#991 comment 6887528297](https://github.com/nashspence/riverhog/issues/991#issuecomment-6887528297), under the [#903 convention](https://github.com/nashspence/riverhog/issues/903). Owning issue: [#991](https://github.com/nashspence/riverhog/issues/991).

**Read [DESIGN.md](DESIGN.md).** It gives the recommendation, explicit admission/composition model, freshness counterexample, adoption/recovery rules, and decisions that belong to the integration owner.

| Record | Value |
| --- | --- |
| Audited base and sole parent | `codex/964-session-owner` / PR #979 at **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e** |
| Reference branch | `reference/external/991-core-stepping` |
| Producer | OpenAI ChatGPT |
| Exposed model | GPT-6 Astra Pro |
| Configuration | Default mode; parallel source/lifetime/growth analysis and independent review |
| Requester | Riverhog integration agent, relayed by nashspence |
| Publisher | ChatGPT's connected GitHub tools |

## Recommendation and first receiving step

**Leave the current one-step runtime in place.** A bare inner Native loop would omit fresh Core checks of retained preparation rows and captured-source integrity. Deferring catalog adoption also changes capacity, conflict, error and recovery behavior. Monotonic progress publication does not prove those changes safe.

If the owner chooses to develop grouping, the narrower candidate is an explicitly admitted Core envelope over complete existing units, retaining every readiness check, unchanged Native steps/commits and catalog adoption after each owner call. Its possible initial saving is intermediate outer stamps. A larger quantum still needs an explicit scheduling/cancellation/finalization boundary and new composed accounting. The proposed default remains one attempt; two is only an illustrative reservation, not an accepted setting.

**First focused receiving step:** review DESIGN's counterexamples and decision section before adding a loop. Decide whether the limited stamp-only candidate is worth its new coordination proof, or retain one-step behavior while a preparation owner designs an equivalent fresh readiness validator. No new runtime patch is recommended for immediate adoption, and no broad diagnostic rerun is requested.

## Source relationship and validation

All paths outside `.reference/` exactly equal **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**. Its delta from 3b47 is only the Native prefix-reader refinement and nine owning corruption cases. The fresh row's parsed prefix is reused within that observation; publication retains its independent parse. Core and the surrounding callers/declarations are unchanged.

This is a new design scope following phase reference **8b639901c4dbe157dce904acec718a23f580c6ca**. It neither replaces that historical observation nor claims its timings describe the refined source. The later receiving comment [6889181049](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889181049) supersedes the request's pending-CI status and reports d4d7 fully qualified; its qualification and exact-input parity are attributed to that owner, not rerun here.

Performed: exact Git delta/tree/file/dependency audit; inspection of source-owned growth declarations, primitives, scheduler/lifetime behavior and existing corruption/recovery tests; independent cross-review; packet link/hash/source verification. The packet includes explicit current/proposed multiplicities and a 20-case focused receiving matrix.

**No product calls, project imports, test collection/execution, diagnostic workload, archive publication or benchmark ran.** The static counterexamples establish the present proof gaps; a tiny successful run could not resolve them. A conditional two-minute observation outline is included only for a later concrete boundary decision. No throughput, new CI qualification, whole-path G10/G12/G13 closure or timing gate is claimed. Large campaigns remain held under the request.

## Authority

This is externally produced reference material. Its request, publication, evidence or issue link does not establish or extend an accepted design, contract, release requirement or integration authority. Reconcile it with current source and owning-issue decisions through Riverhog's normal integration rail. The exact published commit recorded in #991 is the durable identity; the branch is navigation. No native Development association or closing action is requested.
