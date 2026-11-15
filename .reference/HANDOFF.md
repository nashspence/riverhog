# NON-AUTHORITATIVE EXTERNAL REFERENCE

**Current reading entrypoint:** [Native transition and commit granularity](DESIGN.md).

- **Owning issue:** [#991](https://github.com/nashspence/riverhog/issues/991), responding to [comment 6890881026](https://github.com/nashspence/riverhog/issues/991#issuecomment-6890881026).
- **Convention:** [#903](https://github.com/nashspence/riverhog/issues/903).
- **Reference branch:** [`reference/external/991-native-granularity`](https://github.com/nashspence/riverhog/tree/reference/external/991-native-granularity). The exact published SHA recorded in #991 is the durable handoff identity; this branch URL is navigation.
- **Audited base:** `codex/964-session-owner` / PR #979 at **d4d7b6535be2cf491b7859cacea2a9ada4ac8d9e**, sole parent. Base tree: **98cf298e60e4ba9c178229eae907e164265f6e48**.
- **Produced by:** OpenAI ChatGPT. **Exposed model:** GPT-6 Astra Pro. **Configuration:** Default mode; parallel static protocol, lossless inventory/growth and independent source/test review, with primary-agent reconciliation.
- **Requested by:** Riverhog integration agent, relayed by nashspence. **Published by:** ChatGPT's connected GitHub tools.

## Purpose and first receiving step

Assess one narrow candidate: combine the final `SemanticFinish` child application with its subsequent parent-only promotion to `recording`, returning immediately. The candidate concerns five historical promotion calls and would remove none of the 101 retained Finish prepares.

**Result:** the direct combination changes a real fresh rejection boundary. Deleting or replacing the completed child checkpoint after the first commit is rejected on the current next call; the proposed `recording` dispatch would omit that child check. A Native completed-child discharge contract and a Core observation/adoption mapping are missing. This reference stops at that specific counterexample and contract gap. It supplies no implementation or permission to remove commits.

**First focused receiving step:** reconcile the initial-history raw ordinal pair 100/101 with the d4d7 child-load, parent-promotion and recording dispatch, and record disposition of this specific-counterexample result. The existing boundary and [one-step Core decision](https://github.com/nashspence/riverhog/issues/991#issuecomment-6889902338) remain controlling; any new completion-discharge protocol belongs to a separate owning decision.

## Source and evidence relationship

All product paths equal d4d7; changes are confined to `.reference/`. No product patch is embedded. The historical lossless trace was executed on **3b47cbba770c31eb1487b50f7e3b16533c10e827** and published in **8b639901c4dbe157dce904acec718a23f580c6ca**. The exact two-file prefix-decode delta to d4d7 was inspected; Native phase implementations are unchanged. Historical counts are reanalyzed observations, not a new d4d7 execution or timing result.

This is a new focused response to 6890881026, following occupancy reference **bcb8de725c1958e685fbc4afc417f32bedaece13**. It does not supersede that reference, the historical phase reference, or Core-stepping reference **46ace062078d08cd70f508a3f3a7d834a92cbb40** and its recorded disposition.

## Actual validation and limits

Performed static source/test inspection, exact source-delta and source-pin checks, byte-for-byte reconciliation of published historical raw, offline control/call extraction, independent counterexample review and packet/manifest verification. [EVIDENCE.md](EVIDENCE.md) records methods, attribution, hashes and the machine-readable receipts.

No product imports, tests, fixtures, new fault injection, CI, new runtime diagnostic, caller census, publication/completion run or benchmark was performed. The new complete/partial function-CPU diagnostic figures and d4d7 qualification are attributed to the receiving owner; their raw diagnostic bytes were not supplied for verification. No speedup, avoidable-commit count, fsync attribution or whole-path growth clearance is established.

## Authority

This is externally produced reference material for #991. Its creation, verification, publication, issue reference or request does not establish or extend an accepted design, contract, release requirement or authorization to integrate changes. Scope and decisions remain with the owning issue and the repository's normal integration rail. Reconcile against then-current authoritative source and decisions before any use. No native Development association is requested or expected; no issue completion follows from this handoff.
