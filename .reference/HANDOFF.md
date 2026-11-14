# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owner: [#964](https://github.com/nashspence/riverhog/issues/964), specifically
[comment 6863269082](https://github.com/nashspence/riverhog/issues/964#issuecomment-6863269082).
Convention: [#903](https://github.com/nashspence/riverhog/issues/903).

Branch: [`reference/external/964-imported-history-reuse`](https://github.com/nashspence/riverhog/tree/reference/external/964-imported-history-reuse).
The exact published branch-tip SHA recorded in the owning issue is the durable
handoff identity. Branch navigation alone is not that identity.

## Start here

This candidate removes per-import replay of an identical historical primary
prefix in the server terminal closure. It first validates and indexes the
current raw corpus, then proves each import through exact current endpoint and
historical delivery point checks. Independent readers keep their existing
fresh-provider checks. The existing whole terminal attempt remains synchronous.

**First integration step:** reconcile the four product-file changes and their
maintained witnesses against the integration agent's exact current source;
review the proof lifetime and remaining outer-turn limit in [DESIGN.md](DESIGN.md),
then reproduce the focused selection in [VALIDATION.md](VALIDATION.md). The
owning issue controls whether this finite disposition is accepted.

## Source and provenance

- Audited base: PR #979 at the explicitly requested
  `1ff0a9ce70c23f18aa94d5b52ed036376132021a`.
- Tested local producer implementation: `63843b479986486298d47e4347226a74ee197570`;
  exact tree `00f68336b22d9862a42bdba39f530cd8cf2cc529`.
- GitHub implementation mirror: `4987c777c0005e65d5acdb30d4e2dbb47f7091ff`. Git data publication
  reconstructs the exact tested tree on the exact audited base; producer and
  GitHub commit identities differ. The final reference adds only this packet.
- Later inspected integration source:
  `fecde31e05fb166636cae06772bd519a9d6bb457`. Its difference from the requested
  base is one retention-fixture test correction, with no runtime-code drift.
  Read-only merge-tree reconciliation is clean; that is not runtime qualification
  of the merged candidate.
- Product paths contain proposed edits, not patch-only suggestions. This is a
  new imported-history handoff; it does not replace or rewrite the already
  adopted selected-prefix reference at
  `b7fd18cc60d759f5ca44e7655dbc83a477cb5045`.
- Produced by: OpenAI ChatGPT. Exposed model: GPT-6 Astra Pro. Configuration:
  Default collaboration with parallel implementation/review; reasoning-effort
  setting not exposed. Requested by: the repository user's explicit request to
  unblock their integration agent. Published through the authenticated GitHub
  app connection; commit metadata does not replace this producer record.

## Evidence and limits

Complete repository lint passed in 4.074 seconds.
The focused unit gate passed all 168 tests in
975.719 seconds, with clean source at both ends.
The real terminal F=1/F=4 witness keeps source reads, Native advances and corpus
frames independent of import fanout across both prefix extents. Maintained
corruption, currentness, historical correction, cache capacity and original
receiving cases pass. This is focused evidence; the unit selection is not full
canonical unit qualification.

[VALIDATION.md](VALIDATION.md) contains the exact commands, source relationship,
actual work and timing, failed development-attempt disposition, and unperformed
qualification. [evidence/results.json](evidence/results.json) links the original
receipts and maintains separate work, correctness and timing observations.

This reference does not establish whole-path G13 acceptance, bounded resumable
terminal turns, PostgreSQL execution evidence, or throughput at 1,000, 10,000 or
100,000 members. Qualification-observer cost remains the separate #991 question.
No total archive-size limit or generic reader freshness exception is introduced.

This branch is externally produced input. Its request, creation, testing,
publication or issue reference does not establish or extend an accepted design,
contract, release requirement or authorization to integrate. Authority remains
with the owning issue, subsequent maintainer decisions and the normal integration
rail. Reconcile before adoption; do not apply the reference mechanically.
