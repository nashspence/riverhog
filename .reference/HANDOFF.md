# NON-AUTHORITATIVE EXTERNAL REFERENCE

Owning issue: [#959](https://github.com/nashspence/riverhog/issues/959)
Convention: [#903](https://github.com/nashspence/riverhog/issues/903)
Scheduling: **recommended post-v1 only; not a v1 dependency or release gate**
Audited base: `main` @ `a26e214f77ab806e4230125bf7aad93f8c3328da`
Audited tree: `dfd9f178581c0e1290a31584c824c9a286e55c98`
Reference branch: `reference/external/959-post-v1-hosted-evidence`
Supersedes: `bcabb02bef4d12d45301fcf0419c850ea0e96e06` as current design/handoff.
The earlier reference and publication comment remain preserved historical input.

Produced by: OpenAI ChatGPT
Model: GPT-6 Astra Pro
Configuration/effort: not exposed
Requested by: Riverhog maintainer, @nashspence
Published by: OpenAI ChatGPT through connected GitHub tools
Research date: 2026-10-05

This is a researched design and executable contract specimen, NOT a completed
NiCad/cloud integration. Its publication, tests or issue linkage do not authorize
integration, cloud spending, monitoring activation or changes to release scope.
The owning issue, later maintainer decisions and the then-current integration
rail control. Previous conversation is calibration, not additional authority.
Do not assume today's pre-v1 direct-main rail applies to this post-v1 work.

## Takeover

Read the owning issue, then `INTEGRATION.md`, `RESEARCH.md` and `CONTRACT.json`.
The selected boundary is Actions collection/NiCad analysis, hosted PostgreSQL
custody/private indexes, and a managed verification/query application. No personal
host owns required ingestion, decryption, history, scheduling or recovery.

The older reference's collector/validators may be reconciled as useful starting
code. Its three canaries are not structural fingerprints; its artifact-only,
private-host architecture is superseded. Its workflow must not be copied unchanged.
This new branch changes only `.reference/`; it adds no active workflow or runtime.

`ledger_reference.py` and its synthetic tests exercise logical work-key separation,
bounded ciphertext chunks, reassembly and completed-work eligibility. They are not
a database adapter, encryption implementation, attestation verifier or SQL migration.
Do not make production code import `.reference`.

## Validation performed

Executed in this session:

```sh
cd .reference
python -m unittest -v test_ledger_reference.py
```

Result: **12 tests passed**. Cases cover every work-key field, map ordering,
rejection of extra/invalid identity fields, chunk boundaries, missing/duplicate/
reordered/corrupt chunks, whole-object substitution, size/base64 overhead, partial
work reuse refusal and agreement with `CONTRACT.json`. Bytecode compilation and
JSON parsing were also checked. Publication verifies the supplied blob identities,
base parent and exact remote reference commit, recorded in the owning issue.

Scoped source review used #959 and comments, #903, current AGENTS.md, the latest
main/#954 diff, the earlier integration handoff, and pinned upstream NiCad README
and Type-2 configuration. Primary AWS/GitHub/OpenAI/MCP documentation was checked.

## Not validated

No live NiCad/OpenTxl build or grammar run, PostgreSQL/SQL permissions or transaction
test, AWS deployment/Data API/OIDC/secret configuration, cost measurement,
cryptographic round trip, real attestation, managed ingestion, backup restoration,
account transfer, remote MCP or unattended model invocation was performed.
The full Riverhog `make lint`, `make unit`, `make dist-smoke`, `make build` and
exact-SHA remote integration gates were not run for this reference-only change.
The original reference's 78-pass/1-skip result is historical and is NOT validation
of these new capabilities. Exact publication checks are not production CI passes.

The publication interface has no native issue Development-link operation; record
that limitation in #959 rather than representing Markdown links as native linkage.
Preserve both old and new exact SHAs. No force-push, main/release edit, deployment,
secret/configuration write or task activation is part of this handoff.
