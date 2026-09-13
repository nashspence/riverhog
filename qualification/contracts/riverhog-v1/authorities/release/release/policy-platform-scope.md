# Policy: platform scope

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:policy-platform-scope:596425b169 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-d941896c37"></a>
- Shape: "A platform or architecture claim applies only to the exact published image, distribution, or installation form that carries it. Qualification platforms remain evidence unless an external contract independently protects them."

## Governing policies

- <a id="pa-14a52addbe"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release-publication:planner](../../../evidence/sources.md#src-03a2f48338) — `scripts/release.py::publication_contract`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/publication/policy/platform_scope`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff1a659d0fd29bcc0611f05fc560110a8336560ba71407147714e552c97fc8ae -->

```json
"A platform or architecture claim applies only to the exact published image, distribution, or installation form that carries it. Qualification platforms remain evidence unless an external contract independently protects them."
```
