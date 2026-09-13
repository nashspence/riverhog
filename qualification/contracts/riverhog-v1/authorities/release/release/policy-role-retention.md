# Policy: role retention

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:policy-role-retention:9910dae6ee -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [Release](index.md) |

## External contract

<a id="s-64c9b78054"></a>
- Shape: "Publication creates no new product, API, protocol, provider, or direct-consumption promise. Each publication unit retains its exact release role and links to its existing semantic owners."

## Governing policies

- <a id="pa-528704e770"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

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

- `/external_contract/release/publication/policy/role_retention`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73a980b4501c7c5bbf72594aa8f78e7471a565d14a8647175f3fa5ea96501cfc -->

```json
"Publication creates no new product, API, protocol, provider, or direct-consumption promise. Each publication unit retains its exact release role and links to its existing semantic owners."
```
