# Release tag template

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: release:release:release-tag-template:2a4162b673 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [release](../index.md) |
| Interface | [release](index.md) |
| Family | [release-contract](index.md#f-6cd3d52e18) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-5e3073c859"></a>
- Shape: "v{version}"

## Governing policies

- <a id="pa-ff8df2a266"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/external_contract/release/tag_template`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 268982a6c7e0cdd99400f4c234214a775df3b3dc046efd372f71ad85d564ff3e -->

```json
"v{version}"
```
