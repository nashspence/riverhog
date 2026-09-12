# stove0-operator-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-operator-contracts:stove0-operator-contracts-component-boundary:4b9c2c4e22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-b10b8af2b3) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-52b6845a2d"></a>
| Field | Shape |
|---|---|
| <a id="s-168f25e3db"></a>`console_scripts` | empty object |
| <a id="s-d6066c1e10"></a>`dependencies` | ["http-api-contracts","lifecycle-events","riverhog-protocol","stove0-observer-protocol","stove0-protocol","stove0-recipe-config","stove0-target-protocol"] |
| <a id="s-785e0f416b"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-c9271d3c46"></a>`optional_dependencies` | empty object |
| <a id="s-744d1c87d5"></a>`path` | "reference/stove0/packages/operator-contracts" |
| <a id="s-846960df15"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-b85a4635bd"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/50`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bf0c311c39def0e1dc8fc03e6e98aa23658e4254c11e84900f03e9a35ae17835 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "lifecycle-events",
    "riverhog-protocol",
    "stove0-observer-protocol",
    "stove0-protocol",
    "stove0-recipe-config",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-operator-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/operator-contracts",
  "role": "reusable_library"
}
```
