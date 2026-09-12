# stove0-observer-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-protocol:stove0-observer-protocol-component-boundary:fea970d8a5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-3385b8c53d) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-53267c14cb"></a>
| Field | Shape |
|---|---|
| <a id="s-650e81fe10"></a>`console_scripts` | empty object |
| <a id="s-d4dacf0c8f"></a>`dependencies` | ["http-api-contracts","stove0-protocol"] |
| <a id="s-2f1ae905d6"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-36c59827d5"></a>`optional_dependencies` | empty object |
| <a id="s-2da934aff6"></a>`path` | "reference/stove0/packages/observer-protocol" |
| <a id="s-55a1f5ce21"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-e727de5cc8"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/48`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3053f9f50e6579637c9beeacdfee3165baaf3bbe8fa86adfe7257e79ec534d5d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-protocol"
  ],
  "distribution": "stove0-observer-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-protocol",
  "role": "reusable_library"
}
```
