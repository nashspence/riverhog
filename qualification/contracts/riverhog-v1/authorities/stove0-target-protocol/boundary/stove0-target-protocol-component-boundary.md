# stove0-target-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-protocol:stove0-target-protocol-component-boundary:ad5ba6edfb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-c6e39a6cda09) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-4652af96ee64"></a>
| Field | Shape |
|---|---|
| <a id="s-f2d2e09beeed"></a>`console_scripts` | empty object |
| <a id="s-4a01269a2021"></a>`dependencies` | ["http-api-contracts","riverhog-protocol","stove0-protocol"] |
| <a id="s-2854c167cbd9"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-cf25daf23dc5"></a>`optional_dependencies` | empty object |
| <a id="s-05c21d6131ca"></a>`path` | "reference/stove0/packages/target-protocol" |
| <a id="s-c9676bad158e"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-e27715c85b7b"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/54`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3745bf97c2ecbf800973bd9913d326f91011bf5844db09f9badfab02e36eb724 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-protocol",
    "stove0-protocol"
  ],
  "distribution": "stove0-target-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-protocol",
  "role": "reusable_library"
}
```
