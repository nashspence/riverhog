# stove0-target-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-target-client:stove0-target-client-component-boundary:f9de47e9b5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-d8c6d98fef80) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-e06e4f6301fb"></a>
| Field | Shape |
|---|---|
| <a id="s-f9e84f2ae303"></a>`console_scripts` | empty object |
| <a id="s-9a6d9ba606c8"></a>`dependencies` | ["http-api-contracts","stove0-target-protocol"] |
| <a id="s-4523a81c951f"></a>`distribution` | "stove0-target-client" |
| <a id="s-c4f1e7666f4c"></a>`optional_dependencies` | empty object |
| <a id="s-181f399a1e0e"></a>`path` | "reference/stove0/packages/target-client" |
| <a id="s-c4aaf87535bf"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-f3c3b687fa29"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/53`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b201d8b2c97c9751893de9bae7b0cc41aa826a67938c92a5be9c7876ae888a13 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-target-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/target-client",
  "role": "reusable_library"
}
```
