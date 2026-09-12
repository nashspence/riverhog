# stove0-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-api-client:stove0-api-client-component-boundary:0f99add1c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-api-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-097e942862) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-b2afd96c68"></a>
| Field | Shape |
|---|---|
| <a id="s-01f9745308"></a>`console_scripts` | empty object |
| <a id="s-ae4dbcde62"></a>`dependencies` | ["http-api-contracts","stove0-operator-contracts","stove0-protocol"] |
| <a id="s-6b36e061ef"></a>`distribution` | "stove0-api-client" |
| <a id="s-37d74a3b07"></a>`optional_dependencies` | empty object |
| <a id="s-f311017b99"></a>`path` | "reference/stove0/packages/api-client" |
| <a id="s-868c0d35d2"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-3abe92d9cd"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/46`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc419930d0e876f90972a328d4358adfe116adeee6179b337b077459aedd0387 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-operator-contracts",
    "stove0-protocol"
  ],
  "distribution": "stove0-api-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/api-client",
  "role": "reusable_library"
}
```
