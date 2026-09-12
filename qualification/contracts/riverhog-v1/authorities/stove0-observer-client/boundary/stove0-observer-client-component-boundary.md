# stove0-observer-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-observer-client:stove0-observer-client-component-boundary:72a37bbc2a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-6326c3fc14) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6e4b08e3d0"></a>
| Field | Shape |
|---|---|
| <a id="s-1df5f0f3f2"></a>`console_scripts` | empty object |
| <a id="s-126b1c40b8"></a>`dependencies` | ["http-api-contracts","stove0-observer-protocol"] |
| <a id="s-8527c4f8fa"></a>`distribution` | "stove0-observer-client" |
| <a id="s-1515dfc63a"></a>`optional_dependencies` | empty object |
| <a id="s-0e559154cd"></a>`path` | "reference/stove0/packages/observer-client" |
| <a id="s-7e6eb5ca59"></a>`role` | "reusable_library" |

## Governing policies

- <a id="pa-cccc5cc165"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/47`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f00d43de862423fd4865909185cf11bada37089bc755da10a52994584b601856 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-observer-protocol"
  ],
  "distribution": "stove0-observer-client",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/observer-client",
  "role": "reusable_library"
}
```
