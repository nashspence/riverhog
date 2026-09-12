# stove0-review-sampler-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-client:stove0-review-sampler-client-component-boundary:2da2ddf525 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-client](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-5e7fbd6684) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-344a1f9cb3"></a>
| Field | Shape |
|---|---|
| <a id="s-1adff31355"></a>`console_scripts` | empty object |
| <a id="s-5fd720f4b7"></a>`dependencies` | ["http-api-contracts","stove0-review-sampler-protocol"] |
| <a id="s-348bb905b4"></a>`distribution` | "stove0-review-sampler-client" |
| <a id="s-1969571ba3"></a>`optional_dependencies` | empty object |
| <a id="s-79dc547956"></a>`path` | "reference/stove0/targets/review/sampler/client" |
| <a id="s-e7e7da0ba9"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-5289ac7049"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/66`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 19df098b2326b6585b2c084521dbb826fd9ea9ed7dcc9ea3d36b7a9a0f7a9b7e -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "stove0-review-sampler-protocol"
  ],
  "distribution": "stove0-review-sampler-client",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/client",
  "role": "reference_component"
}
```
