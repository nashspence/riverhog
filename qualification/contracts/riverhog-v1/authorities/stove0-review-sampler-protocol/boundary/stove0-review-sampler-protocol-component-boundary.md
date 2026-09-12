# stove0-review-sampler-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler-protocol-component-boundary:f4aeeb66b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-3eabd6aef7) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f765287e40"></a>
| Field | Shape |
|---|---|
| <a id="s-d45747f0a2"></a>`console_scripts` | empty object |
| <a id="s-a37da34b63"></a>`dependencies` | ["http-api-contracts","riverhog-protocol","stove0-protocol"] |
| <a id="s-965d691bce"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-7c63a37fde"></a>`optional_dependencies` | empty object |
| <a id="s-750ef89737"></a>`path` | "reference/stove0/targets/review/sampler/protocol" |
| <a id="s-ca0aed4020"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-b5149b40e2"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/67`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42b78928abf2886def21bfc83b2129b4b35d2edf36112df550bd4cb3e63e4002 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-protocol",
    "stove0-protocol"
  ],
  "distribution": "stove0-review-sampler-protocol",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/sampler/protocol",
  "role": "reference_component"
}
```
