# stove0-review-sampler-protocol component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-sampler-protocol:stove0-review-sampler-protocol-component-boundary:f4aeeb66b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-3eabd6aef7df) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-f765287e4068"></a>
| Field | Shape |
|---|---|
| <a id="s-d45747f0a213"></a>`console_scripts` | empty object |
| <a id="s-a37da34b6359"></a>`dependencies` | ["http-api-contracts","riverhog-protocol","stove0-protocol"] |
| <a id="s-965d691bce8f"></a>`distribution` | "stove0-review-sampler-protocol" |
| <a id="s-7c63a37fde25"></a>`optional_dependencies` | empty object |
| <a id="s-750ef89737ab"></a>`path` | "reference/stove0/targets/review/sampler/protocol" |
| <a id="s-ca0aed40204d"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-b5149b40e29f"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

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
