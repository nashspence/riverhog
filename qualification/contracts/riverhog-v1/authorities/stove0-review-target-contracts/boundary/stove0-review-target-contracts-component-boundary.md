# stove0-review-target-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-target-contracts:stove0-review-target-contracts-component-boundary:463c23ce20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-target-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-73fec5d472) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-69f7f4fe94"></a>
| Field | Shape |
|---|---|
| <a id="s-90a0c5c839"></a>`console_scripts` | empty object |
| <a id="s-f64287a955"></a>`dependencies` | ["stove0-protocol","stove0-target-protocol"] |
| <a id="s-b0b7d774c0"></a>`distribution` | "stove0-review-target-contracts" |
| <a id="s-789bcc2994"></a>`optional_dependencies` | empty object |
| <a id="s-90ad2fecaf"></a>`path` | "reference/stove0/targets/review/contracts" |
| <a id="s-8f401e021e"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-5f3d6054fc"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/62`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b43d146a1b9722143a00cfeaf3c1568fef363e0867de2ced24a5cdef0256c1ca -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-review-target-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/contracts",
  "role": "reference_component"
}
```
