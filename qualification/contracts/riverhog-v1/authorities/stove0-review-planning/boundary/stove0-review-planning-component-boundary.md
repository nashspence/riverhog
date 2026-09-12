# stove0-review-planning component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-planning:stove0-review-planning-component-boundary:d9df6bda01 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-planning](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-348231c3bd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-30d599413f"></a>
| Field | Shape |
|---|---|
| <a id="s-838eee3419"></a>`console_scripts` | additional keys=`stove0-review-planning` |
| <a id="s-5259a89748"></a>`dependencies` | ["stove0-media-sampling-observer-contracts","stove0-protocol","stove0-review-target-contracts"] |
| <a id="s-4306bfea0a"></a>`distribution` | "stove0-review-planning" |
| <a id="s-41c377eee3"></a>`optional_dependencies` | empty object |
| <a id="s-9c97d80105"></a>`path` | "reference/stove0/targets/review/planning" |
| <a id="s-48d77bca08"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-bb06fe84a8"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/64`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7ebd931b6d8a76f36f030b0228c4681748aa8701e58ea8059fe59e481efdbd3f -->

```json
{
  "console_scripts": {
    "stove0-review-planning": "stove0_review_planning.conformance:main"
  },
  "dependencies": [
    "stove0-media-sampling-observer-contracts",
    "stove0-protocol",
    "stove0-review-target-contracts"
  ],
  "distribution": "stove0-review-planning",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/planning",
  "role": "reference_component"
}
```
