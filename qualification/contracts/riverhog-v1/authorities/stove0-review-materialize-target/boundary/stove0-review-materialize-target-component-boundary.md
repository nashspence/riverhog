# stove0-review-materialize-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-materialize-target:stove0-review-materialize-target-component-boundary:921c3e7a46 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-materialize-target](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-f006806d98) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-0a57b09105"></a>
| Field | Shape |
|---|---|
| <a id="s-ae16c0646e"></a>`console_scripts` | additional keys=`stove0-review-materialize-target` |
| <a id="s-03430a3ea6"></a>`dependencies` | ["riverhog-client","stove0-review-target-contracts","stove0-review-target-support","stove0-target-support"] |
| <a id="s-4b4e5887ac"></a>`distribution` | "stove0-review-materialize-target" |
| <a id="s-a5b42ea7c4"></a>`optional_dependencies` | empty object |
| <a id="s-b87e8fbf15"></a>`path` | "reference/stove0/targets/review/materialize-target" |
| <a id="s-e2e2825c11"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-de49108869"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/63`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2ecdea7f17eeddfb0da55a218e84f31ba817563b8fe6b74a4c2d33748237d3c9 -->

```json
{
  "console_scripts": {
    "stove0-review-materialize-target": "stove0_review_materialize_target.app:main"
  },
  "dependencies": [
    "riverhog-client",
    "stove0-review-target-contracts",
    "stove0-review-target-support",
    "stove0-target-support"
  ],
  "distribution": "stove0-review-materialize-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/review/materialize-target",
  "role": "reference_component"
}
```
