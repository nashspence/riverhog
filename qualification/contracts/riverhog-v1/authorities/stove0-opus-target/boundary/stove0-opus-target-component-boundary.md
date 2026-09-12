# stove0-opus-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-opus-target:stove0-opus-target-component-boundary:1eafd2ccdf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-opus-target](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-e8795446bd) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-c4841ca843"></a>
| Field | Shape |
|---|---|
| <a id="s-6c1acdc417"></a>`console_scripts` | additional keys=`stove0-opus-target` |
| <a id="s-346db4fe12"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-media-archive-target-contracts","stove0-media-archive-target-support","stove0-protocol","stove0-target-support"] |
| <a id="s-0f45949ae6"></a>`distribution` | "stove0-opus-target" |
| <a id="s-87cfe49755"></a>`optional_dependencies` | empty object |
| <a id="s-f46ee5560f"></a>`path` | "reference/stove0/targets/opus/target" |
| <a id="s-8aee48be48"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-ff4a61cb17"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/61`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 563700c71e80d40a986cc3e1e0cb3491b5c7fa4f6f93e95f51ba56bdf42a8594 -->

```json
{
  "console_scripts": {
    "stove0-opus-target": "stove0_opus_target.app:target_main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "stove0-media-archive-target-contracts",
    "stove0-media-archive-target-support",
    "stove0-protocol",
    "stove0-target-support"
  ],
  "distribution": "stove0-opus-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/opus/target",
  "role": "reference_component"
}
```
