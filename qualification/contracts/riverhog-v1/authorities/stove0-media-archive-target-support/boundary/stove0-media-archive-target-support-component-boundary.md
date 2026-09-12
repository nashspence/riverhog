# stove0-media-archive-target-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-archive-target-support:stove0-media-archive-target-support-compo-e239b6cb59:ca2b980897 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-5c800aafc5) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-85d7aec0bf"></a>
| Field | Shape |
|---|---|
| <a id="s-f3324fb055"></a>`console_scripts` | empty object |
| <a id="s-da2c010104"></a>`dependencies` | ["riverhog-protocol","stove0-media-archive-target-contracts","stove0-media-metadata-observer-contracts","stove0-observer-protocol","stove0-protocol","stove0-target-protocol"] |
| <a id="s-5801d5727c"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-27ffb3ed07"></a>`optional_dependencies` | empty object |
| <a id="s-70b3eb841d"></a>`path` | "reference/stove0/targets/media-archive/support" |
| <a id="s-77a2fd5ee6"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-ce886a4a11"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/57`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f49957f019e48f6a03bfc440cb60fd2a6e8863918ab79ca298c5e6305bb45cb1 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol",
    "stove0-media-archive-target-contracts",
    "stove0-media-metadata-observer-contracts",
    "stove0-observer-protocol",
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-media-archive-target-support",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/media-archive/support",
  "role": "reference_component"
}
```
