# stove0-media-archive-target-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-media-archive-target-contracts:stove0-media-archive-target-contracts-com-aef15ecff2:77e2e60a26 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-contracts](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-14771392c8) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-7b50338a38"></a>
| Field | Shape |
|---|---|
| <a id="s-0e3293a814"></a>`console_scripts` | empty object |
| <a id="s-ac9fa96dd5"></a>`dependencies` | ["stove0-protocol","stove0-target-protocol"] |
| <a id="s-82d9a8c0e1"></a>`distribution` | "stove0-media-archive-target-contracts" |
| <a id="s-0fc41310d6"></a>`optional_dependencies` | empty object |
| <a id="s-58ca195fd8"></a>`path` | "reference/stove0/targets/media-archive/contracts" |
| <a id="s-d73da172e2"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-973abb598e"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0f)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5f) — `release.toml`

### Machine authority

- `/boundaries/components/56`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d6c64db666fd47ad3d5fb7c1ffac758cf73b304b33f0702cac385a0d294a99b -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-media-archive-target-contracts",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/media-archive/contracts",
  "role": "reference_component"
}
```
