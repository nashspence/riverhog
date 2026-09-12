# stove0-nvenc-av1-opus-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-component-boundary:e3d80e2d19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-nvenc-av1-opus-target](../index.md) |
| Interface | [boundary](index.md) |
| Family | [components](index.md#f-102afc428814) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb7f3af98e6b"></a>
| Field | Shape |
|---|---|
| <a id="s-15704a78588c"></a>`console_scripts` | additional keys=`stove0-nvenc-av1-opus-target` |
| <a id="s-8b8eaa7953f3"></a>`dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-media-archive-target-contracts","stove0-media-archive-target-support","stove0-protocol","stove0-target-support"] |
| <a id="s-9f44e1c5856d"></a>`distribution` | "stove0-nvenc-av1-opus-target" |
| <a id="s-65a6b77bb26f"></a>`optional_dependencies` | empty object |
| <a id="s-95ff3d0eb4d8"></a>`path` | "reference/stove0/targets/nvenc-av1-opus/target" |
| <a id="s-8fc44f6c5c62"></a>`role` | "reference_component" |

## Governing policies

- <a id="pa-a67c69a77d1d"></a>[boundary/frozen-authority/v1](../../../policies/index.md#p-61994d3f0fa8)

## Evidence

### Qualification

- [make release-check](../../../evidence/sources.md#q-8d8d22d6a6b1)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [release:release.toml](../../../evidence/sources.md#src-c5380dbe5fe0) — `release.toml`

### Machine authority

- `/boundaries/components/59`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 12caa1c19ae6b5d1acfe1a006bd60299eb16aeb0a1830b7f864f15e1da6541e7 -->

```json
{
  "console_scripts": {
    "stove0-nvenc-av1-opus-target": "stove0_nvenc_av1_opus_target.app:target_main"
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
  "distribution": "stove0-nvenc-av1-opus-target",
  "optional_dependencies": {},
  "path": "reference/stove0/targets/nvenc-av1-opus/target",
  "role": "reference_component"
}
```
