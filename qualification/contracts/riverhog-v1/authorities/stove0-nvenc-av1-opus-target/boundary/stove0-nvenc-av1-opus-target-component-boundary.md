# stove0-nvenc-av1-opus-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-nvenc-av1-opus-target:stove0-nvenc-av1-opus-target-component-boundary:e3d80e2d19 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-nvenc-av1-opus-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-nvenc-av1-opus-target` |
| `dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-media-archive-target-contracts","stove0-media-archive-target-support","stove0-protocol","stove0-target-support"] |
| `distribution` | "stove0-nvenc-av1-opus-target" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/nvenc-av1-opus/target" |
| `role` | "reference_component" |

## Governing policies

- `boundary/frozen-authority/v1`

## Evidence

### Qualification

- `make release-check`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`

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
