# stove0-opus-target component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-opus-target:stove0-opus-target-component-boundary:1eafd2ccdf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-opus-target` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | additional keys=`stove0-opus-target` |
| `dependencies` | ["http-api-contracts","riverhog-client","riverhog-protocol","stove0-media-archive-target-contracts","stove0-media-archive-target-support","stove0-protocol","stove0-target-support"] |
| `distribution` | "stove0-opus-target" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/opus/target" |
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
