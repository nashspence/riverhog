# stove0-recipe-config component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-recipe-config:stove0-recipe-config-component-boundary:32ce7b8e00 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-recipe-config` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["config-validation","stove0-protocol","stove0-target-protocol"] |
| `distribution` | "stove0-recipe-config" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/packages/recipe-config" |
| `role` | "reusable_library" |

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

- `/boundaries/components/52`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fcb99aa9a5163ec20103a21a86db6682be65a5af56e04cbba891dc8fe015cef1 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "stove0-protocol",
    "stove0-target-protocol"
  ],
  "distribution": "stove0-recipe-config",
  "optional_dependencies": {},
  "path": "reference/stove0/packages/recipe-config",
  "role": "reusable_library"
}
```
