# stove0-review-target-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-review-target-contracts:stove0-review-target-contracts-component-boundary:463c23ce20 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0-review-target-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

| Field | Shape |
|---|---|
| `console_scripts` | empty object |
| `dependencies` | ["stove0-protocol","stove0-target-protocol"] |
| `distribution` | "stove0-review-target-contracts" |
| `optional_dependencies` | empty object |
| `path` | "reference/stove0/targets/review/contracts" |
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
