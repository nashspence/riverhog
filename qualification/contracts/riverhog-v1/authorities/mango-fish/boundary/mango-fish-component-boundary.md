# mango-fish component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:mango-fish:mango-fish-component-boundary:23e5b8889e -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/25`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract summary

| Field | Shape |
|---|---|
| `console_scripts` | object (1 fields) |
| `dependencies` | array (2 items) |
| `distribution` | "mango-fish" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/applications/mango-fish" |
| `role` | "reference_application" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 113e1f07a2dadb12b61fe99c441ef6fe3a3fa0f3d7be453dccb6c61757b7a072 -->

```json
{
  "console_scripts": {
    "mango-fish": "mango_fish.cli:main"
  },
  "dependencies": [
    "lifecycle-events",
    "state-schema"
  ],
  "distribution": "mango-fish",
  "optional_dependencies": {},
  "path": "reference/riverhog/applications/mango-fish",
  "role": "reference_application"
}
```
