# gogurt-path-volume-support component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-path-volume-support:gogurt-path-volume-support-component-boundary:78929299bf -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-path-volume-support` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/21`

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
| `console_scripts` | object (0 fields) |
| `dependencies` | array (2 items) |
| `distribution` | "gogurt-path-volume-support" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/mounted-volume/path-support" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 41d7be3cea5bba8db0ae3711119e2a9fb2f5b3635c4450b39f1d07e74181657d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "config-validation",
    "gogurt-core"
  ],
  "distribution": "gogurt-path-volume-support",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/path-support",
  "role": "reference_component"
}
```
