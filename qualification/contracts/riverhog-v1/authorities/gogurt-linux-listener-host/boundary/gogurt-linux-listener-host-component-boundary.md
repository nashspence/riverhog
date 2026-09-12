# gogurt-linux-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-linux-listener-host:gogurt-linux-listener-host-component-boundary:d8ca97d4f9 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-linux-listener-host` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/16`

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
| `dependencies` | array (1 items) |
| `distribution` | "gogurt-linux-listener-host" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/listener-host/linux" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d6686206b45878ae5565f688a1515b7fd3597b6f7bde75adf69b3bd3b9af47f8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-linux-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/linux",
  "role": "reference_component"
}
```
