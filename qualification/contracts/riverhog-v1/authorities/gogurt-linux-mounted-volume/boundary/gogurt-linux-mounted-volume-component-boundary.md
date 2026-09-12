# gogurt-linux-mounted-volume component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-linux-mounted-volume:gogurt-linux-mounted-volume-component-boundary:e47d4ab54c -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-linux-mounted-volume` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/19`

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
| `distribution` | "gogurt-linux-mounted-volume" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/mounted-volume/linux" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6e608e21ea12cffabb5daaa9bb85fd7e3748086bb8036acd780496862be03ab8 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-core",
    "gogurt-path-volume-support"
  ],
  "distribution": "gogurt-linux-mounted-volume",
  "optional_dependencies": {},
  "path": "reference/gogurt/mounted-volume/linux",
  "role": "reference_component"
}
```
