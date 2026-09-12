# gogurt-macos-listener-host component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt-macos-listener-host:gogurt-macos-listener-host-component-boundary:6f9cd3cc91 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-macos-listener-host` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/17`

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
| `distribution` | "gogurt-macos-listener-host" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/listener-host/macos" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6ee48c6d7241661ed76d2281ee69acc454552cbd91bef0641dd35ff593f2cdf -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt-macos-listener-host",
  "optional_dependencies": {},
  "path": "reference/gogurt/listener-host/macos",
  "role": "reference_component"
}
```
