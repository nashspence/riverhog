# gogurt component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:gogurt:gogurt-component-boundary:33eef380d6 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/15`

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
| `dependencies` | array (3 items) |
| `distribution` | "gogurt" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/gogurt/application" |
| `role` | "reference_application" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44cf87689308a59b50d1f183145ad8732790c85607a9b3a702f5e463f728105a -->

```json
{
  "console_scripts": {
    "gogurt": "gogurt.cli:main"
  },
  "dependencies": [
    "config-validation",
    "gogurt-core",
    "gogurt-listener-runtime"
  ],
  "distribution": "gogurt",
  "optional_dependencies": {},
  "path": "reference/gogurt/application",
  "role": "reference_application"
}
```
