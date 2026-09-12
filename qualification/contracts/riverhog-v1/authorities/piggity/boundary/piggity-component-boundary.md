# piggity component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:piggity:piggity-component-boundary:43cd09ec8c -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/26`

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
| `dependencies` | array (7 items) |
| `distribution` | "piggity" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/applications/piggity" |
| `role` | "reference_application" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 385c7e9cfbd8ded25aa1a4d6175f7d4ba15e76acbff592970dcc91cbeec0e0f1 -->

```json
{
  "console_scripts": {
    "piggity": "piggity.main:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-application-access",
    "riverhog-client",
    "riverhog-protocol",
    "riverhog-provenance",
    "state-schema",
    "time-formats"
  ],
  "distribution": "piggity",
  "optional_dependencies": {},
  "path": "reference/riverhog/applications/piggity",
  "role": "reference_application"
}
```
