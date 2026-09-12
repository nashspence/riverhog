# riverhog-application-access component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-application-access:riverhog-application-access-component-boundary:529bb88f14 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-application-access` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/4`

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
| `distribution` | "riverhog-application-access" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/riverhog-application-access" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 51196e02687ed1bed81a81e73945f59d7e16e2dab709e6d9cfbef6b2b22a437d -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "riverhog-protocol"
  ],
  "distribution": "riverhog-application-access",
  "optional_dependencies": {},
  "path": "packages/riverhog-application-access",
  "role": "reusable_library"
}
```
