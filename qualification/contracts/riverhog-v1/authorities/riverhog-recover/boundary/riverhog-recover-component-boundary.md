# riverhog-recover component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-recover:riverhog-recover-component-boundary:23d8e1f86c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-recover` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/35`

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
| `distribution` | "riverhog-recover" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/recovery" |
| `role` | "reference_application" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cbf685bdb666a92c0942e8c92d1e82b4c6bc3811bad73b2808a9be283205053f -->

```json
{
  "console_scripts": {
    "riverhog-recover": "riverhog_recover.cli:main"
  },
  "dependencies": [
    "riverhog-archive-contracts",
    "riverhog-protocol",
    "riverhog-provenance"
  ],
  "distribution": "riverhog-recover",
  "optional_dependencies": {},
  "path": "reference/riverhog/recovery",
  "role": "reference_application"
}
```
