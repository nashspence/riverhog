# riverhog-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-client:riverhog-client-component-boundary:ad6c59ed6b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/6`

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
| `dependencies` | array (4 items) |
| `distribution` | "riverhog-client" |
| `optional_dependencies` | object (0 fields) |
| `path` | "packages/riverhog-client" |
| `role` | "reusable_library" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e178deea5a616eb3c3dfe0ca6cce73d0c3de381d54649d383895f27e2249bcca -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts",
    "riverhog-application-access",
    "riverhog-protocol",
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-client",
  "optional_dependencies": {},
  "path": "packages/riverhog-client",
  "role": "reusable_library"
}
```
