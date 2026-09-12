# riverhog-ftp-adapter-api-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-ftp-adapter-api-client:riverhog-ftp-adapter-api-client-component-boundary:080243d900 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-ftp-adapter-api-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/28`

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
| `distribution` | "riverhog-ftp-adapter-api-client" |
| `optional_dependencies` | object (0 fields) |
| `path` | "reference/riverhog/ingress/ftp-api-client" |
| `role` | "reference_component" |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 32f59f147ed4efabd494b32e3cfc2e7a7776a4e5fe4cc96549f9f100a90fb5f5 -->

```json
{
  "console_scripts": {},
  "dependencies": [
    "http-api-contracts"
  ],
  "distribution": "riverhog-ftp-adapter-api-client",
  "optional_dependencies": {},
  "path": "reference/riverhog/ingress/ftp-api-client",
  "role": "reference_component"
}
```
