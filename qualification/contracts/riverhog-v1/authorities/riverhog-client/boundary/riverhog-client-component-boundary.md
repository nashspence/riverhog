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

## Contract

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
