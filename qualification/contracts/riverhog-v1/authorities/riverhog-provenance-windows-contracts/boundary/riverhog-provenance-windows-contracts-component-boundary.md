# riverhog-provenance-windows-contracts component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:riverhog-provenance-windows-contracts:riverhog-provenance-windows-contracts-com-27ca113ded:d918bb9f73 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance-windows-contracts` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/31`

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
    "riverhog-provenance-contracts"
  ],
  "distribution": "riverhog-provenance-windows-contracts",
  "optional_dependencies": {},
  "path": "reference/riverhog/provenance/contracts/windows",
  "role": "reference_component"
}
```
