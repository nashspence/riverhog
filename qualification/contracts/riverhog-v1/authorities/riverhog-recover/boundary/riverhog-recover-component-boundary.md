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

## Contract

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
