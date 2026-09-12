# stove0-client component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-client:stove0-client-component-boundary:11bea8fac1 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-client` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/40`

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
    "stove0": "stove0_cli.main:main"
  },
  "dependencies": [
    "stove0-api-client",
    "stove0-protocol",
    "stove0-recipe-config"
  ],
  "distribution": "stove0-client",
  "optional_dependencies": {},
  "path": "reference/stove0/application/client",
  "role": "reference_application"
}
```
