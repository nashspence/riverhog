# stove0-server component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:stove0-server:stove0-server-component-boundary:151ebfc28c -->

| Audit field | Value |
|---|---|
| Authority | `stove0-server` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/41`

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
    "stove0-server": "stove0_api.app:main"
  },
  "dependencies": [
    "http-api-contracts",
    "riverhog-client",
    "riverhog-protocol",
    "state-schema",
    "stove0-observer-client",
    "stove0-observer-protocol",
    "stove0-operator-contracts",
    "stove0-protocol",
    "stove0-recipe-config",
    "stove0-target-client",
    "stove0-target-protocol",
    "time-formats"
  ],
  "distribution": "stove0-server",
  "optional_dependencies": {},
  "path": "reference/stove0/application/server",
  "role": "reference_application"
}
```
