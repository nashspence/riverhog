# state-schema component boundary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:state-schema:state-schema-component-boundary:a60d84df07 -->

| Audit field | Value |
|---|---|
| Authority | `state-schema` |
| Interface | `boundary` |
| Family | `components` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/boundaries/components/13`

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
  "dependencies": [],
  "distribution": "state-schema",
  "optional_dependencies": {},
  "path": "packages/state-schema",
  "role": "internal_build_unit"
}
```
