# Contract projection identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: boundary:repository:contract-projection-identity:5fc959cadf -->

| Audit field | Value |
|---|---|
| Authority | `repository` |
| Interface | `boundary` |
| Family | `identity` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/schema`
- `/series`

## Effective policies

- `boundary/frozen-authority/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `release:release.toml` — `release.toml`
- Proof: `make release-check`
- Proof: `make build`

## Contract

```json
[
  "riverhog-contract-freeze/v1",
  "v1"
]
```
