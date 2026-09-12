# PIGGITY_LOCAL_DATABASE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: configuration-environment:configuration:piggity-local-database:f05c943e01 -->

| Audit field | Value |
|---|---|
| Authority | `configuration` |
| Interface | `configuration-environment` |
| Family | `variables` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/configuration_environment/2`

## Effective policies

- `compatibility/configuration/v1`

## Executable sources and proof

- `configuration-environment:PIGGITY_LOCAL_DATABASE` — `configuration-environment:PIGGITY_LOCAL_DATABASE`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make unit`
- Proof: `make compose-smoke`

## Contract

```json
{
  "consumers": [
    "piggity"
  ],
  "name": "PIGGITY_LOCAL_DATABASE"
}
```
