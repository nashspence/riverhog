# stove0-control durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-control:stove0-control-durable-state:d6236ad32e -->

| Audit field | Value |
|---|---|
| Authority | `stove0-control` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/3`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:stove0-control` — `state:stove0-control`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: state-schema/postgresql
