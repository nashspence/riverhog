# gogurt-listener durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:gogurt-listener:gogurt-listener-durable-state:11bfac7224 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt-listener` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/4`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:gogurt-listener` — `state:gogurt-listener`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: gogurt-listener-state/v1
