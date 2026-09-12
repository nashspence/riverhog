# mango-fish-cursor durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:mango-fish-cursor:mango-fish-cursor-durable-state:2504b0617b -->

| Audit field | Value |
|---|---|
| Authority | `mango-fish-cursor` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/2`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:mango-fish-cursor` — `state:mango-fish-cursor`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: state-schema/sqlite
