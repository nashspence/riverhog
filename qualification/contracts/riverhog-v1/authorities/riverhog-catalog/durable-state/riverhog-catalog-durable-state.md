# riverhog-catalog durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:riverhog-catalog:riverhog-catalog-durable-state:e07af31f57 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-catalog` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/0`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:riverhog-catalog` — `state:riverhog-catalog`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: state-schema/postgresql
