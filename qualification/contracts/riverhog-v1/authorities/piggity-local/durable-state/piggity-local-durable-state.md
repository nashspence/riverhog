# piggity-local durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:piggity-local:piggity-local-durable-state:570c36bae9 -->

| Audit field | Value |
|---|---|
| Authority | `piggity-local` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/1`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:piggity-local` — `state:piggity-local`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: state-schema/sqlite
