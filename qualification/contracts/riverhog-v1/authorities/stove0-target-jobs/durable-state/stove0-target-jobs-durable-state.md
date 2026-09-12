# stove0-target-jobs durable state

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: durable-state:stove0-target-jobs:stove0-target-jobs-durable-state:2b6eb6272b -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-jobs` |
| Interface | `durable-state` |
| Family | `owners` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/durable_state/owners/5`

## Effective policies

- `compatibility/components/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `state:stove0-target-jobs` — `state:stove0-target-jobs`
- Proof: `make release-check`
- Proof: `make database-qualification`

## Contract

- `format`: stove0-target-job-state/v1
