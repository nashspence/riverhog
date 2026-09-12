# stove0-target-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-target-conformance:stove0-target-conformance:f16077bd02 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-target-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/stove0-target-conformance/name`
- `/external_contract/cli/stove0-target-conformance/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:stove0-target-conformance` — `reference/stove0/packages/target-support/src/stove0_target_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract

- Parser name: `stove0-target-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _AppendAction | no | Path | --case |
