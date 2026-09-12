# stove0-observer-conformance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-observer-conformance:stove0-observer-conformance:2b03b92040 -->

| Audit field | Value |
|---|---|
| Authority | `stove0-observer-conformance` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/cli/stove0-observer-conformance/name`
- `/external_contract/cli/stove0-observer-conformance/parameters`

## Effective policies

- `compatibility/cli/v1`

## Executable sources and proof

- `cli:stove0-observer-conformance` — `reference/stove0/packages/observer-support/src/stove0_observer_support/conformance.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Contract

- Parser name: `stove0-observer-conformance`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _StoreAction | yes |  |  |
| `` | _AppendAction | no | Path | --invocation |
| `` | _AppendAction | no | Path | --semantic-vectors |
| `` | _AppendAction | no |  | --semantic-validator-provider |
