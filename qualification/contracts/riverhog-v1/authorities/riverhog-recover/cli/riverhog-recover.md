# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:e47dfeaf6b -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-recover` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:riverhog-recover` — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=0, minimum=0, reason=fixed-command-argument-arity |

## Contract

- Parser name: `riverhog-recover`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `` | _VersionAction | no |  | --version |
| `` | _StoreAction | yes | Path |  |
| `` | _StoreAction | no | Path |  |
| `` | _StoreTrueAction | no |  | --description-only |
| `` | _StoreTrueAction | no |  | --tags-only |
| `` | _StoreAction | no | Path | --passphrases-file |
| `` | _StoreAction | no |  | --age-command |
