# stove0 evaluation review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-evaluation-review:24dcbb9695 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `evaluation` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/review/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: review_evaluation_variant](../operation/operation-parity-review-evaluation-variant.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| value | cli-value | `contract_max` | maximum=5, minimum=1, reason=schema-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `review`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `evaluation_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | evaluation_id |
| `variant_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | variant_id |
| `rating` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 5, 'minimum': 1, 'name': 'integer range'} | --rating |
| `note` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --note |
