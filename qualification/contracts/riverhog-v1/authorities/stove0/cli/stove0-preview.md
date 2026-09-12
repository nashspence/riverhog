# stove0 preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-preview:5f4659b542 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `preview` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/stove0/commands/preview/name`
- `/external_contract/cli/stove0/commands/preview/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: preview_workflow](../operation/operation-parity-preview-workflow.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `preview`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| `inputs` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | inputs |
| `revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |
| `intent` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'file'} | --intent |
