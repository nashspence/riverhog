# stove0 work create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-work-create:64b28fd403 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `work` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/cli/stove0/commands/work/commands/create/name`
- `/external_contract/cli/stove0/commands/work/commands/create/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: create_work](../operation/operation-parity-create-work.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| `inputs` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | inputs |
| `preview_sha256` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --preview-sha256 |
| `revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |
| `intent` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'file'} | --intent |
