# gogurt run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-run:f4364383c7 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `run` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/cli/gogurt/commands/run/name`
- `/external_contract/cli/gogurt/commands/run/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:gogurt` — `reference/gogurt/application/src/gogurt/cli.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `mount_point` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | mount_point |
| `config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| `actions_dir` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --actions-dir |
| `autorun` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --autorun |
| `dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| `mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |
