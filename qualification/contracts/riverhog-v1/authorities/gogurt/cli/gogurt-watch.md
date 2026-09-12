# gogurt watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt-watch:0e422f05a7 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `watch` |
| Contract elements | 1 |
| Extent decisions | 8 |

## Machine authority

- `/external_contract/cli/gogurt/commands/watch/name`
- `/external_contract/cli/gogurt/commands/watch/parameters`

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
| value | cli-value | `contract_max` | maximum=3600, minimum=0.1, reason=schema-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `config` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --config |
| `actions_dir` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'path'} | --actions-dir |
| `interval_seconds` | TyperOption | no | {'class': 'typer._click.types.FloatRange', 'maximum': 3600, 'minimum': 0.1, 'name': 'float range'} | --interval |
| `include_existing` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --include-existing |
| `autorun` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --autorun |
| `dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run |
| `mounted_volume_provider` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --mounted-volume-provider |
