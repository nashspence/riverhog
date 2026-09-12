# gogurt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:gogurt:gogurt:962bf8db21 -->

| Audit field | Value |
|---|---|
| Authority | `gogurt` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/gogurt/name`
- `/external_contract/cli/gogurt/parameters`

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

## Contract


### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `_version` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --version |
| `install_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --install-completion |
| `show_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --show-completion |
