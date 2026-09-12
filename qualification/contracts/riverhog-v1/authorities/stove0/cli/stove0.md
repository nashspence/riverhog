# stove0

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0:cf6895ddfc -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 7 |

## Machine authority

- `/external_contract/cli/stove0/name`
- `/external_contract/cli/stove0/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
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
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract


### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `version` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --version |
| `base_url` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --base-url |
| `token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --token |
| `json_output` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
| `allow_insecure_http` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --allow-insecure-http |
| `install_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --install-completion |
| `show_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --show-completion |
