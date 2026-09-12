# piggity app key create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-create:5e03401d10 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `app` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/create/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: create_app_key](../../riverhog/operation/operation-parity-create-app-key.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | occurrences | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| `allow` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --allow |
| `expires_in` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --expires-in |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
