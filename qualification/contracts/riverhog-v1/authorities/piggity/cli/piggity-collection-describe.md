# piggity collection describe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-describe:d9879d2658 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `collection` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/describe/name`
- `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: get_collection](../../riverhog/operation/operation-parity-get-collection.md)
- [Operation parity: replace_collection_description](../../riverhog/operation/operation-parity-replace-collection-description.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `describe`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `collection` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection |
| `description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| `clear` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --clear |
| `if_match` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --if-match |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
