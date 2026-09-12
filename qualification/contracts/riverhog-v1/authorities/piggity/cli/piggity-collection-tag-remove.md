# piggity collection tag remove

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-remove:0cdb676ee8 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `collection` |
| Contract elements | 1 |
| Extent decisions | 6 |

## Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/remove/parameters`

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
- [Operation parity: remove_collection_tag](../../riverhog/operation/operation-parity-remove-collection-tag.md)

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

- Parser name: `remove`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| `tag` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | tag |
| `revision` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'minimum': 1, 'name': 'integer range'} | --revision |
| `tag_set_identity` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag-set-identity |
| `operation_id` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --operation-id |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
