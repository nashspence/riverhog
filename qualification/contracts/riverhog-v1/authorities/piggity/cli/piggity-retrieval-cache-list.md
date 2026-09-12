# piggity retrieval cache list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-retrieval-cache-list:8ea942fad2 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `retrieval` |
| Contract elements | 1 |
| Extent decisions | 15 |

## Machine authority

- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/name`
- `/external_contract/cli/piggity/commands/retrieval/commands/cache/commands/list/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: list_retrieval_cache_objects](../../riverhog/operation/operation-parity-list-retrieval-cache-objects.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| value | cli-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract

- Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `page_size` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --page-size |
| `page_token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --page-token |
| `sort` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --sort |
| `order` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --order |
| `query` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --query, -q |
| `collection_id` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'minimum': 1, 'name': 'integer range'} | --collection |
| `source_store` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --source-store |
| `cache_store` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --cache-store |
| `state` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --state |
| `protection` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --protection |
| `expires_before` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --expires-before |
| `expires_after` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --expires-after |
| `selectors` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --selectors |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |
