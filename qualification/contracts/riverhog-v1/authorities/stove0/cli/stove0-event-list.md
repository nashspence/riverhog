# stove0 event list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-event-list:6ce5ed27c4 -->

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `cli` |
| Family | `event` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/stove0/commands/event/commands/list/name`
- `/external_contract/cli/stove0/commands/event/commands/list/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:stove0` — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: list_events](../operation/operation-parity-list-events.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| value | cli-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `after` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --after |
| `limit` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --limit |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/event/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/event/commands/list/parameters`

<!-- exact-contract-value: c5a15fad184dce9004bf6a5d73b682313fdbe44cfb6323c6bd53c3cb57f198e6 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "after",
    "nargs": 1,
    "options": [
      "--after"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": 100,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "--limit"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 100,
      "minimum": 1,
      "name": "integer range"
    }
  }
]
```
