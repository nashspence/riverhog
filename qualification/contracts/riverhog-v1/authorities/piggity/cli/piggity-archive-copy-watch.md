# piggity archive copy watch

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-copy-watch:b6d2f5361a -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `archive` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`
- `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: get_archive_copy_job](../../riverhog/operation/operation-parity-get-archive-copy-job.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `watch`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `selector` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selector |
| `interval` | TyperOption | no | {'class': 'typer._click.types.FloatRange', 'minimum': 0.1, 'name': 'float range'} | --interval |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/name`

<!-- exact-contract-value: 73da76bff71a604995ddd94e223ffa8b7c171b54e0a953c0fb794ac85a61534b -->

```json
"watch"
```

### `/external_contract/cli/piggity/commands/archive/commands/copy/commands/watch/parameters`

<!-- exact-contract-value: eb6e1b2a1271291e3a99a3c15afd19951a252010905621030980f9b269a6d741 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selector",
    "nargs": 1,
    "options": [
      "selector"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": 1,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "interval",
    "nargs": 1,
    "options": [
      "--interval"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.FloatRange",
      "minimum": 0.1,
      "name": "float range"
    }
  },
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "json_mode",
    "nargs": 1,
    "options": [
      "--json"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```
