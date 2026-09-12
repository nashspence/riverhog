# piggity catalog-sync changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-catalog-sync-changes:98f7416749 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `catalog-sync` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/name`
- `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: list_catalog_sync_changes](../../riverhog/operation/operation-parity-list-catalog-sync-changes.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| value | cli-value | `contract_max` | maximum=100, minimum=1, reason=schema-maximum |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `changes`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `cursor` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --cursor |
| `limit` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --limit |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/name`

<!-- exact-contract-value: 1914b3c81072ec685b513a4497b8db642b0328afb8aeaede6dd78a6311879dbe -->

```json
"changes"
```

### `/external_contract/cli/piggity/commands/catalog-sync/commands/changes/parameters`

<!-- exact-contract-value: c6bd6c88bc65afa601c5ff23dda19db38e2fea7fd510f20cf16a9bd7a471c4ef -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "cursor",
    "nargs": 1,
    "options": [
      "--cursor"
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
