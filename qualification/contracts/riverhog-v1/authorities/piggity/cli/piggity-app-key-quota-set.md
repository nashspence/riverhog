# piggity app key quota set

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-app-key-quota-set:d2e9dff321 -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `app` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/name`
- `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Related interface records

- [Operation parity: set_app_key_download_quota](../../riverhog/operation/operation-parity-set-app-key-download-quota.md)

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary

- Parser name: `set`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| `key_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | key_id |
| `limit` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | limit |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/name`

<!-- exact-contract-value: c7f5814b92ec9430648136406d844823df66e1af010dfa9456b5ec7ff5017f8b -->

```json
"set"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/quota/commands/set/parameters`

<!-- exact-contract-value: 638090c9b4c8b867084196d21d13a4c6d162923fdd5cb85b06f606947f84e108 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "app_name",
    "nargs": 1,
    "options": [
      "app_name"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "key_id",
    "nargs": 1,
    "options": [
      "key_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "limit"
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
