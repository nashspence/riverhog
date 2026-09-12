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

## Contract summary

- Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `app_name` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | app_name |
| `allow` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --allow |
| `expires_in` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --expires-in |
| `json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/piggity/commands/app/commands/key/commands/create/parameters`

<!-- exact-contract-value: eeb2732f492dc62d062ab5de787037cf5ad08f71627de03805257de5a00069ac -->

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
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": true,
    "name": "allow",
    "nargs": 1,
    "options": [
      "--allow"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "expires_in",
    "nargs": 1,
    "options": [
      "--expires-in"
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
