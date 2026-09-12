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

## Contract summary


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

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/name`

<!-- exact-contract-value: 12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126 -->

```json
""
```

### `/external_contract/cli/stove0/parameters`

<!-- exact-contract-value: 27817a8b5af625ab0a68de36b2bb2e1429d160e66a7ebcca783657718cd8b547 -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "version",
    "nargs": 1,
    "options": [
      "--version"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "base_url",
    "nargs": 1,
    "options": [
      "--base-url"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "token",
    "nargs": 1,
    "options": [
      "--token"
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
    "name": "json_output",
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
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "allow_insecure_http",
    "nargs": 1,
    "options": [
      "--allow-insecure-http"
    ],
    "required": false,
    "secondary_options": [
      "--no-allow-insecure-http"
    ],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "install_completion",
    "nargs": 1,
    "options": [
      "--install-completion"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "show_completion",
    "nargs": 1,
    "options": [
      "--show-completion"
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
