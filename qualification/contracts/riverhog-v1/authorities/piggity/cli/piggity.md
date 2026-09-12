# piggity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity:c2de8fb83b -->

| Audit field | Value |
|---|---|
| Authority | `piggity` |
| Interface | `cli` |
| Family | `root` |
| Contract elements | 1 |
| Extent decisions | 3 |

## Machine authority

- `/external_contract/cli/piggity/name`
- `/external_contract/cli/piggity/parameters`

## Effective policies

- `compatibility/cli/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `cli:piggity` — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- Proof: `make dist-smoke`
- Proof: `make operation-qualification`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |
| cardinality | values-per-occurrence | `fixed` | maximum=1, minimum=1, reason=fixed-command-argument-arity |

## Contract summary


### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| `_version` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --version |
| `install_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --install-completion |
| `show_completion` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --show-completion |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/name`

<!-- exact-contract-value: 12ae32cb1ec02d01eda3581b127c1fee3b0dc53572ed6baf239721a03d82e126 -->

```json
""
```

### `/external_contract/cli/piggity/parameters`

<!-- exact-contract-value: 6c3eaa1e80ed7eec26f0605768d3652ccdb37504c04b8b85a9dc9a439b170363 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "_version",
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
