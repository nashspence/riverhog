# stove0 preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0:stove0-preview:5f4659b542 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0](../index.md) |
| Interface | [cli](index.md) |
| Family | [preview](families/preview/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

- <a id="s-df3533fcff"></a>Parser name: `preview`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-90627304a6"></a>`recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| <a id="s-f6a4b5a531"></a>`inputs` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | inputs |
| <a id="s-4f9f8ea3f9"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |
| <a id="s-4ed179ad38"></a>`intent` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'file'} | --intent |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --intent](#s-4ed179ad38) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter recipe_id](#s-90627304a6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-4f9f8ea3f9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: preview_workflow](../operation/operation-parity-preview-workflow.md)

## Governing policies

- <a id="pa-8f73c51364"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ccafece340"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/preview/name`
- `/external_contract/cli/stove0/commands/preview/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/preview/name`

<!-- exact-contract-value: 99e505dc299c68bc16363d9d8ee5b76599ce41e2154ddb0dc8e2a00c57dec7a5 -->

```json
"preview"
```

### `/external_contract/cli/stove0/commands/preview/parameters`

<!-- exact-contract-value: fbd2f667e95fa41096ff61d53335fbd2ff64a22dd74a9f1a7fadbbba9ab33231 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "recipe_id",
    "nargs": 1,
    "options": [
      "recipe_id"
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
    "name": "inputs",
    "nargs": -1,
    "options": [
      "inputs"
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
    "name": "revision",
    "nargs": 1,
    "options": [
      "--revision"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "intent",
    "nargs": 1,
    "options": [
      "--intent"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "file"
    }
  }
]
```
