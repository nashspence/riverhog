# piggity collection describe

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-describe:d9879d2658 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [collection](families/collection/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- <a id="s-2dd51f2dda"></a>Parser name: `describe`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-a79c8f4d1c"></a>`collection` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection |
| <a id="s-c6fc67bf05"></a>`description` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --description |
| <a id="s-6146b8bc01"></a>`clear` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --clear |
| <a id="s-fe5337d00f"></a>`if_match` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --if-match |
| <a id="s-92d36de323"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --clear](#s-6146b8bc01) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter collection](#s-a79c8f4d1c) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --description](#s-c6fc67bf05) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --if-match](#s-fe5337d00f) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-92d36de323) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_collection](../../riverhog/operation/operation-parity-get-collection.md)
- [Operation parity: replace_collection_description](../../riverhog/operation/operation-parity-replace-collection-description.md)

## Governing policies

- <a id="pa-117c0eba81"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-319463811e"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/describe/name`
- `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/describe/name`

<!-- exact-contract-value: ccbb85a554fc61cc780e2cae6cc0e75e15a01539011884b8e460657a860ded8e -->

```json
"describe"
```

### `/external_contract/cli/piggity/commands/collection/commands/describe/parameters`

<!-- exact-contract-value: 645f658239e15189970ff762b7508b23e08fa999995a7f8ecbed88e0376a7925 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection",
    "nargs": 1,
    "options": [
      "collection"
    ],
    "required": true,
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
    "name": "description",
    "nargs": 1,
    "options": [
      "--description"
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
    "name": "clear",
    "nargs": 1,
    "options": [
      "--clear"
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
    "name": "if_match",
    "nargs": 1,
    "options": [
      "--if-match"
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
