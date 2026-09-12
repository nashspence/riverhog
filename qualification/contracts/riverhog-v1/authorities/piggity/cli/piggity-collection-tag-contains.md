# piggity collection tag contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-contains:6ea4522e5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [cli](index.md) |
| Family | [collection](families/collection/index.md) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

- <a id="s-b569d0f93b"></a>Parser name: `contains`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-26476668d5"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-2482b9c520"></a>`tag` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | tag |
| <a id="s-4f6f468c54"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'minimum': 1, 'name': 'integer range'} | --revision |
| <a id="s-3380feb855"></a>`tag_set_identity` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag-set-identity |
| <a id="s-dbfd84e078"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-26476668d5) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-dbfd84e078) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-4f6f468c54) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter tag](#s-2482b9c520) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tag-set-identity](#s-3380feb855) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [Operation parity: get_collection](../../riverhog/operation/operation-parity-get-collection.md)
- [Operation parity: collection_contains_tag](../../riverhog/operation/operation-parity-collection-contains-tag.md)

## Governing policies

- <a id="pa-e446c8be87"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-02f5d08e23"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/parameters`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/name`

<!-- exact-contract-value: dde0e3dc0ad9a0daf42c9d0ab5a9a1c466093b12ccae1ca4c613754a34526930 -->

```json
"contains"
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/parameters`

<!-- exact-contract-value: add53ce5019710415eb0f73299d64482e6cf7c94e539208b0b4120bf5aabd62f -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "collection_id",
    "nargs": 1,
    "options": [
      "collection_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntParamType",
      "name": "integer"
    }
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "tag",
    "nargs": 1,
    "options": [
      "tag"
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
      "class": "typer._click.types.IntRange",
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "tag_set_identity",
    "nargs": 1,
    "options": [
      "--tag-set-identity"
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
