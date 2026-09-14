# piggity collection tag contains

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-contains:982a1ade05 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

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

### Result and failure contract

- <a id="s-c17cb15bda"></a>Result identity: `piggity-cli-result/collection/tag/contains/v1`
- <a id="s-5d9a251515"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-aca710ae8f"></a>Structured output: `optional-json`
- <a id="s-d68d425b58"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-cb1e96f23c"></a>`completed` | <a id="s-cce5a8053c"></a>`0` | <a id="s-642d4e9f5f"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-addc816fa3"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-5ce775ed08"></a>`usage` | <a id="s-8ce329d0b4"></a>`2` | <a id="s-56f2161c4c"></a>`{"all":"empty"}` | <a id="s-eacb26b0b9"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-af1778233a"></a>`operational` | <a id="s-92a6d9d9e1"></a>`1` | <a id="s-8c0219d819"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-17e7c8e235"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

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

- [GET /v1/collections/{collection_id}/tags:contains](../../riverhog/http-operations/get-v1-collections-collection-id-tags-contains.md)
- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)

## Governing policies

- <a id="pa-5e02b89273"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-0fcae87cee"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

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
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/result_contract`

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

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/contains/result_contract`

<!-- exact-contract-value: c968a8ed324bdbc50b941234a97755e515e2d045fc8bed6014fa4baabba986a4 -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "stderr": {
        "all": "noncontractual-usage-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    },
    {
      "exit_status": 1,
      "id": "operational",
      "stderr": {
        "human": "noncontractual-diagnostic",
        "json": "empty"
      },
      "stdout": {
        "human": "empty",
        "json": "http-api-contracts.ErrorResponse"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/collection/tag/contains/v1",
  "profile_id": "piggity-cli-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": "named-command-result"
      }
    }
  ]
}
```
