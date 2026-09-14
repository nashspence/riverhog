# piggity collection tag add

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-collection-tag-add:b899dfacf5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e5409890af"></a>Parser name: `add`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-89f773ed42"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-59325e9744"></a>`tag` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | tag |
| <a id="s-487a06b9dc"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'minimum': 1, 'name': 'integer range'} | --revision |
| <a id="s-a62b2b4357"></a>`tag_set_identity` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --tag-set-identity |
| <a id="s-5f64c2a2b6"></a>`operation_id` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --operation-id |
| <a id="s-0fed947d41"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-e25fa0ddf2"></a>Result identity: `piggity-cli-result/collection/tag/add/v1`
- <a id="s-52746705af"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-e5556369a2"></a>Structured output: `optional-json`
- <a id="s-3858088b78"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-83dbedefc3"></a>`completed` | <a id="s-3d39e7d230"></a>`0` | <a id="s-a15682d4d9"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-799b1e002a"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-0050a03f4e"></a>`usage` | <a id="s-05aae3ab98"></a>`2` | <a id="s-8d69126a78"></a>`{"all":"empty"}` | <a id="s-ac8393fcf1"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-b8e62ed087"></a>`operational` | <a id="s-2a07c84dd5"></a>`1` | <a id="s-3644a24c46"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-cadd31fbbf"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-89f773ed42) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-0fed947d41) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --operation-id](#s-5f64c2a2b6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-487a06b9dc) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter tag](#s-59325e9744) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tag-set-identity](#s-a62b2b4357) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/collections/{collection_id}](../../riverhog/http-operations/get-v1-collections-collection-id.md)
- [POST /v1/collections/{collection_id}/tags:add](../../riverhog/http-operations/post-v1-collections-collection-id-tags-add.md)

## Governing policies

- <a id="pa-d6d9d6405f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ec8af9e9bc"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/name`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/parameters`
- `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/name`

<!-- exact-contract-value: 7b8a6f33b43ca26a3f2aa73e408748f9ceb391ac21dfe746c94563016ab72f85 -->

```json
"add"
```

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/parameters`

<!-- exact-contract-value: 1c409adfdd49b30b7c29eafa845578a90b1754270c23c112375011f77caeccb3 -->

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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "operation_id",
    "nargs": 1,
    "options": [
      "--operation-id"
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

### `/external_contract/cli/piggity/commands/collection/commands/tag/commands/add/result_contract`

<!-- exact-contract-value: e0f8601db9d876292ab49af66054504efa93f8617b1907d68ab3b14bb1e0d470 -->

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
  "identity": "piggity-cli-result/collection/tag/add/v1",
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
