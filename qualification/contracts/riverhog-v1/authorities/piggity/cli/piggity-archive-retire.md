# piggity archive retire

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:piggity:piggity-archive-retire:b6ba8ab347 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [piggity](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-e5a72fb08f"></a>Parser name: `retire`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-74ff7f0ecb"></a>`collection_id` | TyperArgument | yes | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | collection_id |
| <a id="s-1cad34944b"></a>`store` | TyperOption | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --store |
| <a id="s-6334a27c26"></a>`dry_run` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --dry-run, --plan |
| <a id="s-bc3027a32b"></a>`confirm` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --confirm |
| <a id="s-bb4c28ef4b"></a>`json_mode` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --json |

### Result and failure contract

- <a id="s-f7b7d91e17"></a>Result identity: `piggity-cli-result/archive/retire/v1`
- <a id="s-a76ad1c0ea"></a>Profile: `piggity-cli-human-json/v1`
- <a id="s-f0a299c8ac"></a>Structured output: `optional-json`
- <a id="s-e6bb996387"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-99895d5327"></a>`completed` | <a id="s-679543bbd6"></a>`0` | <a id="s-755e242947"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-2f630eeedf"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-8910a67b1f"></a>`usage` | <a id="s-a159384c27"></a>`2` | <a id="s-71881e206f"></a>`{"all":"empty"}` | <a id="s-e56b218030"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-8b235c6ce0"></a>`operational` | <a id="s-29c43e7cf1"></a>`1` | <a id="s-95921aba6c"></a>`{"human":"empty","json":"http-api-contracts.ErrorResponse"}` | <a id="s-a89dbe5a9b"></a>`{"human":"noncontractual-diagnostic","json":"empty"}` |
| <a id="s-8a42e3889d"></a>`blocked` | <a id="s-c8010cb636"></a>`1` | <a id="s-d0711626b5"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-47bee7eb9a"></a>`{"human":"empty"}` |
| <a id="s-f0b56a096d"></a>`confirmation-declined` | <a id="s-a10a6558d9"></a>`1` | <a id="s-bf862a00be"></a>`{"human":"noncontractual-presentation-of-command-result"}` | <a id="s-df55cf1650"></a>`{"human":"noncontractual-diagnostic"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter collection_id](#s-74ff7f0ecb) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --confirm](#s-bc3027a32b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --dry-run](#s-6334a27c26) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --json](#s-bb4c28ef4b) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --store](#s-1cad34944b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/archive/copies/retire](../../riverhog/http-operations/post-v1-archive-copies-retire.md)
- [POST /v1/archive/copies/retirement-plan](../../riverhog/http-operations/post-v1-archive-copies-retirement-plan.md)

## Governing policies

- <a id="pa-2c524e82ec"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-50e6828b95"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:piggity](../../../evidence/sources.md#src-094022231f) — `reference/riverhog/applications/piggity/src/piggity/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/piggity/commands/archive/commands/retire/name`
- `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`
- `/external_contract/cli/piggity/commands/archive/commands/retire/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/piggity/commands/archive/commands/retire/name`

<!-- exact-contract-value: a2a9d4ffbc4c361801bc55cbc97e993b34974d1cb94ba86138a02d7b5630ad91 -->

```json
"retire"
```

### `/external_contract/cli/piggity/commands/archive/commands/retire/parameters`

<!-- exact-contract-value: 116b926952db53e8cab7ae68dd815774c23383fb5dd194c74c89118b771e12fa -->

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
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "store",
    "nargs": 1,
    "options": [
      "--store"
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
    "name": "dry_run",
    "nargs": 1,
    "options": [
      "--dry-run",
      "--plan"
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
    "name": "confirm",
    "nargs": 1,
    "options": [
      "--confirm"
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

### `/external_contract/cli/piggity/commands/archive/commands/retire/result_contract`

<!-- exact-contract-value: e99c37e84bcf9fb7967e367c01e9ff5b94eb2dd6e1e88d8e41adbcafbfd52551 -->

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
    },
    {
      "exit_status": 1,
      "id": "blocked",
      "stderr": {
        "human": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    },
    {
      "exit_status": 1,
      "id": "confirmation-declined",
      "stderr": {
        "human": "noncontractual-diagnostic"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "piggity-cli-result/archive/retire/v1",
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
