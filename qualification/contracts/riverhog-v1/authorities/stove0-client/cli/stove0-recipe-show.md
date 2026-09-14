# stove0 recipe show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-recipe-show:e8b7581061 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-116f2b9e63"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-66aae1f8d7"></a>`recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| <a id="s-cda51628b9"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |

### Result and failure contract

- <a id="s-1f6f36e48a"></a>Result identity: `stove0-cli-result/recipe/show/v1`
- <a id="s-9b412e87f9"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-516bf94843"></a>Structured output: `optional-json`
- <a id="s-6dee1f43e5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-1d2a22e58c"></a>`completed` | <a id="s-8842546ce2"></a>`0` | <a id="s-db87fee5b5"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-89f8a7ee86"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-8c79485a28"></a>`usage` | <a id="s-21688e16b9"></a>`2` | <a id="s-e6afc2507c"></a>`{"all":"empty"}` | <a id="s-7560ccf2c1"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-c962263214"></a>`operational` | <a id="s-5a7852a0c1"></a>`1` | <a id="s-66bc85e664"></a>`{"all":"empty"}` | <a id="s-90217cc27c"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter recipe_id](#s-66aae1f8d7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-cda51628b9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/recipes/{recipe_id}](../../stove0/http-operations/get-v1-recipes-recipe-id.md)

## Governing policies

- <a id="pa-4349300657"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-43c7a37bac"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/show/name`
- `/external_contract/cli/stove0/commands/recipe/commands/show/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/show/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/recipe/commands/show/parameters`

<!-- exact-contract-value: b1ae0d39b9c52a02c16ef66a0728c68179d0e233305ec5e56c7fc2d9f0dfe8b5 -->

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
  }
]
```

### `/external_contract/cli/stove0/commands/recipe/commands/show/result_contract`

<!-- exact-contract-value: d724ed75af7874e691ed48d1b369b068b98ae4e12916be8da6e79931a1ec5b9b -->

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
        "all": "stove0-cli-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/recipe/show/v1",
  "profile_id": "stove0-cli-human-json/v1",
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
