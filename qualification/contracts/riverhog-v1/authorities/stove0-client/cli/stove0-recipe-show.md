# stove0 recipe show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-recipe-show:cc48e521c3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-116f2b9e63"></a>Parser name: `show`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-66aae1f8d7"></a>`recipe_id`<br>`recipe_id` | required positional; 1 value | text | not recorded |
| <a id="s-cda51628b9"></a>`revision`<br>`--revision` | optional option; 1 value | integer | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fe98ed1543"></a>`help` | <a id="s-800a8412cf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-33ff0af9d9"></a>`0` | <a id="s-0db5a9e619"></a>`"noncontractual-framework-help"` | <a id="s-0c6c86fc60"></a>`"empty"` |

### Result and failure contract

- <a id="s-1f6f36e48a"></a>Result identity: `stove0-cli-result/recipe/show/v1`
- <a id="s-9b412e87f9"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-516bf94843"></a>Structured output: `optional-json`
- <a id="s-6dee1f43e5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1d2a22e58c"></a>`completed` | <a id="s-7a4b639c36"></a>`{"kind":"command-completed"}` | <a id="s-8842546ce2"></a>`0` | <a id="s-db87fee5b5"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_recipe response 200](../../stove0/http-operations/get-v1-recipes-recipe-id.md#s-ffae93a9f9) | <a id="s-89f8a7ee86"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8c79485a28"></a>`usage` | <a id="s-97c69ee2f2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-21688e16b9"></a>`2` | <a id="s-e6afc2507c"></a>all: `empty` | <a id="s-7560ccf2c1"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-c962263214"></a>`operational` | <a id="s-7ff6451ba8"></a>`{"kind":"application-error"}` | <a id="s-5a7852a0c1"></a>`1` | <a id="s-66bc85e664"></a>all: `empty` | <a id="s-90217cc27c"></a>all: `noncontractual-diagnostic` |

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
- [stove0_api_client.Stove0ApiClient.get_recipe](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-recipe.md)

## Governing policies

- <a id="pa-4fc193e785"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1062bd8eb0"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::show_recipe](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L175)

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/show/name`
- `/external_contract/cli/stove0/commands/recipe/commands/show/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/recipe/commands/show/terminating_controls`

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

<!-- exact-contract-value: 0ccd416856e8d691287c3bc72e57b78cbeaace09009b9d3257acee7d0f67e08e -->

```json
{
  "failures": [
    {
      "exit_status": 2,
      "id": "usage",
      "selected_by": {
        "kind": "parser-rejected-invocation"
      },
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
      "selected_by": {
        "kind": "application-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
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
      "selected_by": {
        "kind": "command-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "application": "stove0",
          "kind": "http-operation-response",
          "method": "GET",
          "operation_id": "get_recipe",
          "path": "/v1/recipes/{recipe_id}",
          "schema": {
            "$ref": "#/components/schemas/RecipeView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/recipe/commands/show/terminating_controls`

<!-- exact-contract-value: 654ffd6937a42b17b4204e0750fd74bd42751d2241f4a4632015efb38811a79c -->

```json
[
  {
    "exit_status": 0,
    "id": "help",
    "stderr": "empty",
    "stdout": "noncontractual-framework-help",
    "trigger": {
      "kind": "option-present",
      "options": [
        "--help"
      ]
    }
  }
]
```
