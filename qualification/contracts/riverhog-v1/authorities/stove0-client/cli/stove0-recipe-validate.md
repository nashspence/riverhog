# stove0 recipe validate

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-recipe-validate:160d004957 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c2446b375b"></a>Parser name: `validate`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-b8713f6067"></a>`path` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'file'} | path |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5d3d16ac70"></a>`help` | <a id="s-497325c2c6"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-dfe8d2ca7f"></a>`0` | <a id="s-2eb3f595b2"></a>`"noncontractual-framework-help"` | <a id="s-0a3310add6"></a>`"empty"` |

### Result and failure contract

- <a id="s-25d9608b6e"></a>Result identity: `stove0-cli-result/recipe/validate/v1`
- <a id="s-453cf15582"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-f97c934fa9"></a>Structured output: `optional-json`
- <a id="s-92eade532c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c21881a37c"></a>`completed` | <a id="s-20c5e936c6"></a>`{"kind":"command-completed"}` | <a id="s-5dd3e89b2b"></a>`0` | <a id="s-a921e6957c"></a>human: `noncontractual-presentation-of-command-result`; json: [stove0-recipe-catalog-validation/v1](#s-a921e6957c) | <a id="s-6c21a31a21"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a468e4be4c"></a>`usage` | <a id="s-6bee01f5c8"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-22932551d7"></a>`2` | <a id="s-4562e7c916"></a>all: `empty` | <a id="s-1a8131ce7d"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-d9580518c5"></a>`operational` | <a id="s-4170cf0b97"></a>`{"kind":"application-error"}` | <a id="s-cd98d79afb"></a>`1` | <a id="s-c1bc01a565"></a>all: `empty` | <a id="s-0d4ee34a0b"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter path](#s-b8713f6067) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-022a496645"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-86af34119c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/validate/name`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/result_contract`
- `/external_contract/cli/stove0/commands/recipe/commands/validate/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/validate/name`

<!-- exact-contract-value: 2c9877104bf173f3ecf6b4d44be3e77da3b1b3c87e448be5f6cec01b12ccf804 -->

```json
"validate"
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/parameters`

<!-- exact-contract-value: 16bb2841bd48031955d2caf741ae9c4ff040273016852cfe532eb80ee09dbe8b -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "path",
    "nargs": 1,
    "options": [
      "path"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "file"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/result_contract`

<!-- exact-contract-value: 363eb4734df737d7aaf62a2476702b1f8d51814aaeda6154e3f97958d731e9b1 -->

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
  "identity": "stove0-cli-result/recipe/validate/v1",
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
          "identity": "stove0-recipe-catalog-validation/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "catalog_sha256": {
                "pattern": "^[0-9a-f]{64}$",
                "type": "string"
              },
              "format": {
                "const": "stove0-recipe-catalog-validation/v1"
              },
              "operation_count": {
                "minimum": 0,
                "type": "integer"
              },
              "recipe_count": {
                "minimum": 0,
                "type": "integer"
              },
              "recipes": {
                "items": {
                  "type": "object"
                },
                "type": "array"
              }
            },
            "required": [
              "format",
              "catalog_sha256",
              "operation_count",
              "recipe_count",
              "recipes"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/recipe/commands/validate/terminating_controls`

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
