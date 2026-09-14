# stove0 recipe list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-recipe-list:42fc48e96d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9785a5ea66"></a>Parser name: `list`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4d57de814c"></a>`help` | <a id="s-a358d585b7"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-a4817c46a6"></a>`0` | <a id="s-462c16610a"></a>`"noncontractual-framework-help"` | <a id="s-fe89d95a56"></a>`"empty"` |

### Result and failure contract

- <a id="s-edee39bb14"></a>Result identity: `stove0-cli-result/recipe/list/v1`
- <a id="s-ef7734295f"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-da13b40f0d"></a>Structured output: `optional-json`
- <a id="s-73208d05dd"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f0457c88f9"></a>`completed` | <a id="s-12cadb7e1a"></a>`{"kind":"command-completed"}` | <a id="s-f2feb057f0"></a>`0` | <a id="s-f1349e7de6"></a>`human: noncontractual-presentation-of-command-result; json: HTTP list_recipes — #/components/schemas/RecipeCatalogView` | <a id="s-2c2fbdadc0"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-079f1d376b"></a>`usage` | <a id="s-f89324a614"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd2686ceae"></a>`2` | <a id="s-a19663b429"></a>`all: empty` | <a id="s-7b0d2f8785"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-83d7dc4cdb"></a>`operational` | <a id="s-c459c68121"></a>`{"kind":"application-error"}` | <a id="s-68040b2c9d"></a>`1` | <a id="s-fc51d3932e"></a>`all: empty` | <a id="s-911ecf6d08"></a>`all: stove0-cli-diagnostic/v1` |

## Maintained corroboration

### Related interface records

- [GET /v1/recipes](../../stove0/http-operations/get-v1-recipes.md)

## Governing policies

- <a id="pa-6a8ecabb26"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/list/name`
- `/external_contract/cli/stove0/commands/recipe/commands/list/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/recipe/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/recipe/commands/list/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/recipe/commands/list/result_contract`

<!-- exact-contract-value: aa8fcc55521207bcbd5aaf247d122b03a456f5619a32a0439048782084361496 -->

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
        "all": "stove0-cli-diagnostic/v1"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "stove0-cli-result/recipe/list/v1",
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
          "operation_id": "list_recipes",
          "path": "/v1/recipes",
          "schema": {
            "$ref": "#/components/schemas/RecipeCatalogView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/recipe/commands/list/terminating_controls`

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
