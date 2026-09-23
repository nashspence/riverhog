# stove0 recipe list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-recipe-list:efe8d527a8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-9785a5ea66"></a>Parser name: `list`

| Field | Value |
|---|---|
| <a id="s-e104637b6e"></a>`parameters` | `[]` |
- <a id="s-a0fc18a03f"></a>Extra arguments at this parser: rejected.
- <a id="s-9d725bdf1a"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-8b3e18ac27"></a>Unknown options at this parser: rejected.

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
| <a id="s-f0457c88f9"></a>`completed` | <a id="s-12cadb7e1a"></a>`{"kind":"command-completed"}` | <a id="s-f2feb057f0"></a>`0` | <a id="s-f1349e7de6"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_recipes response 200](../../stove0/http-operations/get-v1-recipes.md#s-a3267f057b) | <a id="s-2c2fbdadc0"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-079f1d376b"></a>`usage` | <a id="s-f89324a614"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bd2686ceae"></a>`2` | <a id="s-a19663b429"></a>all: `"empty"` | <a id="s-7b0d2f8785"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-83d7dc4cdb"></a>`operational` | <a id="s-c459c68121"></a>`{"kind":"application-error"}` | <a id="s-68040b2c9d"></a>`1` | <a id="s-fc51d3932e"></a>all: `"empty"` | <a id="s-911ecf6d08"></a>all: `"noncontractual-diagnostic"` |

## Maintained corroboration

### Related interface records

- [GET /v1/recipes](../../stove0/http-operations/get-v1-recipes.md)
- [stove0_api_client.Stove0ApiClient.list_recipes](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-recipes.md)

## Governing policies

- <a id="pa-be87237ec0"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::list\_recipes](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L169)

### Machine authority

- `/external_contract/cli/stove0/commands/recipe/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/recipe/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/recipe/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/recipe/commands/list/name`
- `/external_contract/cli/stove0/commands/recipe/commands/list/parameters`
- `/external_contract/cli/stove0/commands/recipe/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/recipe/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/recipe/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/recipe/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/recipe/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

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

<!-- exact-contract-value: e2c78bd4f6fef1e516a0e29bb2cd95ce0e614b18046fd2e3b65afacca65af37c -->

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

</details>
