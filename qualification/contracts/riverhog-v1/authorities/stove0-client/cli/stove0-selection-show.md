# stove0 selection show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-selection-show:56892f4eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-631ffef902"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-3a8e06a180"></a>`selection_sha256` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | selection_sha256 |
| <a id="s-5f3ed31399"></a>`continuation` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --continuation |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c17e8b12f7"></a>`help` | <a id="s-59885f30e5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-cf7e66534e"></a>`0` | <a id="s-1ef2df6556"></a>`"noncontractual-framework-help"` | <a id="s-a539cdc0b0"></a>`"empty"` |

### Result and failure contract

- <a id="s-d210b84233"></a>Result identity: `stove0-cli-result/selection/show/v1`
- <a id="s-0b6284761d"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a27d7b6dc8"></a>Structured output: `optional-json`
- <a id="s-ace4c7429c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c6c631bec6"></a>`completed` | <a id="s-ea28642db4"></a>`{"kind":"command-completed"}` | <a id="s-7823741763"></a>`0` | <a id="s-efde2ec553"></a>`human: noncontractual-presentation-of-command-result; json: HTTP get_artifact_selection — #/components/schemas/ArtifactSelectionPage` | <a id="s-d1d06225ac"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-fd43bf3579"></a>`usage` | <a id="s-17f7f65a97"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-c77811de56"></a>`2` | <a id="s-74e235e593"></a>`all: empty` | <a id="s-00f14ac540"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-2a69054a57"></a>`operational` | <a id="s-945853de8e"></a>`{"kind":"application-error"}` | <a id="s-6daeab7dea"></a>`1` | <a id="s-70784d34b1"></a>`all: empty` | <a id="s-f4d855a9a3"></a>`all: stove0-cli-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --continuation](#s-5f3ed31399) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter selection_sha256](#s-3a8e06a180) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/artifact-selections/{selection_sha256}](../../stove0/http-operations/get-v1-artifact-selections-selection-sha256.md)

## Governing policies

- <a id="pa-dd8e886884"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-5fb90c5893"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/selection/commands/show/name`
- `/external_contract/cli/stove0/commands/selection/commands/show/parameters`
- `/external_contract/cli/stove0/commands/selection/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/selection/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/selection/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/selection/commands/show/parameters`

<!-- exact-contract-value: 0b10d00386773bfc4b66ab848fefb0e9ce0dfa31cd1793be7c11acef3238bc00 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "selection_sha256",
    "nargs": 1,
    "options": [
      "selection_sha256"
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
    "name": "continuation",
    "nargs": 1,
    "options": [
      "--continuation"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/selection/commands/show/result_contract`

<!-- exact-contract-value: 1d5611700ab8e039e98abb8c91082758ffe1d20c0a9287bda3cfd4b41a4eb202 -->

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
  "identity": "stove0-cli-result/selection/show/v1",
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
          "operation_id": "get_artifact_selection",
          "path": "/v1/artifact-selections/{selection_sha256}",
          "schema": {
            "$ref": "#/components/schemas/ArtifactSelectionPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/selection/commands/show/terminating_controls`

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
