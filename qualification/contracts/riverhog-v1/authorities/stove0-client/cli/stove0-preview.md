# stove0 preview

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-preview:db03fccdfb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-df3533fcff"></a>Parser name: `preview`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-90627304a6"></a>`recipe_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | recipe_id |
| <a id="s-f6a4b5a531"></a>`inputs` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | inputs |
| <a id="s-4f9f8ea3f9"></a>`revision` | TyperOption | no | {'class': 'typer._click.types.IntParamType', 'name': 'integer'} | --revision |
| <a id="s-4ed179ad38"></a>`intent` | TyperOption | no | {'class': 'typer.models.TyperPath', 'name': 'file'} | --intent |

### Result and failure contract

- <a id="s-f0d6d9f631"></a>Result identity: `stove0-cli-result/preview/v1`
- <a id="s-d94100912f"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-90e981514f"></a>Structured output: `optional-json`
- <a id="s-3ff143de55"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-2f61bb447e"></a>`completed` | <a id="s-dc75be26dd"></a>`0` | <a id="s-90f8319fa1"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-2a3db21a11"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-eecaf28643"></a>`usage` | <a id="s-0f3aff7714"></a>`2` | <a id="s-985ca4bcd1"></a>`{"all":"empty"}` | <a id="s-59db1ec1ac"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-49a0fea184"></a>`operational` | <a id="s-4cbe8d680b"></a>`1` | <a id="s-fc7a2b7d11"></a>`{"all":"empty"}` | <a id="s-9a97297e4b"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --intent](#s-4ed179ad38) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter recipe_id](#s-90627304a6) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --revision](#s-4f9f8ea3f9) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/workflow-previews](../../stove0/http-operations/post-v1-workflow-previews.md)

## Governing policies

- <a id="pa-6e9f89eccf"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-79124938dd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/preview/name`
- `/external_contract/cli/stove0/commands/preview/parameters`
- `/external_contract/cli/stove0/commands/preview/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/preview/name`

<!-- exact-contract-value: 99e505dc299c68bc16363d9d8ee5b76599ce41e2154ddb0dc8e2a00c57dec7a5 -->

```json
"preview"
```

### `/external_contract/cli/stove0/commands/preview/parameters`

<!-- exact-contract-value: fbd2f667e95fa41096ff61d53335fbd2ff64a22dd74a9f1a7fadbbba9ab33231 -->

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
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "inputs",
    "nargs": -1,
    "options": [
      "inputs"
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
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "intent",
    "nargs": 1,
    "options": [
      "--intent"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "file"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/preview/result_contract`

<!-- exact-contract-value: 6625a09dfa309d6cef65d2fd88dc23ccd7cb751445eaa96d3b15d950c36acb8c -->

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
  "identity": "stove0-cli-result/preview/v1",
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
