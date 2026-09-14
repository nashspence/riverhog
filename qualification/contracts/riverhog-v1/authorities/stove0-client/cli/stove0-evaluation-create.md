# stove0 evaluation create

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-create:371e523eef -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-0639b8f049"></a>Parser name: `create`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-64aae09091"></a>`definition` | TyperArgument | yes | {'class': 'typer.models.TyperPath', 'name': 'path'} | definition |

### Result and failure contract

- <a id="s-74eacf6ac7"></a>Result identity: `stove0-cli-result/evaluation/create/v1`
- <a id="s-ff5977ae08"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a868448080"></a>Structured output: `optional-json`
- <a id="s-7cf3e9237c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-46af80b7b7"></a>`completed` | <a id="s-d3f0e53b88"></a>`0` | <a id="s-9b426931bf"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-ccdaa139a2"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-30d4cc4c6d"></a>`usage` | <a id="s-c0aa9d39b5"></a>`2` | <a id="s-f451ed53d8"></a>`{"all":"empty"}` | <a id="s-7e01cc7178"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-374c3d9cbe"></a>`operational` | <a id="s-bc6f34f455"></a>`1` | <a id="s-0b972869d2"></a>`{"all":"empty"}` | <a id="s-ff39e53b14"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter definition](#s-64aae09091) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations](../../stove0/http-operations/post-v1-evaluations.md)

## Governing policies

- <a id="pa-b50436a903"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-4d1946aa66"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/create/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/create/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/create/name`

<!-- exact-contract-value: 5498a731a187f424a5800943afcba027f3a6cd684e38fe6e40c02bee1753152d -->

```json
"create"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/parameters`

<!-- exact-contract-value: 4fb330ee8d838285ee3bb2f2f8c99dccc40350eb1951a541aaaddd3d0f750f22 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "definition",
    "nargs": 1,
    "options": [
      "definition"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer.models.TyperPath",
      "name": "path"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/evaluation/commands/create/result_contract`

<!-- exact-contract-value: b0cff0c6ebb956bb63654dd1058fd123037569ecb965324a4940c7aacf1f2532 -->

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
  "identity": "stove0-cli-result/evaluation/create/v1",
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
