# stove0 work coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-coordination:f77a446372 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d917371341"></a>Parser name: `coordination`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-8b4c2e9366"></a>`work_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | work_id |

### Result and failure contract

- <a id="s-755b3978c4"></a>Result identity: `stove0-cli-result/work/coordination/v1`
- <a id="s-883df1c153"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-05e02d1df1"></a>Structured output: `optional-json`
- <a id="s-8b3bf55a6a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-95a2520682"></a>`completed` | <a id="s-3fa44044aa"></a>`0` | <a id="s-e9c0f1feb5"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-0f79daf1bc"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-e971db28b3"></a>`usage` | <a id="s-bf408e532d"></a>`2` | <a id="s-497fcfdef2"></a>`{"all":"empty"}` | <a id="s-38736f8345"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-39e91f4c50"></a>`operational` | <a id="s-710ada742e"></a>`1` | <a id="s-e187198243"></a>`{"all":"empty"}` | <a id="s-2ac6c55e66"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-8b4c2e9366) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}/coordination](../../stove0/http-operations/get-v1-work-work-id-coordination.md)

## Governing policies

- <a id="pa-1c46d0202a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-1d4127a71d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/coordination/name`
- `/external_contract/cli/stove0/commands/work/commands/coordination/parameters`
- `/external_contract/cli/stove0/commands/work/commands/coordination/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/coordination/name`

<!-- exact-contract-value: c6c6c1f431776373875764cc5d56c4d9db9a1fa15c32dfea2b2ed67ecbe770d4 -->

```json
"coordination"
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/parameters`

<!-- exact-contract-value: 817c2e603d886c184e6cc1469f89564372168f34c065c797f0d455ecaece1909 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "work_id",
    "nargs": 1,
    "options": [
      "work_id"
    ],
    "required": true,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/result_contract`

<!-- exact-contract-value: 53ed27b628287ccc4cfa1f3248f81b6947ad37006c30b62d51966d4edae96cdf -->

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
  "identity": "stove0-cli-result/work/coordination/v1",
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
