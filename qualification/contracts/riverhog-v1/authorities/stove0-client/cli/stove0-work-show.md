# stove0 work show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-show:a8d60981c2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d9cdd32a1b"></a>Parser name: `show`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-b1c892ea3c"></a>`work_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | work_id |

### Result and failure contract

- <a id="s-2fb66510a4"></a>Result identity: `stove0-cli-result/work/show/v1`
- <a id="s-978130c87e"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-54930cbc7d"></a>Structured output: `optional-json`
- <a id="s-914565836f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-629b6d149c"></a>`completed` | <a id="s-b5e6315ee6"></a>`0` | <a id="s-86be555bfb"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-77c9561fc0"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-8a8621d16c"></a>`usage` | <a id="s-4221ec3012"></a>`2` | <a id="s-6ebeaf1e2e"></a>`{"all":"empty"}` | <a id="s-1a8a62cf86"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-60cc34c12b"></a>`operational` | <a id="s-d7a29dfafa"></a>`1` | <a id="s-4482655236"></a>`{"all":"empty"}` | <a id="s-a6ce542e8b"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-b1c892ea3c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}](../../stove0/http-operations/get-v1-work-work-id.md)

## Governing policies

- <a id="pa-9368dcb521"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-75bd4de90a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/show/name`
- `/external_contract/cli/stove0/commands/work/commands/show/parameters`
- `/external_contract/cli/stove0/commands/work/commands/show/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/work/commands/show/parameters`

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

### `/external_contract/cli/stove0/commands/work/commands/show/result_contract`

<!-- exact-contract-value: 57306be99d9792283d4330a5ca74c787e21cf6e44f1d3410f5db1a147587ec89 -->

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
  "identity": "stove0-cli-result/work/show/v1",
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
