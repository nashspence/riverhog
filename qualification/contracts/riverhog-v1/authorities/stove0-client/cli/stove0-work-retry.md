# stove0 work retry

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-retry:8761a69fd1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1214a560d0"></a>Parser name: `retry`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-785be0640b"></a>`work_id` | TyperArgument | yes | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | work_id |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8e5029c691"></a>`help` | <a id="s-ab4d4a82a5"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-d948352481"></a>`0` | <a id="s-cb26cd69ae"></a>`"noncontractual-framework-help"` | <a id="s-91eca05a60"></a>`"empty"` |

### Result and failure contract

- <a id="s-fb59c29cef"></a>Result identity: `stove0-cli-result/work/retry/v1`
- <a id="s-5b3cf1f858"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-2d34286d6d"></a>Structured output: `optional-json`
- <a id="s-bdc6ab67e0"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-437987b251"></a>`completed` | <a id="s-87acfd3a5f"></a>`{"kind":"command-completed"}` | <a id="s-366b650490"></a>`0` | <a id="s-36208c901c"></a>`human: noncontractual-presentation-of-command-result; json: HTTP retry_work — #/components/schemas/WorkView` | <a id="s-6adbe3d4a6"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cfe509833e"></a>`usage` | <a id="s-3c3c42beb9"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2cfe0a74a0"></a>`2` | <a id="s-f23c2b95e0"></a>`all: empty` | <a id="s-41c3b7b094"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-b68aaa7ca6"></a>`operational` | <a id="s-94b6bc197a"></a>`{"kind":"application-error"}` | <a id="s-8ce01eb716"></a>`1` | <a id="s-c7a9af1353"></a>`all: empty` | <a id="s-beb87bc14c"></a>`all: noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-785be0640b) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/retry](../../stove0/http-operations/post-v1-work-work-id-retry.md)

## Governing policies

- <a id="pa-ed54744c73"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-aa6e9288c9"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/retry/name`
- `/external_contract/cli/stove0/commands/work/commands/retry/parameters`
- `/external_contract/cli/stove0/commands/work/commands/retry/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/retry/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/retry/name`

<!-- exact-contract-value: edf916f660660da65a1f21d7ab77d99621262f447acf39a4202ea81551863e66 -->

```json
"retry"
```

### `/external_contract/cli/stove0/commands/work/commands/retry/parameters`

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

### `/external_contract/cli/stove0/commands/work/commands/retry/result_contract`

<!-- exact-contract-value: 51edb57f3e7c46e151dd6cbd3cdbaf93a10c951962b585fba120467241408f9b -->

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
  "identity": "stove0-cli-result/work/retry/v1",
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
          "method": "POST",
          "operation_id": "retry_work",
          "path": "/v1/work/{work_id}/retry",
          "schema": {
            "$ref": "#/components/schemas/WorkView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/work/commands/retry/terminating_controls`

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
