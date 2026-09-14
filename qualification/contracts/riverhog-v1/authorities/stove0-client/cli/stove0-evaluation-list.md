# stove0 evaluation list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-list:a81807409d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d26d9f9112"></a>Parser name: `list`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-f50a4411fc"></a>`page_size` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --page-size |
| <a id="s-4963909371"></a>`page_token` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --page-token |
| <a id="s-0d4e4d3089"></a>`phase` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --phase |
| <a id="s-a514dcb5b0"></a>`query` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --query, -q |
| <a id="s-93bf41766c"></a>`sort` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --sort |
| <a id="s-5fdde6abb9"></a>`order` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --order |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-7b858d4c37"></a>`help` | <a id="s-6fcbef24ef"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-6517ecc879"></a>`0` | <a id="s-bfb40854eb"></a>`"noncontractual-framework-help"` | <a id="s-67d2a22bd2"></a>`"empty"` |

### Result and failure contract

- <a id="s-ce45b26fe6"></a>Result identity: `stove0-cli-result/evaluation/list/v1`
- <a id="s-53c91a9300"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-5d7159035e"></a>Structured output: `optional-json`
- <a id="s-a5bc68136f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-232a5e3217"></a>`completed` | <a id="s-652665e4c3"></a>`{"kind":"command-completed"}` | <a id="s-75aea032b2"></a>`0` | <a id="s-8873cb138f"></a>`human: noncontractual-presentation-of-command-result; json: HTTP list_evaluations — #/components/schemas/EvaluationPage` | <a id="s-33183459fc"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7bdaa2d501"></a>`usage` | <a id="s-0e3f50ed22"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-16fefef1e9"></a>`2` | <a id="s-c5c4c4a435"></a>`all: empty` | <a id="s-03440efae2"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-08b6136e37"></a>`operational` | <a id="s-776db2ca39"></a>`{"kind":"application-error"}` | <a id="s-8a90438d98"></a>`1` | <a id="s-cf2a6bdd87"></a>`all: empty` | <a id="s-67dbbce263"></a>`all: stove0-cli-diagnostic/v1` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --order](#s-5fdde6abb9) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-f50a4411fc) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-f50a4411fc) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-4963909371) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --phase](#s-0d4e4d3089) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-a514dcb5b0) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --sort](#s-93bf41766c) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/evaluations](../../stove0/http-operations/get-v1-evaluations.md)

## Governing policies

- <a id="pa-f986c89c8f"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7ee3f294c8"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/list/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/list/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/list/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/list/parameters`

<!-- exact-contract-value: d815a71bab11de6766932cfcd452a0b35bc7b01deff42da278fcf3fd16eb889d -->

```json
[
  {
    "count": false,
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 100,
      "minimum": 1,
      "name": "integer range"
    }
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
    ],
    "required": false,
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
    "name": "phase",
    "nargs": 1,
    "options": [
      "--phase"
    ],
    "required": false,
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
    "name": "query",
    "nargs": 1,
    "options": [
      "--query",
      "-q"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": "updated_at",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "sort",
    "nargs": 1,
    "options": [
      "--sort"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.StringParamType",
      "name": "text"
    }
  },
  {
    "count": false,
    "default": "desc",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "order",
    "nargs": 1,
    "options": [
      "--order"
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

### `/external_contract/cli/stove0/commands/evaluation/commands/list/result_contract`

<!-- exact-contract-value: ed6818eede5470f47280918a5d3f4e553d6c6c8a63d6182ac99a4135d952df5b -->

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
  "identity": "stove0-cli-result/evaluation/list/v1",
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
          "operation_id": "list_evaluations",
          "path": "/v1/evaluations",
          "schema": {
            "$ref": "#/components/schemas/EvaluationPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/evaluation/commands/list/terminating_controls`

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
