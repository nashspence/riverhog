# stove0 health

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-health:0bf3a60dd9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dd0f6f8ecb"></a>Parser name: `health`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-0148aceb45"></a>`ready` | TyperOption | no | {'class': 'typer._click.types.BoolParamType', 'name': 'boolean'} | --ready |

### Result and failure contract

- <a id="s-5d8c322e3f"></a>Result identity: `stove0-cli-result/health/v1`
- <a id="s-42231855ed"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-df5db33ebc"></a>Structured output: `optional-json`
- <a id="s-6adea3f0b3"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-72134d018e"></a>`completed` | <a id="s-324e8e1f27"></a>`0` | <a id="s-da84358252"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-99cbfd66ae"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-6c139261cd"></a>`usage` | <a id="s-575c71b251"></a>`2` | <a id="s-ebf1c9f740"></a>`{"all":"empty"}` | <a id="s-1e06c2e43c"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-0e2aeffdcf"></a>`operational` | <a id="s-8632e797e5"></a>`1` | <a id="s-748d755ba1"></a>`{"all":"empty"}` | <a id="s-398ef99981"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --ready](#s-0148aceb45) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /health/live](../../stove0/http-operations/get-health-live.md)
- [GET /health/ready](../../stove0/http-operations/get-health-ready.md)

## Governing policies

- <a id="pa-da4b3702ba"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c7ced6581d"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/health/name`
- `/external_contract/cli/stove0/commands/health/parameters`
- `/external_contract/cli/stove0/commands/health/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/health/name`

<!-- exact-contract-value: 69b7c75b4a0260f2e018aae0172314b00e80b3899c79ba4d71e070ba74cd8db4 -->

```json
"health"
```

### `/external_contract/cli/stove0/commands/health/parameters`

<!-- exact-contract-value: bb7aa89b48a0fb797521cb9cfad5a62b55dce8cb3a19bcda2acbd80306355cb4 -->

```json
[
  {
    "count": false,
    "default": false,
    "envvar": null,
    "is_flag": true,
    "kind": "TyperOption",
    "multiple": false,
    "name": "ready",
    "nargs": 1,
    "options": [
      "--ready"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.BoolParamType",
      "name": "boolean"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/health/result_contract`

<!-- exact-contract-value: c7454f7dd1fcdb3111d0e7e4ef0a8818d6da1cdba1761ebb86939d5111ce3a8b -->

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
  "identity": "stove0-cli-result/health/v1",
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
