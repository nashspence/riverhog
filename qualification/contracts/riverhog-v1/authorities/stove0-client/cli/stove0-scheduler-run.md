# stove0 scheduler run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-scheduler-run:847cbca83e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-009e6012f8"></a>Parser name: `run`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-aa38c45548"></a>`role` | TyperOption | no | {'class': 'typer._click.types.StringParamType', 'name': 'text'} | --role |
| <a id="s-b52681f913"></a>`work_limit` | TyperOption | no | {'class': 'typer._click.types.IntRange', 'maximum': 100, 'minimum': 1, 'name': 'integer range'} | --work-limit |

### Result and failure contract

- <a id="s-cab5b03c4d"></a>Result identity: `stove0-cli-result/scheduler/run/v1`
- <a id="s-5ac66a2861"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a4ed59c974"></a>Structured output: `optional-json`
- <a id="s-e651d1d907"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-3cd8e1e861"></a>`completed` | <a id="s-5f7df994d2"></a>`0` | <a id="s-d30f65713f"></a>`{"human":"noncontractual-presentation-of-command-result","json":"named-command-result"}` | <a id="s-f95d3230bd"></a>`{"all":"empty"}` |

#### Failure outcomes

| Identity | Exit status | stdout | stderr |
|---|---|---|---|
| <a id="s-1781c86dda"></a>`usage` | <a id="s-2ea2f7a35e"></a>`2` | <a id="s-778f47c230"></a>`{"all":"empty"}` | <a id="s-06c843881f"></a>`{"all":"noncontractual-usage-diagnostic"}` |
| <a id="s-0c1c73c97d"></a>`operational` | <a id="s-ef010bcd02"></a>`1` | <a id="s-2aae7aa000"></a>`{"all":"empty"}` | <a id="s-792d0c4b3a"></a>`{"all":"stove0-cli-diagnostic/v1"}` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --role](#s-aa38c45548) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --work-limit](#s-b52681f913) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --work-limit](#s-b52681f913) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/admin/scheduler/run](../../stove0/http-operations/post-v1-admin-scheduler-run.md)

## Governing policies

- <a id="pa-54ac3aa683"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-7fd653835a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/stove0/commands/scheduler/commands/run/name`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/parameters`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/result_contract`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/scheduler/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/parameters`

<!-- exact-contract-value: 198dc1e55bab3f6ecd224ceb7b0ff54337834c094dafda254b965b16a742f4f1 -->

```json
[
  {
    "count": false,
    "default": "combined",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "role",
    "nargs": 1,
    "options": [
      "--role"
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
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "work_limit",
    "nargs": 1,
    "options": [
      "--work-limit"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "class": "typer._click.types.IntRange",
      "maximum": 100,
      "minimum": 1,
      "name": "integer range"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/result_contract`

<!-- exact-contract-value: 74262e883ba2e71b1367357690d2f58f767cd7323de5f6fe4f6fa09b22a701c9 -->

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
  "identity": "stove0-cli-result/scheduler/run/v1",
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
