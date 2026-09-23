# stove0 scheduler run

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-scheduler-run:cffcc800c4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-009e6012f8"></a>Parser name: `run`
- <a id="s-f8a17ffde9"></a>Extra arguments at this parser: rejected.
- <a id="s-680433515f"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-8e29c74190"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-aa38c45548"></a>`role`<br>`--role` | optional option; 1 value | text | `"combined"`<br>Env: `null` |
| <a id="s-b52681f913"></a>`work_limit`<br>`--work-limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-8e0f1cf848"></a>`help` | <a id="s-5a797c5942"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-8c322c5726"></a>`0` | <a id="s-1e251dc9fd"></a>`"noncontractual-framework-help"` | <a id="s-527d58000d"></a>`"empty"` |

### Result and failure contract

- <a id="s-cab5b03c4d"></a>Result identity: `stove0-cli-result/scheduler/run/v1`
- <a id="s-5ac66a2861"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-a4ed59c974"></a>Structured output: `optional-json`
- <a id="s-e651d1d907"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3cd8e1e861"></a>`completed` | <a id="s-79e944ee58"></a>`{"kind":"command-completed"}` | <a id="s-5f7df994d2"></a>`0` | <a id="s-d30f65713f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP run_scheduler response 200](../../stove0/http-operations/post-v1-admin-scheduler-run.md#s-2c882565d9) | <a id="s-f95d3230bd"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1781c86dda"></a>`usage` | <a id="s-ec6786247f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-2ea2f7a35e"></a>`2` | <a id="s-778f47c230"></a>all: `"empty"` | <a id="s-06c843881f"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-0c1c73c97d"></a>`operational` | <a id="s-9ea62eed0c"></a>`{"kind":"application-error"}` | <a id="s-ef010bcd02"></a>`1` | <a id="s-2aae7aa000"></a>all: `"empty"` | <a id="s-792d0c4b3a"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --role](#s-aa38c45548) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --work-limit](#s-b52681f913) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --work-limit](#s-b52681f913) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [POST /v1/admin/scheduler/run](../../stove0/http-operations/post-v1-admin-scheduler-run.md)
- [stove0_api_client.Stove0ApiClient.run_scheduler](../../stove0-api-client/python/stove0-api-client-stove0apiclient-run-scheduler.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2767b83d1b"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-8f66d89321"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::scheduler\_run](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L495)

### Machine authority

- `/external_contract/cli/stove0/commands/scheduler/commands/run/allow_extra_args`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/name`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/parameters`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/result_contract`
- `/external_contract/cli/stove0/commands/scheduler/commands/run/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/scheduler/commands/run/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/name`

<!-- exact-contract-value: 5e87f618bd8837e87070ae7f83753c6a23ff095f43de6ededcd38ae535031c29 -->

```json
"run"
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/parameters`

<!-- exact-contract-value: 3c9b2fb2e4e4c41f42c5e1f4b79e04f8da998b1a37015d1242834181c1fe1245 -->

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
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 100,
      "min_open": false,
      "minimum": 1,
      "name": "integer range"
    }
  }
]
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/result_contract`

<!-- exact-contract-value: aa2a8851fb9da8f737b8f21dfb04c9d05cbf48abd7ca514c273f811f885d2ce8 -->

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
  "identity": "stove0-cli-result/scheduler/run/v1",
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
          "operation_id": "run_scheduler",
          "path": "/v1/admin/scheduler/run",
          "schema": {
            "$ref": "#/components/schemas/SchedulerRun"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/scheduler/commands/run/terminating_controls`

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
