# stove0 scheduler status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-scheduler-status:448e391472 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-3d72f99e06"></a>Parser name: `status`

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-21dc67e019"></a>`help` | <a id="s-94e507bfaf"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-475297d962"></a>`0` | <a id="s-cf6256bc2b"></a>`"noncontractual-framework-help"` | <a id="s-e6b9407691"></a>`"empty"` |

### Result and failure contract

- <a id="s-0319224d49"></a>Result identity: `stove0-cli-result/scheduler/status/v1`
- <a id="s-3a2b679584"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-58ab06b372"></a>Structured output: `optional-json`
- <a id="s-1db9fc7229"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-52cbd8040a"></a>`completed` | <a id="s-04b0c497fa"></a>`{"kind":"command-completed"}` | <a id="s-d388f73705"></a>`0` | <a id="s-e023aab05d"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP scheduler_status response 200](../../stove0/http-operations/get-v1-admin-scheduler.md#s-5842ab8baa) | <a id="s-d555e20799"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-96472c5c23"></a>`usage` | <a id="s-28b39827d3"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-011eb9221b"></a>`2` | <a id="s-caba155fc7"></a>all: `empty` | <a id="s-ac781ede89"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-c1fb3ccadc"></a>`operational` | <a id="s-2b08cf4d3c"></a>`{"kind":"application-error"}` | <a id="s-81f91e13aa"></a>`1` | <a id="s-71df70d7cf"></a>all: `empty` | <a id="s-7456733526"></a>all: `noncontractual-diagnostic` |

## Maintained corroboration

### Related interface records

- [GET /v1/admin/scheduler](../../stove0/http-operations/get-v1-admin-scheduler.md)
- [stove0_api_client.Stove0ApiClient.scheduler_status](../../stove0-api-client/python/stove0-api-client-stove0apiclient-scheduler-status.md)

## Governing policies

- <a id="pa-4c0eb2cfc5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::scheduler_status](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L489)

### Machine authority

- `/external_contract/cli/stove0/commands/scheduler/commands/status/name`
- `/external_contract/cli/stove0/commands/scheduler/commands/status/parameters`
- `/external_contract/cli/stove0/commands/scheduler/commands/status/result_contract`
- `/external_contract/cli/stove0/commands/scheduler/commands/status/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/scheduler/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/stove0/commands/scheduler/commands/status/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/stove0/commands/scheduler/commands/status/result_contract`

<!-- exact-contract-value: c6a0e89cf25052bf2fbc79948de4d2ba3880769c6c97ae02609078c33370d6bd -->

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
  "identity": "stove0-cli-result/scheduler/status/v1",
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
          "operation_id": "scheduler_status",
          "path": "/v1/admin/scheduler",
          "schema": {
            "$ref": "#/components/schemas/SchedulerStatus"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/scheduler/commands/status/terminating_controls`

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
