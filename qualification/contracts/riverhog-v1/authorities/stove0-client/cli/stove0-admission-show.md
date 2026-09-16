# stove0 admission show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-admission-show:40aad34e74 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ada52463dd"></a>Parser name: `show`

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d195271b1a"></a>`admission_id`<br>`admission_id` | required positional; 1 value | text | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-5f92a6a113"></a>`help` | <a id="s-8d4c6d80c1"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-633409a999"></a>`0` | <a id="s-f7ea0d307e"></a>`"noncontractual-framework-help"` | <a id="s-d838f9eb7b"></a>`"empty"` |

### Result and failure contract

- <a id="s-b4c3a3f0de"></a>Result identity: `stove0-cli-result/admission/show/v1`
- <a id="s-ee2465604a"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-d6a0db16a6"></a>Structured output: `optional-json`
- <a id="s-a74e167dd5"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-954407e9c5"></a>`completed` | <a id="s-c9cb9b96ab"></a>`{"kind":"command-completed"}` | <a id="s-08abd8d8b3"></a>`0` | <a id="s-bc98471949"></a>human: `noncontractual-presentation-of-command-result`; json: [HTTP get_admission response 200](../../stove0/http-operations/get-v1-admissions-admission-id.md#s-b0cd202f5e) | <a id="s-526439aaf8"></a>all: `empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-29b3e7fc43"></a>`usage` | <a id="s-290bf89815"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-96af832097"></a>`2` | <a id="s-ea5474628d"></a>all: `empty` | <a id="s-25747f5e1c"></a>all: `noncontractual-usage-diagnostic` |
| <a id="s-2aad4b2f9f"></a>`operational` | <a id="s-959b903a2a"></a>`{"kind":"application-error"}` | <a id="s-d930265dac"></a>`1` | <a id="s-342a25eafd"></a>all: `empty` | <a id="s-c32a63f0f4"></a>all: `noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter admission_id](#s-d195271b1a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions/{admission_id}](../../stove0/http-operations/get-v1-admissions-admission-id.md)
- [stove0_api_client.Stove0ApiClient.get_admission](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-admission.md)

## Governing policies

- <a id="pa-78ddb0bdb3"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-124f2ad43f"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::show_admission](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L246)

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/show/name`
- `/external_contract/cli/stove0/commands/admission/commands/show/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/show/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/admission/commands/show/parameters`

<!-- exact-contract-value: f0147f7c85fb6352af335636f4d2091373b3e3691b02d05f9bf96424681ec398 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "admission_id",
    "nargs": 1,
    "options": [
      "admission_id"
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

### `/external_contract/cli/stove0/commands/admission/commands/show/result_contract`

<!-- exact-contract-value: 0c3ca148406bd0ff2ed2209bd517c28999e53e0ad99b679dfd699b15a949fd46 -->

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
  "identity": "stove0-cli-result/admission/show/v1",
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
          "operation_id": "get_admission",
          "path": "/v1/admissions/{admission_id}",
          "schema": {
            "$ref": "#/components/schemas/AdmissionView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/admission/commands/show/terminating_controls`

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
