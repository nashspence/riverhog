# stove0 work cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-cancel:9efb8f5371 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-33d656b25b"></a>Parser name: `cancel`
- <a id="s-6384369575"></a>Extra arguments at this parser: rejected.
- <a id="s-adf3c3100b"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-91190d1871"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c5c44334bc"></a>`work_id`<br>`work_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-b75fac6350"></a>`help` | <a id="s-79a811ee08"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b0ffac60c0"></a>`0` | <a id="s-36a8df7d68"></a>`"noncontractual-framework-help"` | <a id="s-4eaa0e630d"></a>`"empty"` |

### Result and failure contract

- <a id="s-884fba5164"></a>Result identity: `stove0-cli-result/work/cancel/v1`
- <a id="s-8babce3cbb"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-e436c381bf"></a>Structured output: `optional-json`
- <a id="s-3a5e833c1e"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d9c8b3c193"></a>`completed` | <a id="s-8fffcce507"></a>`{"kind":"command-completed"}` | <a id="s-41d6e8872c"></a>`0` | <a id="s-9a645fda84"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_work response 200](../../stove0/http-operations/post-v1-work-work-id-cancel.md#s-efcda5be1d) | <a id="s-87f57052a2"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-54b75e53f3"></a>`usage` | <a id="s-0fae4291c2"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-03a876a2c8"></a>`2` | <a id="s-d056f44cca"></a>all: `"empty"` | <a id="s-752959be23"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-86498a591f"></a>`operational` | <a id="s-5bb24c858b"></a>`{"kind":"application-error"}` | <a id="s-d7ef2d885b"></a>`1` | <a id="s-7789755aa0"></a>all: `"empty"` | <a id="s-3a6f6e7a13"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-c5c44334bc) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/work/{work_id}/cancel](../../stove0/http-operations/post-v1-work-work-id-cancel.md)
- [stove0_api_client.Stove0ApiClient.cancel_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-cancel-work.md)

## Governing policies

- <a id="pa-876a88a4fd"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-aba4804531"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::cancel\_work](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L361)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/cancel/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/cancel/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/cancel/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/cancel/name`
- `/external_contract/cli/stove0/commands/work/commands/cancel/parameters`
- `/external_contract/cli/stove0/commands/work/commands/cancel/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/stove0/commands/work/commands/cancel/parameters`

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

### `/external_contract/cli/stove0/commands/work/commands/cancel/result_contract`

<!-- exact-contract-value: 7ed7a708bde1a245feb82db126b38f1a9552a345d2e695715b88c96f26d857fb -->

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
  "identity": "stove0-cli-result/work/cancel/v1",
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
          "operation_id": "cancel_work",
          "path": "/v1/work/{work_id}/cancel",
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

### `/external_contract/cli/stove0/commands/work/commands/cancel/terminating_controls`

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
