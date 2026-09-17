# stove0 evaluation step

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-step:fc31e59d60 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7b832294f5"></a>Parser name: `step`
- <a id="s-745b649388"></a>Extra arguments at this parser: rejected.
- <a id="s-6829629c0d"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-b038b59d5a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f53149fb11"></a>`evaluation_id`<br>`evaluation_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c4350e8099"></a>`help` | <a id="s-5b486667c2"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-50408b11f8"></a>`0` | <a id="s-62dc119970"></a>`"noncontractual-framework-help"` | <a id="s-3499b75cf9"></a>`"empty"` |

### Result and failure contract

- <a id="s-bba331acd8"></a>Result identity: `stove0-cli-result/evaluation/step/v1`
- <a id="s-aff274e909"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-b67d374207"></a>Structured output: `optional-json`
- <a id="s-5834497144"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1373d1d5f6"></a>`completed` | <a id="s-1b845ba708"></a>`{"kind":"command-completed"}` | <a id="s-a326df76ff"></a>`0` | <a id="s-e7f1dd20a2"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP step_evaluation response 200](../../stove0/http-operations/post-v1-evaluations-evaluation-id-step.md#s-303b123a1e) | <a id="s-701932a564"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-1f825edc0c"></a>`usage` | <a id="s-58f7a92189"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-988c2e9963"></a>`2` | <a id="s-8846e3c2cb"></a>all: `"empty"` | <a id="s-571b05c5d0"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-827cc9f415"></a>`operational` | <a id="s-15118c5ce6"></a>`{"kind":"application-error"}` | <a id="s-2d5b3aa773"></a>`1` | <a id="s-3314bda65f"></a>all: `"empty"` | <a id="s-61ffff316d"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-f53149fb11) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/step](../../stove0/http-operations/post-v1-evaluations-evaluation-id-step.md)
- [stove0_api_client.Stove0ApiClient.step_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-step-evaluation.md)

## Governing policies

- <a id="pa-4d9fd49985"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-ce5a7cef2a"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::step\_evaluation](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L430)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/step/allow_extra_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/step/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/step/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/name`

<!-- exact-contract-value: 0d9f50d8178cb7c5b044c4dce43f1a35c44697ac03e70407e2dda2324fa92f56 -->

```json
"step"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/parameters`

<!-- exact-contract-value: ca673eb21049942d705991d78449bdc62878a1a010aacb714cce5fe46aff8f88 -->

```json
[
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "evaluation_id",
    "nargs": 1,
    "options": [
      "evaluation_id"
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

### `/external_contract/cli/stove0/commands/evaluation/commands/step/result_contract`

<!-- exact-contract-value: f736ba484a08568dae9956455236e27a1ed6c8e7c9590758f937b31f97f5ac68 -->

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
  "identity": "stove0-cli-result/evaluation/step/v1",
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
          "operation_id": "step_evaluation",
          "path": "/v1/evaluations/{evaluation_id}/step",
          "schema": {
            "$ref": "#/components/schemas/EvaluationView"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/evaluation/commands/step/terminating_controls`

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
