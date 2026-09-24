# stove0 evaluation cancel

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-evaluation-cancel:e8ab8cd37c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bd474dd5e9"></a>Parser name: `cancel`
- <a id="s-283b2d92d0"></a>Extra arguments at this parser: rejected.
- <a id="s-714dd9d36d"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-def1fb1e4e"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-a6b26b3e58"></a>`evaluation_id`<br>`evaluation_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-59e0bd63ff"></a>`help` | <a id="s-ff137a0b63"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-9e7dfb3d9a"></a>`0` | <a id="s-aece9ac4ce"></a>`"noncontractual-framework-help"` | <a id="s-54d377e4a0"></a>`"empty"` |

### Result and failure contract

- <a id="s-3667ac872a"></a>Result identity: `stove0-cli-result/evaluation/cancel/v1`
- <a id="s-d28b123584"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-8dda9cc6fa"></a>Structured output: `optional-json`
- <a id="s-927d0a3e46"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-66f08d46e8"></a>`completed` | <a id="s-8209ec125a"></a>`{"kind":"command-completed"}` | <a id="s-9e8adeafa6"></a>`0` | <a id="s-aa4583d852"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP cancel_evaluation response 200](../../stove0/http-operations/post-v1-evaluations-evaluation-id-cancel.md#s-26caea921f) | <a id="s-b8e0a176a5"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3e373a579e"></a>`usage` | <a id="s-99fa0c5451"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-26d924329d"></a>`2` | <a id="s-f521850645"></a>all: `"empty"` | <a id="s-f82632ffb3"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-03bc7af40a"></a>`operational` | <a id="s-78ef9a56a7"></a>`{"kind":"application-error"}` | <a id="s-758ab81407"></a>`1` | <a id="s-4bb6e027a4"></a>all: `"empty"` | <a id="s-70f84b39ab"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-a6b26b3e58) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [POST /v1/evaluations/{evaluation_id}/cancel](../../stove0/http-operations/post-v1-evaluations-evaluation-id-cancel.md)
- [stove0_api_client.Stove0ApiClient.cancel_evaluation](../../stove0-api-client/python/stove0-api-client-stove0apiclient-cancel-evaluation.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-29e80b8d74"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-cdab65367f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::cancel\_evaluation](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L476)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/allow_extra_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/cancel/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/name`

<!-- exact-contract-value: 5a83111e44703c9dd7431bae2754317bb495994fbf736cf7739842ded4dcbc20 -->

```json
"cancel"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/parameters`

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

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/result_contract`

<!-- exact-contract-value: 3c69a248eb8aa3937a72913fffd6cb756ef88d6b43072a562f2481430d60d2de -->

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
  "identity": "stove0-cli-result/evaluation/cancel/v1",
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
          "operation_id": "cancel_evaluation",
          "path": "/v1/evaluations/{evaluation_id}/cancel",
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

### `/external_contract/cli/stove0/commands/evaluation/commands/cancel/terminating_controls`

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
