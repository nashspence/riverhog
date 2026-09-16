# stove0 evaluation review

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-evaluation-review:50fb42045b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-dac3c32382"></a>Parser name: `review`
- <a id="s-902365f0dd"></a>Extra arguments at this parser: rejected.
- <a id="s-eb2c85ca53"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-86174cd032"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-5d4571c4fb"></a>`evaluation_id`<br>`evaluation_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-988f27c018"></a>`variant_id`<br>`variant_id` | required positional; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-9c3cdf9820"></a>`rating`<br>`--rating` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`5` (inclusive); outside range: reject | not recorded<br>Env: `null` |
| <a id="s-7944ac36f5"></a>`note`<br>`--note` | optional option; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4501799f7c"></a>`help` | <a id="s-334abd1c30"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-3858957c0e"></a>`0` | <a id="s-619cb73a26"></a>`"noncontractual-framework-help"` | <a id="s-af2b576d3e"></a>`"empty"` |

### Result and failure contract

- <a id="s-d0c428a900"></a>Result identity: `stove0-cli-result/evaluation/review/v1`
- <a id="s-561263e206"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-388d72e33b"></a>Structured output: `optional-json`
- <a id="s-850341bf66"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-53a29984fb"></a>`completed` | <a id="s-eef49099d2"></a>`{"kind":"command-completed"}` | <a id="s-36afc45e35"></a>`0` | <a id="s-9f1ae67281"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP review_evaluation_variant response 200](../../stove0/http-operations/put-v1-evaluations-evaluation-id-variants-variant-id-review.md#s-efd76461f6) | <a id="s-04329a4ad8"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-42e1fbfaa0"></a>`usage` | <a id="s-72aca966d7"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-0e1e570e05"></a>`2` | <a id="s-e7c98e3419"></a>all: `"empty"` | <a id="s-b76f8da6e1"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-3a8c36178d"></a>`operational` | <a id="s-2a62627fc0"></a>`{"kind":"application-error"}` | <a id="s-c6e37e069c"></a>`1` | <a id="s-0bdf213c00"></a>all: `"empty"` | <a id="s-ebb2d3185a"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter evaluation_id](#s-5d4571c4fb) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --note](#s-7944ac36f5) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --rating](#s-9c3cdf9820) | `value · cli-value · contract_max` | maximum=5; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --rating](#s-9c3cdf9820) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter variant_id](#s-988f27c018) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [PUT /v1/evaluations/{evaluation_id}/variants/{variant_id}/review](../../stove0/http-operations/put-v1-evaluations-evaluation-id-variants-variant-id-review.md)
- [stove0_api_client.Stove0ApiClient.review_evaluation_variant](../../stove0-api-client/python/stove0-api-client-stove0apiclient-review-evaluation-variant.md)

## Governing policies

- <a id="pa-0cebf7d3f5"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-9a76bc5b83"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — `reference/stove0/application/client/src/stove0_cli/main.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- **Command callback:** [reference/stove0/application/client/src/stove0_cli/main.py::review_evaluation](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L455)

### Machine authority

- `/external_contract/cli/stove0/commands/evaluation/commands/review/allow_extra_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/name`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/result_contract`
- `/external_contract/cli/stove0/commands/evaluation/commands/review/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/evaluation/commands/review/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/review/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/evaluation/commands/review/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/evaluation/commands/review/name`

<!-- exact-contract-value: fe82cb229e29dfc87a309c8d1679ff9f58be239c8e6f3ff52bc431efb3709b6b -->

```json
"review"
```

### `/external_contract/cli/stove0/commands/evaluation/commands/review/parameters`

<!-- exact-contract-value: 3a2617089f8c39ecfb1baab9d9c3c008f5fec2f4958c8ab212b3fcb2b8311993 -->

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
  },
  {
    "envvar": null,
    "kind": "TyperArgument",
    "multiple": false,
    "name": "variant_id",
    "nargs": 1,
    "options": [
      "variant_id"
    ],
    "required": true,
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
    "name": "rating",
    "nargs": 1,
    "options": [
      "--rating"
    ],
    "required": false,
    "secondary_options": [],
    "type": {
      "clamp": false,
      "class": "typer._click.types.IntRange",
      "max_open": false,
      "maximum": 5,
      "min_open": false,
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
    "name": "note",
    "nargs": 1,
    "options": [
      "--note"
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

### `/external_contract/cli/stove0/commands/evaluation/commands/review/result_contract`

<!-- exact-contract-value: 2950354000b50dd9f4895c2ad08e00902ea2f983edccb9ab9920bbb22cd518cb -->

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
  "identity": "stove0-cli-result/evaluation/review/v1",
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
          "method": "PUT",
          "operation_id": "review_evaluation_variant",
          "path": "/v1/evaluations/{evaluation_id}/variants/{variant_id}/review",
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

### `/external_contract/cli/stove0/commands/evaluation/commands/review/terminating_controls`

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
