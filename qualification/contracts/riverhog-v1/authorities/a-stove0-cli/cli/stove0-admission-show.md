# stove0 admission show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-admission-show:38abf4492a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ada52463dd"></a>Parser name: `show`
- <a id="s-27335808ef"></a>Extra arguments at this parser: rejected.
- <a id="s-cec5eb23fa"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-f6b3d67ae5"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-d195271b1a"></a>`admission_id`<br>`admission_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

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
| <a id="s-954407e9c5"></a>`completed` | <a id="s-c9cb9b96ab"></a>`{"kind":"command-completed"}` | <a id="s-08abd8d8b3"></a>`0` | <a id="s-bc98471949"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_admission response 200](../../stove0/http-operations/get-v1-admissions-admission-id.md#s-b0cd202f5e) | <a id="s-526439aaf8"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-29b3e7fc43"></a>`usage` | <a id="s-290bf89815"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-96af832097"></a>`2` | <a id="s-ea5474628d"></a>all: `"empty"` | <a id="s-25747f5e1c"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-2aad4b2f9f"></a>`operational` | <a id="s-959b903a2a"></a>`{"kind":"application-error"}` | <a id="s-d930265dac"></a>`1` | <a id="s-342a25eafd"></a>all: `"empty"` | <a id="s-c32a63f0f4"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter admission_id](#s-d195271b1a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions/{admission_id}](../../stove0/http-operations/get-v1-admissions-admission-id.md)
- [stove0_api_client.Stove0ApiClient.get_admission](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-admission.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-c28f9ce4e8"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6d4b11f310"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::show\_admission](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L246)

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/show/allow_extra_args`
- `/external_contract/cli/stove0/commands/admission/commands/show/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/admission/commands/show/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/admission/commands/show/name`
- `/external_contract/cli/stove0/commands/admission/commands/show/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

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

</details>
