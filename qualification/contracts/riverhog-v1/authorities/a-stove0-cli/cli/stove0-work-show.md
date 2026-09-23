# stove0 work show

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-work-show:9f0ee8b0f9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d9cdd32a1b"></a>Parser name: `show`
- <a id="s-6268004c64"></a>Extra arguments at this parser: rejected.
- <a id="s-49f7e72c67"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-3f669d7530"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-b1c892ea3c"></a>`work_id`<br>`work_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-84f3f27710"></a>`help` | <a id="s-3428b7545f"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-ed117f7f0f"></a>`0` | <a id="s-417cc6c9d2"></a>`"noncontractual-framework-help"` | <a id="s-53fa5c4bc5"></a>`"empty"` |

### Result and failure contract

- <a id="s-2fb66510a4"></a>Result identity: `stove0-cli-result/work/show/v1`
- <a id="s-978130c87e"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-54930cbc7d"></a>Structured output: `optional-json`
- <a id="s-914565836f"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-629b6d149c"></a>`completed` | <a id="s-7279593f05"></a>`{"kind":"command-completed"}` | <a id="s-b5e6315ee6"></a>`0` | <a id="s-86be555bfb"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP get_work response 200](../../stove0/http-operations/get-v1-work-work-id.md#s-0ec142a631) | <a id="s-77c9561fc0"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8a8621d16c"></a>`usage` | <a id="s-62cf5ad654"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4221ec3012"></a>`2` | <a id="s-6ebeaf1e2e"></a>all: `"empty"` | <a id="s-1a8a62cf86"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-60cc34c12b"></a>`operational` | <a id="s-a7a812aa09"></a>`{"kind":"application-error"}` | <a id="s-d7a29dfafa"></a>`1` | <a id="s-4482655236"></a>all: `"empty"` | <a id="s-a6ce542e8b"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-b1c892ea3c) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}](../../stove0/http-operations/get-v1-work-work-id.md)
- [stove0_api_client.Stove0ApiClient.get_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-get-work.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-38428c1ecc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-34ab644812"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::show\_work](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L320)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/show/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/show/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/show/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/show/name`
- `/external_contract/cli/stove0/commands/work/commands/show/parameters`
- `/external_contract/cli/stove0/commands/work/commands/show/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/show/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/show/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/show/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/show/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/show/name`

<!-- exact-contract-value: 8f06acb02230bb5a194e0d7f4143d2ecaa508ef645f91340e0e7629981ca6044 -->

```json
"show"
```

### `/external_contract/cli/stove0/commands/work/commands/show/parameters`

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

### `/external_contract/cli/stove0/commands/work/commands/show/result_contract`

<!-- exact-contract-value: 23e43e1086ae33ff30178f5d7e2937c2a3178f4345dcd8f19932a735feefcd26 -->

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
  "identity": "stove0-cli-result/work/show/v1",
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
          "operation_id": "get_work",
          "path": "/v1/work/{work_id}",
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

### `/external_contract/cli/stove0/commands/work/commands/show/terminating_controls`

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
