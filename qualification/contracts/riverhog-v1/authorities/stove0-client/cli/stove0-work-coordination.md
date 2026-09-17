# stove0 work coordination

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-coordination:d37512ba73 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-d917371341"></a>Parser name: `coordination`
- <a id="s-62b851a792"></a>Extra arguments at this parser: rejected.
- <a id="s-f087c6e381"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d769b24b17"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-8b4c2e9366"></a>`work_id`<br>`work_id` | required positional; 1 value | text | not recorded<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-70d831dbd8"></a>`help` | <a id="s-ce52179604"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-e3a1f8f22d"></a>`0` | <a id="s-bb2702b3ff"></a>`"noncontractual-framework-help"` | <a id="s-6a84953a9b"></a>`"empty"` |

### Result and failure contract

- <a id="s-755b3978c4"></a>Result identity: `stove0-cli-result/work/coordination/v1`
- <a id="s-883df1c153"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-05e02d1df1"></a>Structured output: `optional-json`
- <a id="s-8b3bf55a6a"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-95a2520682"></a>`completed` | <a id="s-dd69aafff3"></a>`{"kind":"command-completed"}` | <a id="s-3fa44044aa"></a>`0` | <a id="s-e9c0f1feb5"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP inspect_work_coordination response 200](../../stove0/http-operations/get-v1-work-work-id-coordination.md#s-51b7e8e06d) | <a id="s-0f79daf1bc"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e971db28b3"></a>`usage` | <a id="s-8302c9399e"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-bf408e532d"></a>`2` | <a id="s-497fcfdef2"></a>all: `"empty"` | <a id="s-38736f8345"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-39e91f4c50"></a>`operational` | <a id="s-aca1a6a9ae"></a>`{"kind":"application-error"}` | <a id="s-710ada742e"></a>`1` | <a id="s-e187198243"></a>all: `"empty"` | <a id="s-2ac6c55e66"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter work_id](#s-8b4c2e9366) | `cardinality · values-per-occurrence · fixed` | shared above |

## Maintained corroboration

### Related interface records

- [GET /v1/work/{work_id}/coordination](../../stove0/http-operations/get-v1-work-work-id-coordination.md)
- [stove0_api_client.Stove0ApiClient.inspect_work_coordination](../../stove0-api-client/python/stove0-api-client-stove0apiclient-inspect-work-coordination.md)

## Governing policies

- <a id="pa-3fa12340ed"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-5951d6149e"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::inspect\_work\_coordination](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L326)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/coordination/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/coordination/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/coordination/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/coordination/name`
- `/external_contract/cli/stove0/commands/work/commands/coordination/parameters`
- `/external_contract/cli/stove0/commands/work/commands/coordination/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/coordination/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/coordination/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/name`

<!-- exact-contract-value: c6c6c1f431776373875764cc5d56c4d9db9a1fa15c32dfea2b2ed67ecbe770d4 -->

```json
"coordination"
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/parameters`

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

### `/external_contract/cli/stove0/commands/work/commands/coordination/result_contract`

<!-- exact-contract-value: 196c8cfc966dafc34f27e8d4f7f948478ec7cec205468a07f12e866b5b7151c6 -->

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
  "identity": "stove0-cli-result/work/coordination/v1",
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
          "operation_id": "inspect_work_coordination",
          "path": "/v1/work/{work_id}/coordination",
          "schema": {
            "$ref": "#/components/schemas/BranchSetEvaluation"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/work/commands/coordination/terminating_controls`

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
