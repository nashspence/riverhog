# stove0 work list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-work-list:d7ddb82070 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7133feb3f9"></a>Parser name: `list`
- <a id="s-902d80d49f"></a>Extra arguments at this parser: rejected.
- <a id="s-80e85ac7bd"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-67cfe8898a"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-c57dac5cff"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-37ac272d38"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-db3eaea3f6"></a>`phase`<br>`--phase` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-32a1a20eda"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-62f62ee964"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"updated_at"`<br>Env: `null` |
| <a id="s-36f021e7bf"></a>`order`<br>`--order` | optional option; 1 value | text | `"desc"`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-91de9dfde0"></a>`help` | <a id="s-db41d994c0"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-ac0cf7b6a7"></a>`0` | <a id="s-e965504753"></a>`"noncontractual-framework-help"` | <a id="s-5034405de9"></a>`"empty"` |

### Result and failure contract

- <a id="s-d99a82bac1"></a>Result identity: `stove0-cli-result/work/list/v1`
- <a id="s-cdaa51db01"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-3a441109cd"></a>Structured output: `optional-json`
- <a id="s-ca7a024c85"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-cfaad4eb3c"></a>`completed` | <a id="s-70bfb1a4f0"></a>`{"kind":"command-completed"}` | <a id="s-dc60a0c98e"></a>`0` | <a id="s-bccb712615"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_work response 200](../../stove0/http-operations/get-v1-work.md#s-ce11aed2b2) | <a id="s-7d8e38c2f4"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-721178caa2"></a>`usage` | <a id="s-c14f9c828a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-75a4f42a5e"></a>`2` | <a id="s-4963102e52"></a>all: `"empty"` | <a id="s-7d17a8a87f"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-adddd1c52f"></a>`operational` | <a id="s-551f717ba1"></a>`{"kind":"application-error"}` | <a id="s-495c9eff99"></a>`1` | <a id="s-153c0aa6e4"></a>all: `"empty"` | <a id="s-8580ceb8a3"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --order](#s-36f021e7bf) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-c57dac5cff) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-c57dac5cff) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-37ac272d38) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --phase](#s-db3eaea3f6) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-32a1a20eda) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --sort](#s-62f62ee964) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/work](../../stove0/http-operations/get-v1-work.md)
- [stove0_api_client.Stove0ApiClient.list_work](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-work.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-8a3471f4dc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-0e96f2d8c0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::list\_work](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L267)

### Machine authority

- `/external_contract/cli/stove0/commands/work/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/work/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/work/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/work/commands/list/name`
- `/external_contract/cli/stove0/commands/work/commands/list/parameters`
- `/external_contract/cli/stove0/commands/work/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/work/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/work/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/work/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/work/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/work/commands/list/parameters`

<!-- exact-contract-value: dd4eb3e1d8e28cae304826d6de2bfba02b746d2e1b3dd5cc01eea5adf21a45d6 -->

```json
[
  {
    "count": false,
    "default": 25,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_size",
    "nargs": 1,
    "options": [
      "--page-size"
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
  },
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "page_token",
    "nargs": 1,
    "options": [
      "--page-token"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "phase",
    "nargs": 1,
    "options": [
      "--phase"
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
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "query",
    "nargs": 1,
    "options": [
      "--query",
      "-q"
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
    "default": "updated_at",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "sort",
    "nargs": 1,
    "options": [
      "--sort"
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
    "default": "desc",
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "order",
    "nargs": 1,
    "options": [
      "--order"
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

### `/external_contract/cli/stove0/commands/work/commands/list/result_contract`

<!-- exact-contract-value: aec91111a81b22fb246a575630e8bb41471a7b62b9807a6716dc15cd2fe5f8d3 -->

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
  "identity": "stove0-cli-result/work/list/v1",
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
          "operation_id": "list_work",
          "path": "/v1/work",
          "schema": {
            "$ref": "#/components/schemas/WorkPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/work/commands/list/terminating_controls`

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
