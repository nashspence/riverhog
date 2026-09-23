# stove0 admission list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-stove0-cli:stove0-admission-list:cda25c8b47 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-cli](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-551c9532b6"></a>Parser name: `list`
- <a id="s-fe8f038f81"></a>Extra arguments at this parser: rejected.
- <a id="s-9f57df67e4"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-d32a79dd08"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-7949639bab"></a>`page_size`<br>`--page-size` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `25`<br>Env: `null` |
| <a id="s-f3c2ae9eeb"></a>`page_token`<br>`--page-token` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-279e13c010"></a>`policy_id`<br>`--policy-id` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-3b178f7301"></a>`state_filter`<br>`--state` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-9e85cafb07"></a>`query`<br>`--query`, `-q` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-a4618c1112"></a>`sort`<br>`--sort` | optional option; 1 value | text | `"created_at"`<br>Env: `null` |
| <a id="s-fcdb8ef2ac"></a>`order`<br>`--order` | optional option; 1 value | text | `"desc"`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-979af9a603"></a>`help` | <a id="s-d463f44ef8"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-af087e3be4"></a>`0` | <a id="s-a8d770a64c"></a>`"noncontractual-framework-help"` | <a id="s-33deb92eab"></a>`"empty"` |

### Result and failure contract

- <a id="s-3aae28d887"></a>Result identity: `stove0-cli-result/admission/list/v1`
- <a id="s-77404bee25"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-1e91980917"></a>Structured output: `optional-json`
- <a id="s-a638c2cea3"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-9b1da451fc"></a>`completed` | <a id="s-ea4999f1a0"></a>`{"kind":"command-completed"}` | <a id="s-3f1ca5510e"></a>`0` | <a id="s-7f482a4113"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_admissions response 200](../../stove0/http-operations/get-v1-admissions.md#s-7778dfac1e) | <a id="s-b62f9d36f0"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-6b977617c2"></a>`usage` | <a id="s-f073de919f"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-542792ac2b"></a>`2` | <a id="s-0a08b18460"></a>all: `"empty"` | <a id="s-b9a626706a"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-38dd72adab"></a>`operational` | <a id="s-dc65060ea3"></a>`{"kind":"application-error"}` | <a id="s-807b0c5322"></a>`1` | <a id="s-0fedc66b8d"></a>all: `"empty"` | <a id="s-0a89eaab77"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --order](#s-fcdb8ef2ac) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-size](#s-7949639bab) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --page-size](#s-7949639bab) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --page-token](#s-f3c2ae9eeb) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --policy-id](#s-279e13c010) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --query](#s-9e85cafb07) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --sort](#s-a4618c1112) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --state](#s-3b178f7301) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/admissions](../../stove0/http-operations/get-v1-admissions.md)
- [stove0_api_client.Stove0ApiClient.list_admissions](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-admissions.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3300fe0b77"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-1d662789a5"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources/authorities.md#src-6203ae7d88) — [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::&lt;module&gt;](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [some-implementations/stove0/application/client/src/a\_stove0\_cli/main.py::list\_admissions](../../../../../../some-implementations/stove0/application/client/src/a_stove0_cli/main.py#L207)

### Machine authority

- `/external_contract/cli/stove0/commands/admission/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/admission/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/admission/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/admission/commands/list/name`
- `/external_contract/cli/stove0/commands/admission/commands/list/parameters`
- `/external_contract/cli/stove0/commands/admission/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/admission/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/admission/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/admission/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/admission/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/admission/commands/list/parameters`

<!-- exact-contract-value: 0855665f013028d95d7a0efcb04f9864b1f156bbeff500e325ef0a47fb79ab15 -->

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
    "name": "policy_id",
    "nargs": 1,
    "options": [
      "--policy-id"
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
    "name": "state_filter",
    "nargs": 1,
    "options": [
      "--state"
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
    "default": "created_at",
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

### `/external_contract/cli/stove0/commands/admission/commands/list/result_contract`

<!-- exact-contract-value: 26fb60db2d30a1feb407efa1e6fbb503b7b6d16fa9a60374b5aa650d9037dba5 -->

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
  "identity": "stove0-cli-result/admission/list/v1",
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
          "operation_id": "list_admissions",
          "path": "/v1/admissions",
          "schema": {
            "$ref": "#/components/schemas/AdmissionPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/admission/commands/list/terminating_controls`

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
