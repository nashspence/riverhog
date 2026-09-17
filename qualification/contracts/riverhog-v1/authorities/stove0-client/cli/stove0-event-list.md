# stove0 event list

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:stove0-client:stove0-event-list:73cc3657a9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-client](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-be551f1555"></a>Parser name: `list`
- <a id="s-454fc7d821"></a>Extra arguments at this parser: rejected.
- <a id="s-d75df555c3"></a>Options after positional arguments at this parser: parsed as options.
- <a id="s-afb4f39dd4"></a>Unknown options at this parser: rejected.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-43a6ce6b6f"></a>`after`<br>`--after` | optional option; 1 value | text | not recorded<br>Env: `null` |
| <a id="s-247945017e"></a>`limit`<br>`--limit` | optional option; 1 value | integer range; minimum=`1` (inclusive); maximum=`100` (inclusive); outside range: reject | `100`<br>Env: `null` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2a6627f35c"></a>`help` | <a id="s-eb61d48036"></a>`{"kind":"option-present","options":["--help"]}` | <a id="s-b115438ebf"></a>`0` | <a id="s-4479f55f29"></a>`"noncontractual-framework-help"` | <a id="s-77e8f1ff62"></a>`"empty"` |

### Result and failure contract

- <a id="s-36a5d156a6"></a>Result identity: `stove0-cli-result/event/list/v1`
- <a id="s-986505ba48"></a>Profile: `stove0-cli-human-json/v1`
- <a id="s-39a4afc6f2"></a>Structured output: `optional-json`
- <a id="s-fd33db5f91"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-c8ce073006"></a>`completed` | <a id="s-557cdb03ea"></a>`{"kind":"command-completed"}` | <a id="s-5c64f8960e"></a>`0` | <a id="s-61966493bf"></a>human: `"noncontractual-presentation-of-command-result"`; json: [HTTP list_events response 200](../../stove0/http-operations/get-v1-events.md#s-793e76f5b2) | <a id="s-9b891bf172"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b0df01f44a"></a>`usage` | <a id="s-48b46cd3ca"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-7d55613288"></a>`2` | <a id="s-e3512743d7"></a>all: `"empty"` | <a id="s-b59ea8c01d"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-bdd488951f"></a>`operational` | <a id="s-192333eeb1"></a>`{"kind":"application-error"}` | <a id="s-2e7a257f75"></a>`1` | <a id="s-327d433ac2"></a>all: `"empty"` | <a id="s-3bbb47658b"></a>all: `"noncontractual-diagnostic"` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: minimum=1

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --after](#s-43a6ce6b6f) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |
| [CLI parameter --limit](#s-247945017e) | `value · cli-value · contract_max` | maximum=100; reason="schema-maximum"; source_constraint={"field":"type.maximum"} |
| [CLI parameter --limit](#s-247945017e) | `cardinality · values-per-occurrence · fixed` | maximum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"} |

## Maintained corroboration

### Related interface records

- [GET /v1/events](../../stove0/http-operations/get-v1-events.md)
- [stove0_api_client.Stove0ApiClient.list_events](../../stove0-api-client/python/stove0-api-client-stove0apiclient-list-events.md)

## Governing policies

- <a id="pa-dfe26bc39a"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-bc4077d998"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:stove0](../../../evidence/sources.md#src-6203ae7d88) — [reference/stove0/application/client/src/stove0\_cli/main.py::&lt;module&gt;](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py)
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- **Command callback:** [reference/stove0/application/client/src/stove0\_cli/main.py::list\_events](../../../../../../reference/stove0/application/client/src/stove0_cli/main.py#L475)

### Machine authority

- `/external_contract/cli/stove0/commands/event/commands/list/allow_extra_args`
- `/external_contract/cli/stove0/commands/event/commands/list/allow_interspersed_args`
- `/external_contract/cli/stove0/commands/event/commands/list/ignore_unknown_options`
- `/external_contract/cli/stove0/commands/event/commands/list/name`
- `/external_contract/cli/stove0/commands/event/commands/list/parameters`
- `/external_contract/cli/stove0/commands/event/commands/list/result_contract`
- `/external_contract/cli/stove0/commands/event/commands/list/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/stove0/commands/event/commands/list/allow_extra_args`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/event/commands/list/allow_interspersed_args`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/stove0/commands/event/commands/list/ignore_unknown_options`

<!-- exact-contract-value: fcbcf165908dd18a9e49f7ff27810176db8e9f63b4352213741664245224f8aa -->

```json
false
```

### `/external_contract/cli/stove0/commands/event/commands/list/name`

<!-- exact-contract-value: dcb452a982945e5e2957930d83d36af5ceee19805ec0c3b30529ae8f44f6e49e -->

```json
"list"
```

### `/external_contract/cli/stove0/commands/event/commands/list/parameters`

<!-- exact-contract-value: fea3dc979b1eb185fc558f747ed0b3920c1eb4915b321b20680abf2ef54cba1c -->

```json
[
  {
    "count": false,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "after",
    "nargs": 1,
    "options": [
      "--after"
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
    "default": 100,
    "envvar": null,
    "is_flag": false,
    "kind": "TyperOption",
    "multiple": false,
    "name": "limit",
    "nargs": 1,
    "options": [
      "--limit"
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
  }
]
```

### `/external_contract/cli/stove0/commands/event/commands/list/result_contract`

<!-- exact-contract-value: 129d6121ec389bd64c63372c0ace3ba77c092ff3f17e997c1a3b1c584dd13eed -->

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
  "identity": "stove0-cli-result/event/list/v1",
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
          "operation_id": "list_events",
          "path": "/v1/events",
          "schema": {
            "$ref": "#/components/schemas/Stove0EventPage"
          },
          "status": "200"
        }
      }
    }
  ]
}
```

### `/external_contract/cli/stove0/commands/event/commands/list/terminating_controls`

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
