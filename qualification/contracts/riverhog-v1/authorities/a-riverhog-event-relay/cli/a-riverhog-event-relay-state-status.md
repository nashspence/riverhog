# a-riverhog-event-relay state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-event-relay:a-riverhog-event-relay-state-status:8bc1858ff8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-bd669b6657"></a>Parser name: `status`
- <a id="s-958fa38e6d"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-402f6c5233"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-d1dde2a33b"></a>`help` | <a id="s-d0d525283b"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-3b145f20b6"></a>`0` | <a id="s-0881fec323"></a>`"noncontractual-framework-help"` | <a id="s-f26d7ea74b"></a>`"empty"` |

### Result and failure contract

- <a id="s-97484fb8f5"></a>Result identity: `a-riverhog-event-relay-cli-result/state/status/v1`
- <a id="s-27ef830cf9"></a>Profile: `a-riverhog-event-relay-cli-state-human-json/v1`
- <a id="s-d8414070be"></a>Structured output: `optional-json`
- <a id="s-4163b3d037"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-76a7bdacbd"></a>`completed` | <a id="s-52acba54f9"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-0cbb1b5a75"></a>`0` | <a id="s-aa17b57585"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-43f21dde20) | <a id="s-a314d39dd3"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d51d5656f0"></a>`usage` | <a id="s-85ed1586c9"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f4bccc97d5"></a>`2` | <a id="s-e38526193e"></a>all: `"empty"` | <a id="s-392d393392"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-60fbc4d80a"></a>`state-schema` | <a id="s-c24dadf714"></a>`{"kind":"state-schema-error"}` | <a id="s-6153b50968"></a>`1` | <a id="s-953292c499"></a>all: `"empty"` | <a id="s-82ff4f944c"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-43f21dde20"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-6e5f95a382"></a>

- <a id="s-539e7d1a44"></a>`type`: `"object"`
- <a id="s-e5d04f99a9"></a>`additionalProperties`: `false`
- <a id="s-c1eb06c7e2"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d7986f02e0"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-8be4caea14"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-e420c258f7"></a>`head_revision` | yes | type="string" |  |
| <a id="s-b1928e5f67"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-402f6c5233) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-3a3fcac945"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-98375adf77"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-79ff0dba62) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/name`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/parameters`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/parameters`

<!-- exact-contract-value: a4eee56605df36f2261ef19e35bdc3c16631d89a610d870dece837b2b2866441 -->

```json
[
  {
    "default": false,
    "dest": "json",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--json"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/result_contract`

<!-- exact-contract-value: f35fdce76b8ffc1b48c60cf212ab91b44d6ab9b5f6b638f1abf0b224e119f1d1 -->

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
      "id": "state-schema",
      "selected_by": {
        "kind": "state-schema-error"
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
  "identity": "a-riverhog-event-relay-cli-result/state/status/v1",
  "profile_id": "a-riverhog-event-relay-cli-state-human-json/v1",
  "structured_output": "optional-json",
  "success": [
    {
      "exit_status": 0,
      "id": "completed",
      "selected_by": {
        "kind": "state-schema-operation-completed"
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "state-schema-status/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "condition": {
                "enum": [
                  "empty",
                  "current",
                  "upgrade_required",
                  "unversioned",
                  "incompatible"
                ]
              },
              "current_revision": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "head_revision": {
                "type": "string"
              },
              "name": {
                "type": "string"
              }
            },
            "required": [
              "name",
              "condition",
              "current_revision",
              "head_revision"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/status/terminating_controls`

<!-- exact-contract-value: 46c96c22d2ed8a51da57bba3ac0f2269bb35f98c5fd6dc3e3ba67e60f772ad72 -->

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
        "-h",
        "--help"
      ]
    }
  }
]
```

</details>
