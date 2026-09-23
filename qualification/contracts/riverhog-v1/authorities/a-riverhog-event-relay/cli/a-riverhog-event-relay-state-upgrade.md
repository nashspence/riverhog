# a-riverhog-event-relay state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-event-relay:a-riverhog-event-relay-state-upgrade:d3d4ff5b57 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-fc65589ec0"></a>Parser name: `upgrade`
- <a id="s-025b6dd515"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-84ac64525a"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-0366fca71e"></a>`help` | <a id="s-a89a3137ad"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-160b62aa4b"></a>`0` | <a id="s-e066278a47"></a>`"noncontractual-framework-help"` | <a id="s-1640ef1343"></a>`"empty"` |

### Result and failure contract

- <a id="s-6661702cdc"></a>Result identity: `a-riverhog-event-relay-cli-result/state/upgrade/v1`
- <a id="s-27486389e1"></a>Profile: `a-riverhog-event-relay-cli-state-human-json/v1`
- <a id="s-c9c094eed4"></a>Structured output: `optional-json`
- <a id="s-48a171f66c"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7b3e0612a4"></a>`completed` | <a id="s-68b1aed707"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-197d8292c1"></a>`0` | <a id="s-096a65b66f"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-6e3b5266d3) | <a id="s-67f0c627fa"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a400d81ab6"></a>`usage` | <a id="s-15edd9734d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-67cbb09587"></a>`2` | <a id="s-8c50cbc24c"></a>all: `"empty"` | <a id="s-2e68e1d022"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-a59ebc8dc3"></a>`state-schema` | <a id="s-fc69d8e0c1"></a>`{"kind":"state-schema-error"}` | <a id="s-adb28b766d"></a>`1` | <a id="s-a0833f6101"></a>all: `"empty"` | <a id="s-f8b7be5329"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-6e3b5266d3"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-14d6e0bd4e"></a>

- <a id="s-0f9f9cbb59"></a>`type`: `"object"`
- <a id="s-fe02df0743"></a>`additionalProperties`: `false`
- <a id="s-e2300c86ae"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-63789c896c"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-d6c9f40a6c"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-d641311f41"></a>`head_revision` | yes | type="string" |  |
| <a id="s-8b2a2fdf96"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-84ac64525a) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-d1832ab6ab"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-43833b989f"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-79ff0dba62) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/name`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/parameters`

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

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 61def15e78ff863f300ebcd92b23863feb52310910074db8896ddb5bdb274f87 -->

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
  "identity": "a-riverhog-event-relay-cli-result/state/upgrade/v1",
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

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/upgrade/terminating_controls`

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
