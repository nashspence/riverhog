# a-riverhog-event-relay state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-event-relay:a-riverhog-event-relay-state-verify:39a43b0bcf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-event-relay](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-ddab3deb24"></a>Parser name: `verify`
- <a id="s-d24d43c3e8"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-f603e14acd"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bced2edcea"></a>`help` | <a id="s-eaacb6e532"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-447d045fd7"></a>`0` | <a id="s-58b1d9e393"></a>`"noncontractual-framework-help"` | <a id="s-a1d59e2105"></a>`"empty"` |

### Result and failure contract

- <a id="s-ce831e417b"></a>Result identity: `a-riverhog-event-relay-cli-result/state/verify/v1`
- <a id="s-9f2a7a8d55"></a>Profile: `a-riverhog-event-relay-cli-state-human-json/v1`
- <a id="s-990d4e07e7"></a>Structured output: `optional-json`
- <a id="s-b6e3af82b8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-720c0df356"></a>`completed` | <a id="s-ecedf31ce0"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-af6506420f"></a>`0` | <a id="s-811f8f43d2"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-a722cb31ec) | <a id="s-04a3ac1794"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-8b5d9920fd"></a>`usage` | <a id="s-c38e784e41"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6de1d2dbd2"></a>`2` | <a id="s-189d1fb262"></a>all: `"empty"` | <a id="s-3044ac8072"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-7b1df23739"></a>`state-schema` | <a id="s-3a8f8eacbc"></a>`{"kind":"state-schema-error"}` | <a id="s-f8d94379d4"></a>`1` | <a id="s-35cbfb76e4"></a>all: `"empty"` | <a id="s-3d218752b7"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-a722cb31ec"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-87ba312474"></a>

- <a id="s-cfbee51e7f"></a>`type`: `"object"`
- <a id="s-b16a6625d9"></a>`additionalProperties`: `false`
- <a id="s-f59b05a36a"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e733a0aac"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-1bea93f05c"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-7646248ee5"></a>`head_revision` | yes | type="string" |  |
| <a id="s-bdecffc4cb"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-f603e14acd) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f5e6691c03"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-51f0ae4058"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-event-relay](../../../evidence/sources/authorities.md#src-79ff0dba62) — [some-implementations/riverhog/applications/a-riverhog-event-relay/src/a\_riverhog\_event\_relay/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-event-relay/src/a_riverhog_event_relay/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/name`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/parameters`

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

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 6b9419b0ea868716005a3d49061facbd4532ff3b3575bf04665f15e752ab1b5c -->

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
  "identity": "a-riverhog-event-relay-cli-result/state/verify/v1",
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

### `/external_contract/cli/a-riverhog-event-relay/commands/state/commands/verify/terminating_controls`

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
