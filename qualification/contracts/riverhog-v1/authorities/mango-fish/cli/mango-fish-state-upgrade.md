# mango-fish state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:mango-fish:mango-fish-state-upgrade:100ea10f47 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [mango-fish](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1b645cf3be"></a>Parser name: `upgrade`
- <a id="s-3262ced249"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-0d0b0accba"></a>`json`<br>`--json` | optional flag; 0 values | not recorded | `false` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-775ae29582"></a>`help` | <a id="s-d5ef9a0dc9"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-8777f6570f"></a>`0` | <a id="s-974e4a7ab1"></a>`"noncontractual-framework-help"` | <a id="s-61d1fb78d9"></a>`"empty"` |

### Result and failure contract

- <a id="s-1f9c641006"></a>Result identity: `mango-fish-cli-result/state/upgrade/v1`
- <a id="s-6634f70e7f"></a>Profile: `mango-fish-cli-state-human-json/v1`
- <a id="s-4ec1d75573"></a>Structured output: `optional-json`
- <a id="s-7fc1728ff8"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-99b8ba6e3a"></a>`completed` | <a id="s-dc75a07b88"></a>`{"kind":"state-schema-operation-completed"}` | <a id="s-1695390eac"></a>`0` | <a id="s-f9d3635779"></a>human: `"noncontractual-presentation-of-command-result"`; json: [state-schema-status/v1](#s-42c7e83338) | <a id="s-6b236ec5eb"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-205080976d"></a>`usage` | <a id="s-c0da708f52"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9607183810"></a>`2` | <a id="s-3b217d607e"></a>all: `"empty"` | <a id="s-74b01bcee7"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-f375f7e14a"></a>`state-schema` | <a id="s-944d8f1499"></a>`{"kind":"state-schema-error"}` | <a id="s-58d1bc86e3"></a>`1` | <a id="s-b86235213d"></a>all: `"empty"` | <a id="s-9c8df75786"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-42c7e83338"></a>`state-schema-status/v1`

Applies to: completed · stdout (json).

<a id="s-84f4c774b6"></a>

- <a id="s-3f719eb873"></a>`type`: `"object"`
- <a id="s-4518611c06"></a>`additionalProperties`: `false`
- <a id="s-4edb605bd6"></a>`required`: `["name","condition","current_revision","head_revision"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f2f02a9ec4"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-b7268a5810"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-766b2494bc"></a>`head_revision` | yes | type="string" |  |
| <a id="s-47c5fc39d9"></a>`name` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --json](#s-0d0b0accba) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-2172d213e7"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-b137b63595"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:mango-fish](../../../evidence/sources/authorities.md#src-3dcd5eedf2) — [reference/riverhog/applications/mango-fish/src/mango\_fish/cli.py::&lt;module&gt;](../../../../../../reference/riverhog/applications/mango-fish/src/mango_fish/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/mango-fish/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/parameters`

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

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 0b0356f8eded4b37b727d55f405c3f150cda259e50fbdbb6739c22125c5b6d47 -->

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
  "identity": "mango-fish-cli-result/state/upgrade/v1",
  "profile_id": "mango-fish-cli-state-human-json/v1",
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

### `/external_contract/cli/mango-fish/commands/state/commands/upgrade/terminating_controls`

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
