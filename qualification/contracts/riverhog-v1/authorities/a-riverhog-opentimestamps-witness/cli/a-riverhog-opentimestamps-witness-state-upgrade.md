# a-riverhog-opentimestamps-witness state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-state-upgrade:4634e7c45f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-2625cdf5b0"></a>Parser name: `upgrade`

| Field | Value |
|---|---|
| <a id="s-b6e90bba21"></a>`parameters` | `[]` |
- <a id="s-d6451878d1"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-bc41a83a80"></a>`help` | <a id="s-73091ac9ed"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-a21fa4d600"></a>`0` | <a id="s-cfa06895d5"></a>`"noncontractual-framework-help"` | <a id="s-5d916eed68"></a>`"empty"` |

### Result and failure contract

- <a id="s-1aac81b3ef"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/state/upgrade/v1`
- <a id="s-6659387726"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-cf36fa5006"></a>Structured output: `always-json`
- <a id="s-e889398243"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-712dcb41a3"></a>`completed` | <a id="s-963a0223dd"></a>`{"kind":"command-completed"}` | <a id="s-5307e8c28e"></a>`0` | <a id="s-c785ba692b"></a>json: [a-riverhog-opentimestamps-witness-cli-state-upgrade/v1](#s-7486a3307c) | <a id="s-50e128f5e2"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4b7f1a57a8"></a>`usage` | <a id="s-26721eb3c1"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-41d1ddad34"></a>`2` | <a id="s-9d98714d51"></a>all: `"empty"` | <a id="s-87275eb680"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-edcfce1f2f"></a>`application-error` | <a id="s-b9f679ba57"></a>`{"kind":"application-error"}` | <a id="s-1038e05e1f"></a>`1` | <a id="s-3ba554c60b"></a>all: `"empty"` | <a id="s-261ff17c59"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-7486a3307c"></a>`a-riverhog-opentimestamps-witness-cli-state-upgrade/v1`

Applies to: completed · stdout (json).

<a id="s-cfc213029f"></a>

- <a id="s-d3bc4dc56b"></a>`type`: `"object"`
- <a id="s-aed36ab0e9"></a>`additionalProperties`: `false`
- <a id="s-33c09b182e"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-cc4619753a) |  |

##### <a id="s-cc4619753a"></a>field `status`

- <a id="s-1b847147b4"></a>`type`: `"object"`
- <a id="s-1d8a6a82ff"></a>`additionalProperties`: `false`
- <a id="s-e850c523d3"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c6677bae3b"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-83dbc2fe94"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-84fdc2fd37"></a>`head_revision` | yes | type="string" |  |
| <a id="s-02e1baf317"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-c315963bfc"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: aa910600a12e8b99f4ebf3d417d6c8dbed959ffc27ee4b60c50255b11c0ed39c -->

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
      "id": "application-error",
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
  "human_json_relationship": "not-applicable",
  "identity": "a-riverhog-opentimestamps-witness-cli-result/state/upgrade/v1",
  "profile_id": "a-riverhog-opentimestamps-witness-cli-json/v1",
  "structured_output": "always-json",
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
        "json": {
          "identity": "a-riverhog-opentimestamps-witness-cli-state-upgrade/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "status": {
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
            },
            "required": [
              "status"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/upgrade/terminating_controls`

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
