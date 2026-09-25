# a-riverhog-opentimestamps-witness state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-state-status:b1dc30b119 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cecb32dc96"></a>Parser name: `status`

| Field | Value |
|---|---|
| <a id="s-7a2b0acde1"></a>`parameters` | `[]` |
- <a id="s-1e894ae995"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-923ee33c8f"></a>`help` | <a id="s-e840f4bc2d"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-f8acf1a17b"></a>`0` | <a id="s-c2ba34078a"></a>`"noncontractual-framework-help"` | <a id="s-2709263169"></a>`"empty"` |

### Result and failure contract

- <a id="s-c46ec577c6"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/state/status/v1`
- <a id="s-ad2e5fee88"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-c79541f330"></a>Structured output: `always-json`
- <a id="s-9a4c1ab41e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f04e04f2a8"></a>`completed` | <a id="s-ef134aafc6"></a>`{"kind":"command-completed"}` | <a id="s-9af99412f6"></a>`0` | <a id="s-46ae6c093a"></a>json: [a-riverhog-opentimestamps-witness-cli-state-status/v1](#s-319f3ae6e3) | <a id="s-9d37a19180"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-28268bc677"></a>`usage` | <a id="s-17a4314a2b"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-3499ea5da1"></a>`2` | <a id="s-fc5cec75b2"></a>all: `"empty"` | <a id="s-4349cf3336"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-5889244fd3"></a>`application-error` | <a id="s-d6881519db"></a>`{"kind":"application-error"}` | <a id="s-4f023b323a"></a>`1` | <a id="s-44bd7a8ea4"></a>all: `"empty"` | <a id="s-122d0155a8"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-319f3ae6e3"></a>`a-riverhog-opentimestamps-witness-cli-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-78b0a888bf"></a>

- <a id="s-9a44509c83"></a>`type`: `"object"`
- <a id="s-8b8eed467b"></a>`additionalProperties`: `false`
- <a id="s-b27f528cf9"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-8dff4f56f7) |  |

##### <a id="s-8dff4f56f7"></a>field `status`

- <a id="s-1b0fb0c8aa"></a>`type`: `"object"`
- <a id="s-ef4dc60c9a"></a>`additionalProperties`: `false`
- <a id="s-059af3f962"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-874bc5f9f1"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-dda1b3ade7"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-23f9dce0f3"></a>`head_revision` | yes | type="string" |  |
| <a id="s-0ba80d9e14"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-576b0dc53f"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/result_contract`

<!-- exact-contract-value: 29e240f1503dc45542b6a7164addd48f418cf5b590149adf32a195ccdd6a9a5b -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/state/status/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-state-status/v1",
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

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/status/terminating_controls`

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
