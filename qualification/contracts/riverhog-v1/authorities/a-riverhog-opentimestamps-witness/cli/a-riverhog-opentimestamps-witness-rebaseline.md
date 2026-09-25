# a-riverhog-opentimestamps-witness rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-rebaseline:670406bf1b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b607e230b6"></a>Parser name: `rebaseline`

| Field | Value |
|---|---|
| <a id="s-bc9c78c95f"></a>`parameters` | `[]` |
- <a id="s-0a5dbd76ce"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-ff6b80960a"></a>`help` | <a id="s-8027508423"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-d39752eb33"></a>`0` | <a id="s-54c283e6f1"></a>`"noncontractual-framework-help"` | <a id="s-89d019ec50"></a>`"empty"` |

### Result and failure contract

- <a id="s-5874195659"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/rebaseline/v1`
- <a id="s-2d1e39b900"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-079028068a"></a>Structured output: `always-json`
- <a id="s-ee3aa7fc7e"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-7725644661"></a>`completed` | <a id="s-e138efc95e"></a>`{"kind":"command-completed"}` | <a id="s-f2219e75f4"></a>`0` | <a id="s-fd02541d8e"></a>json: [a-riverhog-opentimestamps-witness-cli-rebaseline/v1](#s-a71c57dbd1) | <a id="s-da2773407e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e3cad2b126"></a>`usage` | <a id="s-ff633ab35a"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-456eb6b4a2"></a>`2` | <a id="s-9a0652f2e9"></a>all: `"empty"` | <a id="s-82ae454b73"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-50ec13eb92"></a>`application-error` | <a id="s-95f7e9b20a"></a>`{"kind":"application-error"}` | <a id="s-c4709a198a"></a>`1` | <a id="s-64de6f4347"></a>all: `"empty"` | <a id="s-1c70cf25b9"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-a71c57dbd1"></a>`a-riverhog-opentimestamps-witness-cli-rebaseline/v1`

Applies to: completed · stdout (json).

<a id="s-9ae118b6b7"></a>

- <a id="s-88cd7a900d"></a>`type`: `"object"`
- <a id="s-10f0e11cc1"></a>`additionalProperties`: `false`
- <a id="s-673e80ecf9"></a>`required`: `["progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `progress` | yes | [See field `progress`](#s-f33d693f74) |  |

##### <a id="s-f33d693f74"></a>field `progress`

- <a id="s-5dc219f630"></a>`type`: `"object"`
- <a id="s-431a82d252"></a>`additionalProperties`: `false`
- <a id="s-d7a036e777"></a>`required`: `["generation","serial","phase","source_identity","authorization_view_identity","through_revision","reset_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f97d39c62b"></a>`authorization_view_identity` | yes | type=["string","null"] |  |
| <a id="s-41ed198ddf"></a>`generation` | yes | type="integer"; minimum=0 |  |
| <a id="s-c2b1668b05"></a>`phase` | yes | enum=["new","catalog","catchup","following","reset_required"] |  |
| <a id="s-24f62dbdc4"></a>`reset_reason` | yes | type=["string","null"] |  |
| <a id="s-2c47c39c06"></a>`serial` | yes | type="integer"; minimum=0 |  |
| <a id="s-c16f940052"></a>`source_identity` | yes | type=["string","null"] |  |
| <a id="s-899d3010d5"></a>`through_revision` | yes | type="string" |  |

## Governing policies

- <a id="pa-0f277caeb2"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/name`

<!-- exact-contract-value: 6d6afe02995e3b8523f798f248dce3c58f9efae832d68eb5f8626c5ebdbf8423 -->

```json
"rebaseline"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/result_contract`

<!-- exact-contract-value: 23720f286066596e1a0a3e5d85dfd9650f73c717ef8381df63245c609694831c -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/rebaseline/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-rebaseline/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "progress": {
                "additionalProperties": false,
                "properties": {
                  "authorization_view_identity": {
                    "type": [
                      "string",
                      "null"
                    ]
                  },
                  "generation": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "phase": {
                    "enum": [
                      "new",
                      "catalog",
                      "catchup",
                      "following",
                      "reset_required"
                    ]
                  },
                  "reset_reason": {
                    "type": [
                      "string",
                      "null"
                    ]
                  },
                  "serial": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "source_identity": {
                    "type": [
                      "string",
                      "null"
                    ]
                  },
                  "through_revision": {
                    "type": "string"
                  }
                },
                "required": [
                  "generation",
                  "serial",
                  "phase",
                  "source_identity",
                  "authorization_view_identity",
                  "through_revision",
                  "reset_reason"
                ],
                "type": "object"
              }
            },
            "required": [
              "progress"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/rebaseline/terminating_controls`

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
