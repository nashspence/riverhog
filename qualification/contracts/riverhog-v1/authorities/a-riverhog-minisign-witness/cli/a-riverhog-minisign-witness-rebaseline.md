# a-riverhog-minisign-witness rebaseline

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-rebaseline:58a8a2869b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-8d4c9c793e"></a>Parser name: `rebaseline`

| Field | Value |
|---|---|
| <a id="s-ce028adccc"></a>`parameters` | `[]` |
- <a id="s-207bb7b5e1"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-c48928accf"></a>`help` | <a id="s-dad985ba7d"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-1069b226a0"></a>`0` | <a id="s-a1ca9859a7"></a>`"noncontractual-framework-help"` | <a id="s-e651804cf6"></a>`"empty"` |

### Result and failure contract

- <a id="s-ea83256355"></a>Result identity: `a-riverhog-minisign-witness-cli-result/rebaseline/v1`
- <a id="s-82c14b71b9"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-7c6c80c0ec"></a>Structured output: `always-json`
- <a id="s-c7b666dbd4"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-31a9779c59"></a>`completed` | <a id="s-0ade5dd317"></a>`{"kind":"command-completed"}` | <a id="s-c609fd35a3"></a>`0` | <a id="s-4c1b75ab77"></a>json: [a-riverhog-minisign-witness-cli-rebaseline/v1](#s-76132462ee) | <a id="s-a9bb6e1d2e"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-20ed146d9c"></a>`usage` | <a id="s-1820c9fffd"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-4998632a4c"></a>`2` | <a id="s-8be3b7cf90"></a>all: `"empty"` | <a id="s-47bae08f0e"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-ed41844c86"></a>`application-error` | <a id="s-84d55f2d71"></a>`{"kind":"application-error"}` | <a id="s-696d57e075"></a>`1` | <a id="s-91f21f27ca"></a>all: `"empty"` | <a id="s-bc8214f5e5"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-76132462ee"></a>`a-riverhog-minisign-witness-cli-rebaseline/v1`

Applies to: completed · stdout (json).

<a id="s-02d8d5a1b8"></a>

- <a id="s-71a1ae8885"></a>`type`: `"object"`
- <a id="s-2f8c9b4f4c"></a>`additionalProperties`: `false`
- <a id="s-370f78e60e"></a>`required`: `["progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `progress` | yes | [See field `progress`](#s-b5636bfcd2) |  |

##### <a id="s-b5636bfcd2"></a>field `progress`

- <a id="s-c667d82826"></a>`type`: `"object"`
- <a id="s-6c0880cea3"></a>`additionalProperties`: `false`
- <a id="s-ca4cb2e242"></a>`required`: `["generation","serial","phase","source_identity","authorization_view_identity","through_revision","reset_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3acc51ffd6"></a>`authorization_view_identity` | yes | type=["string","null"] |  |
| <a id="s-eb68aa4468"></a>`generation` | yes | type="integer"; minimum=0 |  |
| <a id="s-80210bcd79"></a>`phase` | yes | enum=["new","catalog","catchup","following","reset_required"] |  |
| <a id="s-e76556e55c"></a>`reset_reason` | yes | type=["string","null"] |  |
| <a id="s-7248296dcf"></a>`serial` | yes | type="integer"; minimum=0 |  |
| <a id="s-55ef8b250a"></a>`source_identity` | yes | type=["string","null"] |  |
| <a id="s-32d6cdd64a"></a>`through_revision` | yes | type="string" |  |

## Governing policies

- <a id="pa-fb16b4ecb6"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/name`

<!-- exact-contract-value: 6d6afe02995e3b8523f798f248dce3c58f9efae832d68eb5f8626c5ebdbf8423 -->

```json
"rebaseline"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/result_contract`

<!-- exact-contract-value: c83016dd0e9de83e1e938361595ed1ef49fa743456851aec3f03e87c8ec14d4b -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/rebaseline/v1",
  "profile_id": "a-riverhog-minisign-witness-cli-json/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-rebaseline/v1",
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/rebaseline/terminating_controls`

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
