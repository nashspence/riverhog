# a-riverhog-minisign-witness state status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-state-status:845ac25f8c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-6f254cfa0a"></a>Parser name: `status`

| Field | Value |
|---|---|
| <a id="s-4bc61ff65f"></a>`parameters` | `[]` |
- <a id="s-00d77a7cb0"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-4d874d98bb"></a>`help` | <a id="s-65738effbd"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-aaca36fe54"></a>`0` | <a id="s-286c5843ff"></a>`"noncontractual-framework-help"` | <a id="s-2c1ac073c9"></a>`"empty"` |

### Result and failure contract

- <a id="s-56c01f2f7f"></a>Result identity: `a-riverhog-minisign-witness-cli-result/state/status/v1`
- <a id="s-7c2ebb4083"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-cf4d1be468"></a>Structured output: `always-json`
- <a id="s-7719d6d181"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-abff53d1ee"></a>`completed` | <a id="s-bb6d155b9f"></a>`{"kind":"command-completed"}` | <a id="s-d1266a985c"></a>`0` | <a id="s-d0931a757c"></a>json: [a-riverhog-minisign-witness-cli-state-status/v1](#s-e7400b8fc8) | <a id="s-3c6a1480e4"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-0b841c63bb"></a>`usage` | <a id="s-a843974434"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-34c3cb568a"></a>`2` | <a id="s-510dff496d"></a>all: `"empty"` | <a id="s-8746461e50"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-db8f374d1b"></a>`application-error` | <a id="s-ed66bed702"></a>`{"kind":"application-error"}` | <a id="s-5920f65b4d"></a>`1` | <a id="s-63eaaba072"></a>all: `"empty"` | <a id="s-4e967c41cb"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-e7400b8fc8"></a>`a-riverhog-minisign-witness-cli-state-status/v1`

Applies to: completed · stdout (json).

<a id="s-9d404c9de0"></a>

- <a id="s-bae6617f76"></a>`type`: `"object"`
- <a id="s-38ee387727"></a>`additionalProperties`: `false`
- <a id="s-3361a7309d"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-99ca6dc2e1) |  |

##### <a id="s-99ca6dc2e1"></a>field `status`

- <a id="s-651ba4f3df"></a>`type`: `"object"`
- <a id="s-cc3c0be5cb"></a>`additionalProperties`: `false`
- <a id="s-b690b9ad66"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c326267733"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-66154eabb8"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-3a6872cfab"></a>`head_revision` | yes | type="string" |  |
| <a id="s-913c1debc7"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-f17044c871"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/name`

<!-- exact-contract-value: cfc31bcc34ed7f4cc7895026ae8a54f0494f73757e9f914d0f6ed90f9bc34f51 -->

```json
"status"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/result_contract`

<!-- exact-contract-value: 4efc8f29b06032066f6cd5d1f60d8ea78e93e14d2ec8fa7a9de0e7bc2afc0d7b -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/state/status/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-state-status/v1",
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/status/terminating_controls`

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
