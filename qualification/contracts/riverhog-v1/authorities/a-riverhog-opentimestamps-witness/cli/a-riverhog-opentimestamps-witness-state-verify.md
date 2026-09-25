# a-riverhog-opentimestamps-witness state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-state-verify:a866df340c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-b74fb7526d"></a>Parser name: `verify`

| Field | Value |
|---|---|
| <a id="s-d5a014a017"></a>`parameters` | `[]` |
- <a id="s-93c399745f"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-2931689bf2"></a>`help` | <a id="s-ceb39c36c3"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-00b4e5e956"></a>`0` | <a id="s-b6d85f2509"></a>`"noncontractual-framework-help"` | <a id="s-65d6a27993"></a>`"empty"` |

### Result and failure contract

- <a id="s-cdd43fce97"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/state/verify/v1`
- <a id="s-3f4aac94c1"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-fa75c423dc"></a>Structured output: `always-json`
- <a id="s-e0ec60b8bc"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4f684532fe"></a>`completed` | <a id="s-0f35540bc0"></a>`{"kind":"command-completed"}` | <a id="s-5e1fef1b66"></a>`0` | <a id="s-e9ccad7021"></a>json: [a-riverhog-opentimestamps-witness-cli-state-verify/v1](#s-3c8d64218d) | <a id="s-3d50701732"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-956eb431ac"></a>`usage` | <a id="s-fc77a1d0db"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9cb712ddf3"></a>`2` | <a id="s-59acb84ae4"></a>all: `"empty"` | <a id="s-393dd32bfb"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-e73b6d5dbb"></a>`application-error` | <a id="s-3d15d77e14"></a>`{"kind":"application-error"}` | <a id="s-12edb3154c"></a>`1` | <a id="s-96af317d33"></a>all: `"empty"` | <a id="s-a8cbb2ab87"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-3c8d64218d"></a>`a-riverhog-opentimestamps-witness-cli-state-verify/v1`

Applies to: completed · stdout (json).

<a id="s-d672609945"></a>

- <a id="s-ad5e9783c6"></a>`type`: `"object"`
- <a id="s-463b0bd754"></a>`additionalProperties`: `false`
- <a id="s-779ff9dc36"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-df19fb000e) |  |

##### <a id="s-df19fb000e"></a>field `status`

- <a id="s-3e5c8a3519"></a>`type`: `"object"`
- <a id="s-1e80f21214"></a>`additionalProperties`: `false`
- <a id="s-97cb91d133"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d4ef8a3b1f"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-9506f1f591"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-f711965034"></a>`head_revision` | yes | type="string" |  |
| <a id="s-ec012297b3"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-481bbdf0f1"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: f47b8f24042b68b6c9a7d1423ffe25236cf1eb4c9ce31320446cdfa1ef5a0e4a -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/state/verify/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-state-verify/v1",
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

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/state/commands/verify/terminating_controls`

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
