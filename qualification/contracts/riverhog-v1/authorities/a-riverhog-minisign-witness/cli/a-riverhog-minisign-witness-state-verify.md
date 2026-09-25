# a-riverhog-minisign-witness state verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-state-verify:a1f2e6ca66 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-74b1b9cd31"></a>Parser name: `verify`

| Field | Value |
|---|---|
| <a id="s-422fc6ed01"></a>`parameters` | `[]` |
- <a id="s-89e2ced9e7"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-fb7c4bb93e"></a>`help` | <a id="s-57cd7270c5"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-70eb24a857"></a>`0` | <a id="s-9b4428fbe8"></a>`"noncontractual-framework-help"` | <a id="s-42bf8aff45"></a>`"empty"` |

### Result and failure contract

- <a id="s-9194eef1b3"></a>Result identity: `a-riverhog-minisign-witness-cli-result/state/verify/v1`
- <a id="s-e29e6f4f16"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-d3efb93228"></a>Structured output: `always-json`
- <a id="s-6a602927a3"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4d0aff0048"></a>`completed` | <a id="s-faf4217497"></a>`{"kind":"command-completed"}` | <a id="s-74ca1b3446"></a>`0` | <a id="s-3ef1012fff"></a>json: [a-riverhog-minisign-witness-cli-state-verify/v1](#s-c3ebdca84d) | <a id="s-5424d085e0"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-4410b1f3b9"></a>`usage` | <a id="s-e7b172a418"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-9f841f2e0b"></a>`2` | <a id="s-f7888a9c10"></a>all: `"empty"` | <a id="s-e2d4a63246"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-b14a226fa4"></a>`application-error` | <a id="s-8583d386d3"></a>`{"kind":"application-error"}` | <a id="s-540e882ed8"></a>`1` | <a id="s-bef2dafdc7"></a>all: `"empty"` | <a id="s-fad889e6d8"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-c3ebdca84d"></a>`a-riverhog-minisign-witness-cli-state-verify/v1`

Applies to: completed · stdout (json).

<a id="s-2576adc6db"></a>

- <a id="s-2b35b2d6ae"></a>`type`: `"object"`
- <a id="s-5a99fb541a"></a>`additionalProperties`: `false`
- <a id="s-a7e8aaf868"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-c842d263b8) |  |

##### <a id="s-c842d263b8"></a>field `status`

- <a id="s-2a25cb1930"></a>`type`: `"object"`
- <a id="s-840332cbb1"></a>`additionalProperties`: `false`
- <a id="s-d92f422618"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-44f3685961"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-6008865f57"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-66baa5bb9c"></a>`head_revision` | yes | type="string" |  |
| <a id="s-8fd6a2d841"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-40b6547ae4"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/result_contract`

<!-- exact-contract-value: 9c76c0c8430cc3d1cbaabfb50ba5bc9f890c21bf3922369c09c051641a120900 -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/state/verify/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-state-verify/v1",
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/verify/terminating_controls`

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
