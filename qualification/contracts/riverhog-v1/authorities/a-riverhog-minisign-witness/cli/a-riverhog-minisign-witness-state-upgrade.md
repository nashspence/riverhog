# a-riverhog-minisign-witness state upgrade

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-state-upgrade:00e4e2cc7f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-7d5ade557b"></a>Parser name: `upgrade`

| Field | Value |
|---|---|
| <a id="s-e88f93f74b"></a>`parameters` | `[]` |
- <a id="s-773f38b6fc"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-990b50dabe"></a>`help` | <a id="s-e70bfd842a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-16d8f2c2ff"></a>`0` | <a id="s-276e2c91f5"></a>`"noncontractual-framework-help"` | <a id="s-ddcacc5b66"></a>`"empty"` |

### Result and failure contract

- <a id="s-2c4338b5cd"></a>Result identity: `a-riverhog-minisign-witness-cli-result/state/upgrade/v1`
- <a id="s-d3985e80e5"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-b788201ecb"></a>Structured output: `always-json`
- <a id="s-747f7f5577"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-3bd1826631"></a>`completed` | <a id="s-360d53f339"></a>`{"kind":"command-completed"}` | <a id="s-081e9fb120"></a>`0` | <a id="s-508b520684"></a>json: [a-riverhog-minisign-witness-cli-state-upgrade/v1](#s-c3eae7e9bb) | <a id="s-6a8840f28b"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-b08299b592"></a>`usage` | <a id="s-4dbf491e69"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-206277ef2a"></a>`2` | <a id="s-79a5648443"></a>all: `"empty"` | <a id="s-4f4ae829bd"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-caa969368b"></a>`application-error` | <a id="s-75acd42267"></a>`{"kind":"application-error"}` | <a id="s-e3625a4746"></a>`1` | <a id="s-808641552f"></a>all: `"empty"` | <a id="s-328f7b54cb"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-c3eae7e9bb"></a>`a-riverhog-minisign-witness-cli-state-upgrade/v1`

Applies to: completed · stdout (json).

<a id="s-07cafa0324"></a>

- <a id="s-1eb880b736"></a>`type`: `"object"`
- <a id="s-672528d70a"></a>`additionalProperties`: `false`
- <a id="s-fef858471c"></a>`required`: `["status"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `status` | yes | [See field `status`](#s-6e41d095b7) |  |

##### <a id="s-6e41d095b7"></a>field `status`

- <a id="s-34f77571ab"></a>`type`: `"object"`
- <a id="s-294ba18723"></a>`additionalProperties`: `false`
- <a id="s-9bfbb80609"></a>`required`: `["name","condition","current_revision","head_revision"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69c87fc33a"></a>`condition` | yes | enum=["empty","current","upgrade_required","unversioned","incompatible"] |  |
| <a id="s-ae969d0bab"></a>`current_revision` | yes | type=["string","null"] |  |
| <a id="s-dd0e090d75"></a>`head_revision` | yes | type="string" |  |
| <a id="s-ddda582ce1"></a>`name` | yes | type="string" |  |

## Governing policies

- <a id="pa-1dbb2e47c2"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/name`

<!-- exact-contract-value: 192e80d1fe4e27d140b2db67853ff131d7a670e024c440098f759d2df9f2c230 -->

```json
"upgrade"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/result_contract`

<!-- exact-contract-value: 5b7345b9270a7111f259739b5a3b36f503762bf1d9d82f2da5bdcaf3520c9505 -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/state/upgrade/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-state-upgrade/v1",
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/state/commands/upgrade/terminating_controls`

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
