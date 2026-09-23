# a-riverhog-ftp-spool check-config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-ftp-spool:a-riverhog-ftp-spool-check-config:307853a9d3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-ftp-spool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-38b3d68f42"></a>Parser name: `check-config`

| Field | Value |
|---|---|
| <a id="s-7ba3683a06"></a>`parameters` | `[]` |
- <a id="s-e9ab97b963"></a>Unique long-option abbreviations: accepted.

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6705e66228"></a>`help` | <a id="s-7f3ffb204e"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-52d4585757"></a>`0` | <a id="s-688d69b354"></a>`"noncontractual-framework-help"` | <a id="s-e9dafceaf6"></a>`"empty"` |

### Result and failure contract

- <a id="s-5465d2474a"></a>Result identity: `a-riverhog-ftp-spool-cli-result/check-config/v1`
- <a id="s-3764fe322c"></a>Profile: `a-riverhog-ftp-spool-cli-human-json/v1`
- <a id="s-37da2248c7"></a>Structured output: `optional-json`
- <a id="s-a9fd64ee35"></a>Human/JSON relationship: `same-semantic-result`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-5b5c021895"></a>`completed` | <a id="s-cd1ef90860"></a>`{"kind":"command-completed"}` | <a id="s-832ca95f88"></a>`0` | <a id="s-c5ccd20f38"></a>human: `"noncontractual-presentation-of-command-result"`; json: [a-riverhog-ftp-spool-config-check/v1](#s-0a05a9f31c) | <a id="s-0aa4164287"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-94d89828da"></a>`usage` | <a id="s-d14b58fb71"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-6b828feb67"></a>`2` | <a id="s-796f6f02b9"></a>all: `"empty"` | <a id="s-0e8f38a5d1"></a>all: `"noncontractual-usage-diagnostic"` |

### Local structured outputs


#### <a id="s-0a05a9f31c"></a>`a-riverhog-ftp-spool-config-check/v1`

Applies to: completed · stdout (json).

<a id="s-38f899f696"></a>

- <a id="s-e295301231"></a>`type`: `"object"`
- <a id="s-008ff071cf"></a>`additionalProperties`: `false`
- <a id="s-32f0625b7b"></a>`required`: `["format","status","sources"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-22b2d3bef1"></a>`format` | yes | const="a-riverhog-ftp-spool-config-check/v1" |  |
| <a id="s-126202771e"></a>`sources` | yes | type="integer"; minimum=1 |  |
| <a id="s-ddc91c1170"></a>`status` | yes | const="ok" |  |

## Governing policies

- <a id="pa-dd0e313330"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-ftp-spool](../../../evidence/sources/authorities.md#src-4f9bc5584a) — [some-implementations/riverhog/ingress/ftp/src/a\_riverhog\_ftp\_spool/app.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/ingress/ftp/src/a_riverhog_ftp_spool/app.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/allow_abbrev`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/name`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/parameters`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/result_contract`
- `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/name`

<!-- exact-contract-value: 7d1e6f2a5e6adc41b7eecc20dba2cd88e0570cddf2fd660b1f6f4ddbcd7d1e61 -->

```json
"check-config"
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/parameters`

<!-- exact-contract-value: 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945 -->

```json
[]
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/result_contract`

<!-- exact-contract-value: 5a92e43c831a2e94a9b5299cd1a897fd71e576f4662d6ed3f05e534b6ce8b4cb -->

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
    }
  ],
  "human_json_relationship": "same-semantic-result",
  "identity": "a-riverhog-ftp-spool-cli-result/check-config/v1",
  "profile_id": "a-riverhog-ftp-spool-cli-human-json/v1",
  "structured_output": "optional-json",
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
        "human": "noncontractual-presentation-of-command-result",
        "json": {
          "identity": "a-riverhog-ftp-spool-config-check/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "format": {
                "const": "a-riverhog-ftp-spool-config-check/v1"
              },
              "sources": {
                "minimum": 1,
                "type": "integer"
              },
              "status": {
                "const": "ok"
              }
            },
            "required": [
              "format",
              "status",
              "sources"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-ftp-spool/commands/check-config/terminating_controls`

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
