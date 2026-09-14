# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:13c5f602be -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c9dbdcd915"></a>Parser name: `riverhog-recover`

### Parameters

| Name | Kind | Required | Type | Options |
|---|---|---:|---|---|
| <a id="s-bd0c880f02"></a>`archive` | _StoreAction | yes | Path |  |
| <a id="s-3af69a838a"></a>`output` | _StoreAction | no | Path |  |
| <a id="s-15d3edeac1"></a>`description_only` | _StoreTrueAction | no |  | --description-only |
| <a id="s-ae476bc569"></a>`tags_only` | _StoreTrueAction | no |  | --tags-only |
| <a id="s-27589f8407"></a>`passphrases_file` | _StoreAction | no | Path | --passphrases-file |
| <a id="s-bc196a2661"></a>`age_command` | _StoreAction | no |  | --age-command |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-39383577b3"></a>`help` | <a id="s-893a5ec67a"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-4b2dcde78e"></a>`0` | <a id="s-34adca8f4a"></a>`"noncontractual-framework-help"` | <a id="s-3632e502b5"></a>`"empty"` |
| <a id="s-ef8cb24105"></a>`version` | <a id="s-d91133bda3"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-c45dfa123a"></a>`0` | <a id="s-a5aeb70a7f"></a>`{"distribution":"riverhog-recover","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-dd0a526db8"></a>`"empty"` |

### Result and failure contract

- <a id="s-53870ed077"></a>Result identity: `riverhog-recover-cli-result/root/v1`
- <a id="s-26c4bed674"></a>Profile: `riverhog-recover-cli/v1`
- <a id="s-6073c20d9b"></a>Structured output: `mode-specific`
- <a id="s-2368ff4334"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-e6ffcd91df"></a>`archive-recovered` | <a id="s-b3f25cf921"></a>`{"kind":"options-absent","parameters":["description_only","tags_only"]}` | <a id="s-cc80a4f4a0"></a>`0` | <a id="s-82103433e9"></a>`human: noncontractual-recovery-summary` | <a id="s-1b310db431"></a>`all: empty` |
| <a id="s-1cd6f69133"></a>`description-recovered` | <a id="s-228b200272"></a>`{"kind":"option-equals","parameter":"description_only","value":true}` | <a id="s-e8bbbcc49c"></a>`0` | <a id="s-bdf9e5feff"></a>`json: schema-authority` | <a id="s-018043ff9a"></a>`all: empty` |
| <a id="s-b4a66c971c"></a>`tags-recovered` | <a id="s-191ff5ecba"></a>`{"kind":"option-equals","parameter":"tags_only","value":true}` | <a id="s-70c0bd4c11"></a>`0` | <a id="s-d382d2c367"></a>`json: riverhog-recovered-collection-tags/v1-json-sequence` | <a id="s-cb2e774c6f"></a>`all: empty` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f02faf7dad"></a>`usage` | <a id="s-140eafc026"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-33edb1b1ec"></a>`2` | <a id="s-19f9334bbd"></a>`all: empty` | <a id="s-2dfd45a472"></a>`all: noncontractual-usage-diagnostic` |
| <a id="s-4b8e5acd9d"></a>`recovery` | <a id="s-7253334adc"></a>`{"kind":"recovery-error"}` | <a id="s-3c2553c1f0"></a>`1` | <a id="s-664d5b2d56"></a>`all: empty` | <a id="s-8ca9ad09a8"></a>`all: noncontractual-diagnostic` |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=0; minimum=0; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --description-only](#s-15d3edeac1) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --tags-only](#s-ae476bc569) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

- <a id="pa-a4623d3500"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-c337dd4a4c"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-recover](../../../evidence/sources.md#src-375119d633) — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`
- `/external_contract/cli/riverhog-recover/result_contract`
- `/external_contract/cli/riverhog-recover/terminating_controls`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-recover/name`

<!-- exact-contract-value: 62aebf155460f3da3f1210d92abf5b71ae96fb125e9f2c6f92e1ef7cfe99a7d1 -->

```json
"riverhog-recover"
```

### `/external_contract/cli/riverhog-recover/parameters`

<!-- exact-contract-value: 2d226e23c659f1ca7f09ce8b575f09a4c0b20bb3735e6b6435f110eebb3b772f -->

```json
[
  {
    "dest": "archive",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "output",
    "kind": "_StoreAction",
    "nargs": "?",
    "options": [],
    "required": false,
    "type": "Path"
  },
  {
    "default": false,
    "dest": "description_only",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--description-only"
    ],
    "required": false
  },
  {
    "default": false,
    "dest": "tags_only",
    "kind": "_StoreTrueAction",
    "nargs": 0,
    "options": [
      "--tags-only"
    ],
    "required": false
  },
  {
    "dest": "passphrases_file",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--passphrases-file"
    ],
    "required": false,
    "type": "Path"
  },
  {
    "default": "age",
    "dest": "age_command",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--age-command"
    ],
    "required": false
  }
]
```

### `/external_contract/cli/riverhog-recover/result_contract`

<!-- exact-contract-value: b6c201503259a4db3fc0548159441df7e4ed64ddeed8a9af5f3ea2f75e573aff -->

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
      "id": "recovery",
      "selected_by": {
        "kind": "recovery-error"
      },
      "stderr": {
        "all": "noncontractual-diagnostic"
      },
      "stdout": {
        "all": "empty"
      }
    }
  ],
  "human_json_relationship": "mode-specific-results",
  "identity": "riverhog-recover-cli-result/root/v1",
  "profile_id": "riverhog-recover-cli/v1",
  "structured_output": "mode-specific",
  "success": [
    {
      "exit_status": 0,
      "id": "archive-recovered",
      "selected_by": {
        "kind": "options-absent",
        "parameters": [
          "description_only",
          "tags_only"
        ]
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "human": "noncontractual-recovery-summary"
      }
    },
    {
      "exit_status": 0,
      "id": "description-recovered",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "description_only",
        "value": true
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "authority": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-collection-description-v1.schema.json",
          "kind": "schema-authority",
          "nullable": true
        }
      }
    },
    {
      "exit_status": 0,
      "id": "tags-recovered",
      "selected_by": {
        "kind": "option-equals",
        "parameter": "tags_only",
        "value": true
      },
      "stderr": {
        "all": "empty"
      },
      "stdout": {
        "json": {
          "framing": "newline-delimited-json",
          "identity": "riverhog-recovered-collection-tags/v1-json-sequence",
          "kind": "cli-local-json-sequence",
          "records": {
            "authority": {
              "additionalProperties": false,
              "properties": {
                "format": {
                  "const": "riverhog-recovered-collection-tags/v1"
                },
                "head_identity": {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                },
                "record": {
                  "const": "authority"
                },
                "revision": {
                  "maximum": 9007199254740991,
                  "minimum": 1,
                  "type": "integer"
                },
                "tag_set_identity": {
                  "pattern": "^[0-9a-f]{64}$",
                  "type": "string"
                }
              },
              "required": [
                "format",
                "record",
                "revision",
                "tag_set_identity",
                "head_identity"
              ],
              "type": "object"
            },
            "complete": {
              "additionalProperties": false,
              "properties": {
                "record": {
                  "const": "complete"
                },
                "tag_count": {
                  "minimum": 0,
                  "type": "integer"
                }
              },
              "required": [
                "record",
                "tag_count"
              ],
              "type": "object"
            },
            "tag": {
              "additionalProperties": false,
              "properties": {
                "record": {
                  "const": "tag"
                },
                "tag": {
                  "type": "string"
                }
              },
              "required": [
                "record",
                "tag"
              ],
              "type": "object"
            }
          },
          "sequence": {
            "end": "complete",
            "repeated": "tag",
            "start": "authority"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/riverhog-recover/terminating_controls`

<!-- exact-contract-value: 28287265e454ba0fd87d06e8897b72e2b322d61f1e4ce958e186d2d298073c54 -->

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
  },
  {
    "exit_status": 0,
    "id": "version",
    "stderr": "empty",
    "stdout": {
      "distribution": "riverhog-recover",
      "kind": "installed-coordinated-release-version",
      "serialization": "noncontractual"
    },
    "trigger": {
      "kind": "option-present",
      "options": [
        "--version"
      ]
    }
  }
]
```
