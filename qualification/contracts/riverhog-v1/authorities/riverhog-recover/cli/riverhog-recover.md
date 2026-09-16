# riverhog-recover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:riverhog-recover:riverhog-recover:568929d061 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-recover](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-c9dbdcd915"></a>Parser name: `riverhog-recover`
- <a id="s-7016a87ae7"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-bd0c880f02"></a>`archive` | required positional; 1 value | Path | not recorded |
| <a id="s-3af69a838a"></a>`output` | optional positional; 0–1 values | Path | not recorded |
| <a id="s-15d3edeac1"></a>`description_only`<br>`--description-only` | optional flag; 0 values | not recorded | `false` |
| <a id="s-ae476bc569"></a>`tags_only`<br>`--tags-only` | optional flag; 0 values | not recorded | `false` |
| <a id="s-27589f8407"></a>`passphrases_file`<br>`--passphrases-file` | optional option; 1 value | Path | not recorded |
| <a id="s-bc196a2661"></a>`age_command`<br>`--age-command` | optional option; 1 value | not recorded | `"age"` |

### Argument combinations

- <a id="s-69b548b386"></a>At most one of: [`--description-only`](#s-15d3edeac1), [`--tags-only`](#s-ae476bc569).

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
| <a id="s-e6ffcd91df"></a>`archive-recovered` | <a id="s-b3f25cf921"></a>`{"kind":"options-absent","parameters":["description_only","tags_only"]}` | <a id="s-cc80a4f4a0"></a>`0` | <a id="s-82103433e9"></a>human: `"noncontractual-recovery-summary"` | <a id="s-1b310db431"></a>all: `"empty"` |
| <a id="s-1cd6f69133"></a>`description-recovered` | <a id="s-228b200272"></a>`{"kind":"option-equals","parameter":"description_only","value":true}` | <a id="s-e8bbbcc49c"></a>`0` | <a id="s-bdf9e5feff"></a>json: [Riverhog collection description document v1](../../riverhog-protocol/schema/riverhog-collection-description-document-v1.md); `nullable`: `true` | <a id="s-018043ff9a"></a>all: `"empty"` |
| <a id="s-b4a66c971c"></a>`tags-recovered` | <a id="s-191ff5ecba"></a>`{"kind":"option-equals","parameter":"tags_only","value":true}` | <a id="s-70c0bd4c11"></a>`0` | <a id="s-d382d2c367"></a>json: [riverhog-recovered-collection-tags/v1-json-sequence](#s-245a0f6858) | <a id="s-cb2e774c6f"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-f02faf7dad"></a>`usage` | <a id="s-140eafc026"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-33edb1b1ec"></a>`2` | <a id="s-19f9334bbd"></a>all: `"empty"` | <a id="s-2dfd45a472"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-4b8e5acd9d"></a>`recovery` | <a id="s-7253334adc"></a>`{"kind":"recovery-error"}` | <a id="s-3c2553c1f0"></a>`1` | <a id="s-664d5b2d56"></a>all: `"empty"` | <a id="s-8ca9ad09a8"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-245a0f6858"></a>`riverhog-recovered-collection-tags/v1-json-sequence`

Applies to: tags-recovered · stdout (json).



| Field | Value |
|---|---|
| <a id="s-d90038d625"></a>`framing` | `"newline-delimited-json"` |
| <a id="s-c925d62769"></a>`identity` | `"riverhog-recovered-collection-tags/v1-json-sequence"` |
| <a id="s-400a6e4178"></a>`kind` | `"cli-local-json-sequence"` |
| <a id="s-2ee6189aab"></a>`sequence · end` | `"complete"` |
| <a id="s-bc2e198249"></a>`sequence · repeated` | `"tag"` |
| <a id="s-d81ff93b3f"></a>`sequence · start` | `"authority"` |

##### Record `authority`

<a id="s-776fab7254"></a>

- <a id="s-b828445f49"></a>`type`: `"object"`
- <a id="s-1ac5b0611a"></a>`additionalProperties`: `false`
- <a id="s-82d82bd16a"></a>`required`: `["format","record","revision","tag_set_identity","head_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4dc41ac915"></a>`format` | yes | const="riverhog-recovered-collection-tags/v1" |  |
| <a id="s-6144d5cb43"></a>`head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-f15d09635c"></a>`record` | yes | const="authority" |  |
| <a id="s-a13a682e2a"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-b9d4e27d9c"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Record `complete`

<a id="s-06650150de"></a>

- <a id="s-32f6cc255b"></a>`type`: `"object"`
- <a id="s-1426af015f"></a>`additionalProperties`: `false`
- <a id="s-a88e71fe5d"></a>`required`: `["record","tag_count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-fd3d5c4578"></a>`record` | yes | const="complete" |  |
| <a id="s-dfb58f5178"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

##### Record `tag`

<a id="s-7fae82835b"></a>

- <a id="s-b09f5d560f"></a>`type`: `"object"`
- <a id="s-f255136431"></a>`additionalProperties`: `false`
- <a id="s-3ed4ff89fe"></a>`required`: `["record","tag"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-7ef508fae9"></a>`record` | yes | const="tag" |  |
| <a id="s-dff4c11fbc"></a>`tag` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-bd0c880f02) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |
| [CLI parameter 1](#s-3af69a838a) | `cardinality · values-per-occurrence · contract_max` | maximum=1; minimum=0; reason="optional-command-argument-arity" |
| [CLI parameter --description-only](#s-15d3edeac1) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity" |
| [CLI parameter --tags-only](#s-ae476bc569) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity" |
| [CLI parameter --passphrases-file](#s-27589f8407) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |
| [CLI parameter --age-command](#s-bc196a2661) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |

## Governing policies

- <a id="pa-23fb9963f8"></a>[compatibility/cli/v1](../../../policies/index.md#p-48a89776de)
- <a id="pa-ef8a531856"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)

### Executable sources

- [cli:riverhog-recover](../../../evidence/sources.md#src-375119d633) — `reference/riverhog/recovery/src/riverhog_recover/cli.py::<module>`
- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`

### Machine authority

- `/external_contract/cli/riverhog-recover/allow_abbrev`
- `/external_contract/cli/riverhog-recover/mutually_exclusive_groups`
- `/external_contract/cli/riverhog-recover/name`
- `/external_contract/cli/riverhog-recover/parameters`
- `/external_contract/cli/riverhog-recover/result_contract`
- `/external_contract/cli/riverhog-recover/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/riverhog-recover/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/riverhog-recover/mutually_exclusive_groups`

<!-- exact-contract-value: ee24cabb716498d7db676e44519f748b1cfce92834d3f0f63b569f2545d96646 -->

```json
[
  {
    "parameters": [
      2,
      3
    ],
    "required": false
  }
]
```

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

</details>
