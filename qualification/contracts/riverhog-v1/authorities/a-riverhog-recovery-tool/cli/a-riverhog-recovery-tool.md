# a-riverhog-recovery-tool

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-recovery-tool:a-riverhog-recovery-tool:361e697fa8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-recovery-tool](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-50350bb4a5"></a>Parser name: `a-riverhog-recovery-tool`
- <a id="s-9edeceb093"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-223bd7a36a"></a>`archive` | required positional; 1 value | Path | not recorded |
| <a id="s-7eac9482f6"></a>`output` | optional positional; 0–1 values | Path | not recorded |
| <a id="s-81878bab38"></a>`description_only`<br>`--description-only` | optional flag; 0 values | not recorded | `false` |
| <a id="s-8b54a8e16b"></a>`tags_only`<br>`--tags-only` | optional flag; 0 values | not recorded | `false` |
| <a id="s-431697c50d"></a>`passphrases_file`<br>`--passphrases-file` | optional option; 1 value | Path | not recorded |
| <a id="s-63a302d0d5"></a>`age_command`<br>`--age-command` | optional option; 1 value | not recorded | `"age"` |

### Argument combinations

- <a id="s-312f60be72"></a>At most one of: [`--description-only`](#s-81878bab38), [`--tags-only`](#s-8b54a8e16b).

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-da6b5cc749"></a>`help` | <a id="s-8b204faf72"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-ba70ef3702"></a>`0` | <a id="s-fa37e741e6"></a>`"noncontractual-framework-help"` | <a id="s-36cdd81695"></a>`"empty"` |
| <a id="s-5a7006d22d"></a>`version` | <a id="s-0bd58a0c8e"></a>`{"kind":"option-present","options":["--version"]}` | <a id="s-75cbe05ffc"></a>`0` | <a id="s-e731532f3f"></a>`{"distribution":"a-riverhog-recovery-tool","kind":"installed-coordinated-release-version","serialization":"noncontractual"}` | <a id="s-a2f00d602c"></a>`"empty"` |

### Result and failure contract

- <a id="s-702d2b21c0"></a>Result identity: `a-riverhog-recovery-tool-cli-result/root/v1`
- <a id="s-29a9df1bf7"></a>Profile: `a-riverhog-recovery-tool-cli/v1`
- <a id="s-5b4dcb332f"></a>Structured output: `mode-specific`
- <a id="s-217f3aea8c"></a>Human/JSON relationship: `mode-specific-results`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-37666423e9"></a>`archive-recovered` | <a id="s-58544d9218"></a>`{"kind":"options-absent","parameters":["description_only","tags_only"]}` | <a id="s-78859e90d5"></a>`0` | <a id="s-1034dca16d"></a>human: `"noncontractual-recovery-summary"` | <a id="s-bb5a487560"></a>all: `"empty"` |
| <a id="s-6c541c404c"></a>`description-recovered` | <a id="s-92517f9499"></a>`{"kind":"option-equals","parameter":"description_only","value":true}` | <a id="s-3a06169e26"></a>`0` | <a id="s-97e748e00a"></a>json: [Riverhog collection description document v1](../../riverhog-protocol/schema/riverhog-collection-description-document-v1.md); `nullable`: `true` | <a id="s-362ed34bcd"></a>all: `"empty"` |
| <a id="s-30fb405f5e"></a>`tags-recovered` | <a id="s-8f83f3c9fe"></a>`{"kind":"option-equals","parameter":"tags_only","value":true}` | <a id="s-994946f122"></a>`0` | <a id="s-99b11db028"></a>json: [riverhog-recovered-collection-tags/v1-json-sequence](#s-c7ee2af445) | <a id="s-b5b956a0ee"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-a19db56c28"></a>`usage` | <a id="s-d19364f171"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f5f3955d1e"></a>`2` | <a id="s-969af510fb"></a>all: `"empty"` | <a id="s-0918698a35"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-3ca2a2ba2a"></a>`recovery` | <a id="s-243124515c"></a>`{"kind":"recovery-error"}` | <a id="s-34a95c8a59"></a>`1` | <a id="s-ba0f7d02a7"></a>all: `"empty"` | <a id="s-169d5fda0d"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-c7ee2af445"></a>`riverhog-recovered-collection-tags/v1-json-sequence`

Applies to: tags-recovered · stdout (json).



| Field | Value |
|---|---|
| <a id="s-ba74fe170f"></a>`framing` | `"newline-delimited-json"` |
| <a id="s-a0f54c7254"></a>`identity` | `"riverhog-recovered-collection-tags/v1-json-sequence"` |
| <a id="s-db5fffa8cc"></a>`kind` | `"cli-local-json-sequence"` |
| <a id="s-6e5b4011e5"></a>`sequence · end` | `"complete"` |
| <a id="s-fa920e2dd0"></a>`sequence · repeated` | `"tag"` |
| <a id="s-dc3def6f37"></a>`sequence · start` | `"authority"` |

##### Record `authority`

<a id="s-af5ed3ed62"></a>

- <a id="s-16b048a310"></a>`type`: `"object"`
- <a id="s-fabfcd497a"></a>`additionalProperties`: `false`
- <a id="s-ec4d05426c"></a>`required`: `["format","record","revision","tag_set_identity","head_identity"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-8ba88d75ea"></a>`format` | yes | const="riverhog-recovered-collection-tags/v1" |  |
| <a id="s-6162342f94"></a>`head_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-0e2790fd58"></a>`record` | yes | const="authority" |  |
| <a id="s-efa257ae5e"></a>`revision` | yes | type="integer"; minimum=1; maximum=9007199254740991 |  |
| <a id="s-2b1df15206"></a>`tag_set_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |

##### Record `complete`

<a id="s-c681197c6a"></a>

- <a id="s-9522175597"></a>`type`: `"object"`
- <a id="s-b6671817e6"></a>`additionalProperties`: `false`
- <a id="s-5ccc2b6fa4"></a>`required`: `["record","tag_count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bbc3d0141c"></a>`record` | yes | const="complete" |  |
| <a id="s-6369ed6f6c"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

##### Record `tag`

<a id="s-82d40b131b"></a>

- <a id="s-116ca61fc4"></a>`type`: `"object"`
- <a id="s-a11f671301"></a>`additionalProperties`: `false`
- <a id="s-b64ea2e271"></a>`required`: `["record","tag"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e27c3ae1fd"></a>`record` | yes | const="tag" |  |
| <a id="s-0337a638b4"></a>`tag` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-223bd7a36a) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |
| [CLI parameter 1](#s-7eac9482f6) | `cardinality · values-per-occurrence · contract_max` | maximum=1; minimum=0; reason="optional-command-argument-arity" |
| [CLI parameter --description-only](#s-81878bab38) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity" |
| [CLI parameter --tags-only](#s-8b54a8e16b) | `cardinality · values-per-occurrence · fixed` | maximum=0; minimum=0; reason="fixed-command-argument-arity" |
| [CLI parameter --passphrases-file](#s-431697c50d) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |
| [CLI parameter --age-command](#s-63a302d0d5) | `cardinality · values-per-occurrence · fixed` | maximum=1; minimum=1; reason="fixed-command-argument-arity" |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-f58bc73f71"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-489b7704cf"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-recovery-tool](../../../evidence/sources/authorities.md#src-dbbc3ba671) — [some-implementations/riverhog/recovery/src/a\_riverhog\_recovery\_tool/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/recovery/src/a_riverhog_recovery_tool/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-recovery-tool/allow_abbrev`
- `/external_contract/cli/a-riverhog-recovery-tool/mutually_exclusive_groups`
- `/external_contract/cli/a-riverhog-recovery-tool/name`
- `/external_contract/cli/a-riverhog-recovery-tool/parameters`
- `/external_contract/cli/a-riverhog-recovery-tool/result_contract`
- `/external_contract/cli/a-riverhog-recovery-tool/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-recovery-tool/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-recovery-tool/mutually_exclusive_groups`

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

### `/external_contract/cli/a-riverhog-recovery-tool/name`

<!-- exact-contract-value: 62cfa2cfd521d989fb61b1742be44ce7c1b097434b10c6987dfd7baccd2b856a -->

```json
"a-riverhog-recovery-tool"
```

### `/external_contract/cli/a-riverhog-recovery-tool/parameters`

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

### `/external_contract/cli/a-riverhog-recovery-tool/result_contract`

<!-- exact-contract-value: 567ac57045681501de3e7d494cbede66d1c6d37a4fe036e073e9d668d9aa0c65 -->

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
  "identity": "a-riverhog-recovery-tool-cli-result/root/v1",
  "profile_id": "a-riverhog-recovery-tool-cli/v1",
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

### `/external_contract/cli/a-riverhog-recovery-tool/terminating_controls`

<!-- exact-contract-value: 7c1d1af72cefd7f89662abc329761a43af7dba3fff099c39c3f232362e6770a0 -->

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
      "distribution": "a-riverhog-recovery-tool",
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
