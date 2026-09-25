# a-riverhog-opentimestamps-witness verify

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-verify:fd257e1848 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-85aee92bf3"></a>Parser name: `verify`
- <a id="s-9897d670c9"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-be710d280a"></a>`digest` | required positional; 1 value | not recorded | not recorded |
| <a id="s-b86b827a4d"></a>`bitcoin_rpc_url`<br>`--bitcoin-rpc-url` | required option; 1 value | not recorded | not recorded |
| <a id="s-5fd43f3ea7"></a>`bitcoin_cookie`<br>`--bitcoin-cookie` | required option; 1 value | Path | not recorded |
| <a id="s-48494f9bd0"></a>`minimum_confirmations`<br>`--minimum-confirmations` | optional option; 1 value | int | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-9a4eb0e098"></a>`help` | <a id="s-ba89561ca3"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-8d6d3ceb4e"></a>`0` | <a id="s-86847971ce"></a>`"noncontractual-framework-help"` | <a id="s-2bf735d854"></a>`"empty"` |

### Result and failure contract

- <a id="s-10b8de26ce"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/verify/v1`
- <a id="s-bd1eae8d97"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-0afef28ff3"></a>Structured output: `always-json`
- <a id="s-e497d8e746"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-d28f8c1b28"></a>`completed` | <a id="s-d90a7057b7"></a>`{"kind":"command-completed"}` | <a id="s-718cf7120e"></a>`0` | <a id="s-2a94c6395e"></a>json: [a-riverhog-opentimestamps-witness-cli-verify/v1](#s-030f5296d1) | <a id="s-d1cd53d1ac"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-81ef363771"></a>`usage` | <a id="s-7263abc40d"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-5b55c2a308"></a>`2` | <a id="s-78e1e83ed8"></a>all: `"empty"` | <a id="s-7b4e5a1470"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-fe1e0ccb0d"></a>`application-error` | <a id="s-20c7fa59dc"></a>`{"kind":"application-error"}` | <a id="s-a918dcea04"></a>`1` | <a id="s-8561f2ff6d"></a>all: `"empty"` | <a id="s-87ed881778"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-030f5296d1"></a>`a-riverhog-opentimestamps-witness-cli-verify/v1`

Applies to: completed · stdout (json).

<a id="s-4380f73814"></a>

- <a id="s-08ff9fc650"></a>`type`: `"object"`
- <a id="s-ee625db5e5"></a>`additionalProperties`: `false`
- <a id="s-3f1a950015"></a>`required`: `["verification","confirmation_policy_met"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-60d24b9953"></a>`confirmation_policy_met` | yes | type="boolean" |  |
| `verification` | yes | [See field `verification`](#s-5e1b407c8c) |  |

##### <a id="s-5e1b407c8c"></a>field `verification`

- <a id="s-66f3971255"></a>`type`: `"object"`
- <a id="s-1213b382ba"></a>`additionalProperties`: `false`
- <a id="s-979b795a50"></a>`required`: `["proof_digest","checks","pending_count","unsupported_count"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `checks` | yes | [See field `verification` · field `checks`](#s-3db6c5d3a6) |  |
| <a id="s-2e6bd9ea3e"></a>`pending_count` | yes | type="integer"; minimum=0 |  |
| <a id="s-57bee2ceff"></a>`proof_digest` | yes | type="string" |  |
| <a id="s-0040fe66c3"></a>`unsupported_count` | yes | type="integer"; minimum=0 |  |

##### <a id="s-3db6c5d3a6"></a>field `verification` · field `checks`

- <a id="s-8cc06c67d1"></a>`type`: `"array"`
- `items`: [See field `verification` · field `checks` · `items`](#s-99869b195f)

##### <a id="s-99869b195f"></a>field `verification` · field `checks` · `items`

- <a id="s-91255c828b"></a>`type`: `"object"`
- <a id="s-446431a375"></a>`additionalProperties`: `false`
- <a id="s-a4f2a6b196"></a>`required`: `["status","height","reason","block_hash","block_time","confirmations","tip_hash"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b5fd82356b"></a>`block_hash` | yes | type=["string","null"] |  |
| <a id="s-fed529e095"></a>`block_time` | yes | type=["integer","null"]; minimum=0 |  |
| <a id="s-26c2ad8766"></a>`confirmations` | yes | type=["integer","null"]; minimum=0 |  |
| <a id="s-1d057d4946"></a>`height` | yes | type="integer"; minimum=0 |  |
| <a id="s-a571cfcb12"></a>`reason` | yes | type="string" |  |
| <a id="s-c3431df579"></a>`status` | yes | enum=["valid","invalid","unavailable"] |  |
| <a id="s-36d23c4406"></a>`tip_hash` | yes | type=["string","null"] |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-be710d280a) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --bitcoin-rpc-url](#s-b86b827a4d) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --bitcoin-cookie](#s-5fd43f3ea7) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --minimum-confirmations](#s-48494f9bd0) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-23244d31e0"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-dda5c6f76c"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/name`

<!-- exact-contract-value: 898c74c2eed0452b1e51e567f237c37f1caa1e52f747466d56e76e15d07dc331 -->

```json
"verify"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/parameters`

<!-- exact-contract-value: 25a05f3c477bdc2077ea9d37e5774db6ba654beeda8d99be792315f5c4f1731f -->

```json
[
  {
    "dest": "digest",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  },
  {
    "dest": "bitcoin_rpc_url",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--bitcoin-rpc-url"
    ],
    "required": true
  },
  {
    "dest": "bitcoin_cookie",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--bitcoin-cookie"
    ],
    "required": true,
    "type": "Path"
  },
  {
    "dest": "minimum_confirmations",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--minimum-confirmations"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/result_contract`

<!-- exact-contract-value: f7f55e2002471d1d5a3a71cd4daf130985294549841fc23177ee4273e233cdb5 -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/verify/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-verify/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "confirmation_policy_met": {
                "type": "boolean"
              },
              "verification": {
                "additionalProperties": false,
                "properties": {
                  "checks": {
                    "items": {
                      "additionalProperties": false,
                      "properties": {
                        "block_hash": {
                          "type": [
                            "string",
                            "null"
                          ]
                        },
                        "block_time": {
                          "minimum": 0,
                          "type": [
                            "integer",
                            "null"
                          ]
                        },
                        "confirmations": {
                          "minimum": 0,
                          "type": [
                            "integer",
                            "null"
                          ]
                        },
                        "height": {
                          "minimum": 0,
                          "type": "integer"
                        },
                        "reason": {
                          "type": "string"
                        },
                        "status": {
                          "enum": [
                            "valid",
                            "invalid",
                            "unavailable"
                          ]
                        },
                        "tip_hash": {
                          "type": [
                            "string",
                            "null"
                          ]
                        }
                      },
                      "required": [
                        "status",
                        "height",
                        "reason",
                        "block_hash",
                        "block_time",
                        "confirmations",
                        "tip_hash"
                      ],
                      "type": "object"
                    },
                    "type": "array"
                  },
                  "pending_count": {
                    "minimum": 0,
                    "type": "integer"
                  },
                  "proof_digest": {
                    "type": "string"
                  },
                  "unsupported_count": {
                    "minimum": 0,
                    "type": "integer"
                  }
                },
                "required": [
                  "proof_digest",
                  "checks",
                  "pending_count",
                  "unsupported_count"
                ],
                "type": "object"
              }
            },
            "required": [
              "verification",
              "confirmation_policy_met"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/verify/terminating_controls`

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
