# a-riverhog-opentimestamps-witness ingest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-ingest:99ae9c34a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-161b957135"></a>Parser name: `ingest`
- <a id="s-2b45d07ecc"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-eff212f199"></a>`calendar`<br>`--calendar` | required option; 1 value; collects repeats; no declared occurrence maximum | not recorded | not recorded |
| <a id="s-763c081191"></a>`limit`<br>`--limit` | optional option; 1 value | int | `100` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-6d44cce80a"></a>`help` | <a id="s-c88262d2be"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-7ade8882a0"></a>`0` | <a id="s-de68898ce3"></a>`"noncontractual-framework-help"` | <a id="s-c6fc82d7d3"></a>`"empty"` |

### Result and failure contract

- <a id="s-c30c000713"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/ingest/v1`
- <a id="s-70774736e4"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-f81b63d017"></a>Structured output: `always-json`
- <a id="s-d1c0509c57"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-bd34549856"></a>`completed` | <a id="s-ad2be8eea4"></a>`{"kind":"command-completed"}` | <a id="s-1e80940531"></a>`0` | <a id="s-f3f5c8095f"></a>json: [a-riverhog-opentimestamps-witness-cli-ingest/v1](#s-50f753987f) | <a id="s-67b8d84341"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-060f51be70"></a>`usage` | <a id="s-e1675a9689"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-939ec21b33"></a>`2` | <a id="s-f4646d14fb"></a>all: `"empty"` | <a id="s-c535b1acfe"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-82b8cbe796"></a>`application-error` | <a id="s-2b690b5f73"></a>`{"kind":"application-error"}` | <a id="s-4ffc7165f2"></a>`1` | <a id="s-bf6bdbbbe4"></a>all: `"empty"` | <a id="s-d54b5ee415"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-50f753987f"></a>`a-riverhog-opentimestamps-witness-cli-ingest/v1`

Applies to: completed · stdout (json).

<a id="s-c149a22ffb"></a>

- <a id="s-4b901d19f3"></a>`type`: `"object"`
- <a id="s-d7bd82d6b7"></a>`additionalProperties`: `false`
- <a id="s-aca57e7445"></a>`required`: `["catalog_batch","progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-25a5ad1624"></a>`catalog_batch` | yes | enum=["checkpoint","catalog","changes","reset"] |  |
| `progress` | yes | [See field `progress`](#s-34fb865d21) |  |

##### <a id="s-34fb865d21"></a>field `progress`

- <a id="s-867b64ee67"></a>`type`: `"object"`
- <a id="s-d10165bddb"></a>`additionalProperties`: `false`
- <a id="s-dccac354e0"></a>`required`: `["generation","serial","phase","source_identity","authorization_view_identity","through_revision","reset_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d906f7801d"></a>`authorization_view_identity` | yes | type=["string","null"] |  |
| <a id="s-ddcafdc107"></a>`generation` | yes | type="integer"; minimum=0 |  |
| <a id="s-a785728d39"></a>`phase` | yes | enum=["new","catalog","catchup","following","reset_required"] |  |
| <a id="s-cb7e65f5b2"></a>`reset_reason` | yes | type=["string","null"] |  |
| <a id="s-318f3039d7"></a>`serial` | yes | type="integer"; minimum=0 |  |
| <a id="s-b8cbb9cef4"></a>`source_identity` | yes | type=["string","null"] |  |
| <a id="s-fae5b3bf09"></a>`through_revision` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"a-riverhog-opentimestamps-witness"}; maximum=null; reason="no-declared-semantic-maximum"; source_constraint={"field":"kind"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-eff212f199) | `cardinality · occurrences · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --calendar](#s-eff212f199) | `cardinality · values-per-occurrence · fixed` | shared above |
| [CLI parameter --limit](#s-763c081191) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-167c23c70a"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-6d54d1c03c"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-3317715737"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/name`

<!-- exact-contract-value: 19ff220ea6f1cd1dd97cdc2b911a7e1b784e562a77a5bc9c66b5364aa6e36892 -->

```json
"ingest"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/parameters`

<!-- exact-contract-value: e0c7eb05a17854f2ac5886a25fc3f2c057430769a51d397c1234a11f00a82e23 -->

```json
[
  {
    "dest": "calendar",
    "kind": "_AppendAction",
    "nargs": null,
    "options": [
      "--calendar"
    ],
    "required": true
  },
  {
    "default": 100,
    "dest": "limit",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [
      "--limit"
    ],
    "required": false,
    "type": "int"
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/result_contract`

<!-- exact-contract-value: 7d550eb98212e7cb7bcc2449447463faa57b5c667ac6d6879085e729f0741ab6 -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/ingest/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-ingest/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "catalog_batch": {
                "enum": [
                  "checkpoint",
                  "catalog",
                  "changes",
                  "reset"
                ]
              },
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
              "catalog_batch",
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

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/ingest/terminating_controls`

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
