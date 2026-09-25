# a-riverhog-minisign-witness ingest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-minisign-witness:a-riverhog-minisign-witness-ingest:bf71ee0032 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-minisign-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-cc84cf30ae"></a>Parser name: `ingest`
- <a id="s-783abcec43"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-eba2a92023"></a>`limit`<br>`--limit` | optional option; 1 value | int | `100` |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-55bd2e63fa"></a>`help` | <a id="s-cb57a9c1ad"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-d475bbd9bc"></a>`0` | <a id="s-1173c8bff0"></a>`"noncontractual-framework-help"` | <a id="s-ed295d4968"></a>`"empty"` |

### Result and failure contract

- <a id="s-deebd0b7aa"></a>Result identity: `a-riverhog-minisign-witness-cli-result/ingest/v1`
- <a id="s-1f3301a903"></a>Profile: `a-riverhog-minisign-witness-cli-json/v1`
- <a id="s-fa69a3e00c"></a>Structured output: `always-json`
- <a id="s-76ddc723bb"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-60df3c2cbb"></a>`completed` | <a id="s-70b0da3974"></a>`{"kind":"command-completed"}` | <a id="s-233a3420c2"></a>`0` | <a id="s-358f027acf"></a>json: [a-riverhog-minisign-witness-cli-ingest/v1](#s-eac316d214) | <a id="s-ad870101e3"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-39015c3983"></a>`usage` | <a id="s-90aa27ceeb"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-f9d27f1968"></a>`2` | <a id="s-8504ee20d2"></a>all: `"empty"` | <a id="s-bff0d55ecb"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-d53b59023b"></a>`application-error` | <a id="s-c50207b3dc"></a>`{"kind":"application-error"}` | <a id="s-0f618a01e7"></a>`1` | <a id="s-548be23a5b"></a>all: `"empty"` | <a id="s-b682c0e896"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-eac316d214"></a>`a-riverhog-minisign-witness-cli-ingest/v1`

Applies to: completed · stdout (json).

<a id="s-3fc493451e"></a>

- <a id="s-8c2c8ff337"></a>`type`: `"object"`
- <a id="s-2ea24d6488"></a>`additionalProperties`: `false`
- <a id="s-46d3905f6c"></a>`required`: `["catalog_batch","progress"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0e21262d2"></a>`catalog_batch` | yes | enum=["checkpoint","catalog","changes","reset"] |  |
| `progress` | yes | [See field `progress`](#s-7641671e8d) |  |

##### <a id="s-7641671e8d"></a>field `progress`

- <a id="s-a5140f2642"></a>`type`: `"object"`
- <a id="s-2c6582d15d"></a>`additionalProperties`: `false`
- <a id="s-ab0f57838d"></a>`required`: `["generation","serial","phase","source_identity","authorization_view_identity","through_revision","reset_reason"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ce9345f66"></a>`authorization_view_identity` | yes | type=["string","null"] |  |
| <a id="s-67b2d07154"></a>`generation` | yes | type="integer"; minimum=0 |  |
| <a id="s-3eedf30a0a"></a>`phase` | yes | enum=["new","catalog","catchup","following","reset_required"] |  |
| <a id="s-cd6449372b"></a>`reset_reason` | yes | type=["string","null"] |  |
| <a id="s-a7743b596c"></a>`serial` | yes | type="integer"; minimum=0 |  |
| <a id="s-19d7c4a3fd"></a>`source_identity` | yes | type=["string","null"] |  |
| <a id="s-109278f7ce"></a>`through_revision` | yes | type="string" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter --limit](#s-eba2a92023) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-2a440a5f73"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-971a838222"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-minisign-witness](../../../evidence/sources/authorities.md#src-ab8103c6de) — [some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a\_riverhog\_minisign\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-minisign-witness/src/a_riverhog_minisign_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/allow_abbrev`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/name`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/parameters`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/result_contract`
- `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/name`

<!-- exact-contract-value: 19ff220ea6f1cd1dd97cdc2b911a7e1b784e562a77a5bc9c66b5364aa6e36892 -->

```json
"ingest"
```

### `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/parameters`

<!-- exact-contract-value: f45516f40ce7b77554edf37a6d12ce45648927ca348f1fe81909d06df5d108e4 -->

```json
[
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/result_contract`

<!-- exact-contract-value: 45e730130e97fdde6419179949efda35004287a15bea56bbfac0cc8081fec025 -->

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
  "identity": "a-riverhog-minisign-witness-cli-result/ingest/v1",
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
          "identity": "a-riverhog-minisign-witness-cli-ingest/v1",
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

### `/external_contract/cli/a-riverhog-minisign-witness/commands/ingest/terminating_controls`

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
