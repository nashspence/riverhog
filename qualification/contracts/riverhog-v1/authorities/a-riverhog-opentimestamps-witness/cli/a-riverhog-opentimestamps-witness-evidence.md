# a-riverhog-opentimestamps-witness evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: cli:a-riverhog-opentimestamps-witness:a-riverhog-opentimestamps-witness-evidence:03dfb022c5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-riverhog-opentimestamps-witness](../index.md) |
| Interface | [CLI](index.md) |

## External contract

- <a id="s-1a18a4359a"></a>Parser name: `evidence`
- <a id="s-b7c0d6d5d4"></a>Unique long-option abbreviations: accepted.

### Parameters

Value counts describe supplied CLI values per occurrence. Defaults and environment inputs below are recorded parser metadata; **not recorded** does not imply an explicit null default or the absence of other fallbacks.

| Parameter / spelling | Invocation | Type / constraints | Default / environment |
|---|---|---|---|
| <a id="s-85db7a5223"></a>`digest` | required positional; 1 value | not recorded | not recorded |

### Terminating controls

| Identity | Trigger | Exit status | stdout | stderr |
|---|---|---:|---|---|
| <a id="s-15902f86a1"></a>`help` | <a id="s-0d9e0ff61b"></a>`{"kind":"option-present","options":["-h","--help"]}` | <a id="s-b7d07aeaee"></a>`0` | <a id="s-30b6ba8955"></a>`"noncontractual-framework-help"` | <a id="s-24993c2619"></a>`"empty"` |

### Result and failure contract

- <a id="s-1c6be34047"></a>Result identity: `a-riverhog-opentimestamps-witness-cli-result/evidence/v1`
- <a id="s-dbf87a8bfb"></a>Profile: `a-riverhog-opentimestamps-witness-cli-json/v1`
- <a id="s-ffd864d2de"></a>Structured output: `always-json`
- <a id="s-5d6d138d24"></a>Human/JSON relationship: `not-applicable`

#### Success outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-01bfcd7de8"></a>`completed` | <a id="s-bde813505a"></a>`{"kind":"command-completed"}` | <a id="s-441ca613eb"></a>`0` | <a id="s-5e407ee265"></a>json: [a-riverhog-opentimestamps-witness-cli-evidence/v1](#s-d4e1e5162b) | <a id="s-5afb806465"></a>all: `"empty"` |

#### Failure outcomes

| Identity | Selected by | Exit status | stdout | stderr |
|---|---|---|---|---|
| <a id="s-881e7f0595"></a>`usage` | <a id="s-de7e0dccd9"></a>`{"kind":"parser-rejected-invocation"}` | <a id="s-d637dfd759"></a>`2` | <a id="s-c7bc4f249f"></a>all: `"empty"` | <a id="s-abde307a9b"></a>all: `"noncontractual-usage-diagnostic"` |
| <a id="s-8aba98a5c2"></a>`application-error` | <a id="s-81ef3f9958"></a>`{"kind":"application-error"}` | <a id="s-e6e16187e4"></a>`1` | <a id="s-c7513c04d8"></a>all: `"empty"` | <a id="s-672866f5c5"></a>all: `"noncontractual-diagnostic"` |

### Local structured outputs


#### <a id="s-d4e1e5162b"></a>`a-riverhog-opentimestamps-witness-cli-evidence/v1`

Applies to: completed · stdout (json).

<a id="s-2f9e690808"></a>

- <a id="s-e5ce3c0f35"></a>`type`: `"object"`
- <a id="s-3e84191824"></a>`additionalProperties`: `false`
- <a id="s-17a5abc2cf"></a>`required`: `["digest","statement_base64","proof_base64","job_base64","proof_revisions","due"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-88fd150f86"></a>`digest` | yes | type="string" |  |
| <a id="s-22b98adca4"></a>`due` | yes | type=["integer","null"]; minimum=0 |  |
| <a id="s-325cf907b9"></a>`job_base64` | yes | type="string" |  |
| <a id="s-489e6fb8bb"></a>`proof_base64` | yes | type=["string","null"] |  |
| `proof_revisions` | yes | [See field `proof_revisions`](#s-93054a65fd) |  |
| <a id="s-43d3f2f65e"></a>`statement_base64` | yes | type="string" |  |

##### <a id="s-93054a65fd"></a>field `proof_revisions`

- <a id="s-130677d99c"></a>`type`: `"array"`
- `items`: [See field `proof_revisions` · `items`](#s-83f90764ab)

##### <a id="s-83f90764ab"></a>field `proof_revisions` · `items`

- <a id="s-6c0bad42fa"></a>`type`: `"object"`
- <a id="s-60d76bdcaf"></a>`additionalProperties`: `false`
- <a id="s-9555770565"></a>`required`: `["revision","proof_digest","recorded_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-b93e48c802"></a>`proof_digest` | yes | type="string" |  |
| <a id="s-f2f5d59577"></a>`recorded_at` | yes | type="integer"; minimum=0 |  |
| <a id="s-2a67276af9"></a>`revision` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

Shared facts for every subject below: maximum=1; minimum=1; reason="fixed-command-argument-arity"; source_constraint={"field":"nargs"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [CLI parameter 0](#s-85db7a5223) | `cardinality · values-per-occurrence · fixed` | shared above |

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a6eebef010"></a>[compatibility/cli/v1](../../release/compatibility-guarantees/compatibility-cli.md#p-48a89776de)
- <a id="pa-33a32d2704"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make operation-qualification](../../../evidence/sources/commands.md#q-dd95e4459f)

### Executable sources

- [cli:a-riverhog-opentimestamps-witness](../../../evidence/sources/authorities.md#src-26e499502f) — [some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a\_riverhog\_opentimestamps\_witness/cli.py::&lt;module&gt;](../../../../../../some-implementations/riverhog/applications/a-riverhog-opentimestamps-witness/src/a_riverhog_opentimestamps_witness/cli.py)
- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)

### Machine authority

- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/allow_abbrev`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/name`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/parameters`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/result_contract`
- `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/terminating_controls`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/allow_abbrev`

<!-- exact-contract-value: b5bea41b6c623f7c09f1bf24dcae58ebab3c0cdd90ad966bc43a45b44867e12b -->

```json
true
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/name`

<!-- exact-contract-value: edecec53ab11f560816e75a17681eb83886beace01d7fb927d99707d3d031aac -->

```json
"evidence"
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/parameters`

<!-- exact-contract-value: 77121347de116990d37ff93ff27cd2f5f7aabac98f79343ea6a4022cfff061e2 -->

```json
[
  {
    "dest": "digest",
    "kind": "_StoreAction",
    "nargs": null,
    "options": [],
    "required": true
  }
]
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/result_contract`

<!-- exact-contract-value: e2c136b73d14d3bdc9fb26920b7e4f624cf9a47309951b6cb0ee20e330f645c6 -->

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
  "identity": "a-riverhog-opentimestamps-witness-cli-result/evidence/v1",
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
          "identity": "a-riverhog-opentimestamps-witness-cli-evidence/v1",
          "kind": "cli-local-json-schema",
          "schema": {
            "additionalProperties": false,
            "properties": {
              "digest": {
                "type": "string"
              },
              "due": {
                "minimum": 0,
                "type": [
                  "integer",
                  "null"
                ]
              },
              "job_base64": {
                "type": "string"
              },
              "proof_base64": {
                "type": [
                  "string",
                  "null"
                ]
              },
              "proof_revisions": {
                "items": {
                  "additionalProperties": false,
                  "properties": {
                    "proof_digest": {
                      "type": "string"
                    },
                    "recorded_at": {
                      "minimum": 0,
                      "type": "integer"
                    },
                    "revision": {
                      "minimum": 0,
                      "type": "integer"
                    }
                  },
                  "required": [
                    "revision",
                    "proof_digest",
                    "recorded_at"
                  ],
                  "type": "object"
                },
                "type": "array"
              },
              "statement_base64": {
                "type": "string"
              }
            },
            "required": [
              "digest",
              "statement_base64",
              "proof_base64",
              "job_base64",
              "proof_revisions",
              "due"
            ],
            "type": "object"
          }
        }
      }
    }
  ]
}
```

### `/external_contract/cli/a-riverhog-opentimestamps-witness/commands/evidence/terminating_controls`

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
