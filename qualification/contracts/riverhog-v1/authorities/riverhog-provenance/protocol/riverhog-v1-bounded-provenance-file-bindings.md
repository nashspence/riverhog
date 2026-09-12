# Riverhog v1 bounded provenance file bindings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-bounded-provenance-file-bindings:00ab173f22 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66e) |
| Contract elements | 1 |
| Extent decisions | 4 |

## External contract

<a id="s-8fae636dba"></a>
- <a id="s-161c46ee86"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json
- <a id="s-bfaa76465d"></a>`title`: Riverhog v1 bounded provenance file bindings
- <a id="s-926a3b93f1"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6fb3fdf31"></a>`files` | yes | type="array"; minItems=1; maxItems=512; items=(#/$defs/file); additional keys=`x-riverhog-extent` |  |
| <a id="s-809fd78862"></a>`first_file_order` | yes | type="integer"; minimum=0 |  |
| <a id="s-14600985bd"></a>`schema` | yes | const="riverhog-provenance-bindings/v1" |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-25211fe36a"></a>`file` | type="object"; fields=`bytes`, `current_state_id`, `journal_id`, `omission_reason`, `path`, `sha256`, `status`; oneOf=fields=`status`; additional keys=`not`, `required` \| fields=`status`; additional keys=`not`, `required`; additional keys=`additionalProperties`, `required` |
| <a id="s-8a9c94c093"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=512; minimum=1; progression={"progression":"ordered-provenance-volume-sequence"}; reason="bounded-provenance-binding-volume"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-f6fb3fdf31) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ba7ec24ff1"></a>[definition file · field bytes](#s-25211fe36a) | `value · schema-value · operational_policy` | shared above |
| [field first_file_order](#s-809fd78862) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-8a9c94c093) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-578b00c606"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-b6473c1fde"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)
- <a id="pa-8069a48953"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-e01431ffcb"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json](../../../evidence/sources.md#src-c8e0251dd9) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-bindings-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-bindings-v1.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d7110f2ea76191dcd77d5912a2b2c88ed991997815350f60d93fb40a42c5abc8 -->

```json
{
  "$defs": {
    "file": {
      "additionalProperties": false,
      "oneOf": [
        {
          "not": {
            "required": [
              "omission_reason"
            ]
          },
          "properties": {
            "status": {
              "const": "captured"
            }
          },
          "required": [
            "journal_id",
            "current_state_id"
          ]
        },
        {
          "not": {
            "anyOf": [
              {
                "required": [
                  "journal_id"
                ]
              },
              {
                "required": [
                  "current_state_id"
                ]
              }
            ]
          },
          "properties": {
            "status": {
              "const": "omitted"
            }
          },
          "required": [
            "omission_reason"
          ]
        }
      ],
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "current_state_id": {
          "type": "string"
        },
        "journal_id": {
          "type": "string"
        },
        "omission_reason": {
          "minLength": 1,
          "type": "string"
        },
        "path": {
          "minLength": 1,
          "type": "string"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        },
        "status": {
          "enum": [
            "captured",
            "omitted"
          ]
        }
      },
      "required": [
        "path",
        "bytes",
        "sha256",
        "status"
      ],
      "type": "object"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "files": {
      "items": {
        "$ref": "#/$defs/file"
      },
      "maxItems": 512,
      "minItems": 1,
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "ordered-provenance-volume-sequence",
        "reason": "bounded-provenance-binding-volume"
      }
    },
    "first_file_order": {
      "minimum": 0,
      "type": "integer"
    },
    "schema": {
      "const": "riverhog-provenance-bindings/v1"
    }
  },
  "required": [
    "schema",
    "first_file_order",
    "files"
  ],
  "title": "Riverhog v1 bounded provenance file bindings",
  "type": "object"
}
```
