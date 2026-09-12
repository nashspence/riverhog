# Riverhog v1 bounded provenance file bindings

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-bounded-provenance-file-bindings:00ab173f22 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 4 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-bindings-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-bindings-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| cardinality | items | `segmented_no_total_max` | maximum=512, minimum=1, reason=bounded-provenance-binding-volume |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-bindings-v1.schema.json
- `title`: Riverhog v1 bounded provenance file bindings
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `files` | yes | array |  |
| `first_file_order` | yes | integer |  |
| `schema` | yes | object (1 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `file` | object |
| `sha256` | string |

## Complete owned contract

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
