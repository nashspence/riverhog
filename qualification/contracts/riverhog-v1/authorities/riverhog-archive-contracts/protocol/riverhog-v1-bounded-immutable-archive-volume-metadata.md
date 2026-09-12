# Riverhog v1 bounded immutable archive-volume metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-bounded-immutable-archive-vol-a08fdea82e:4b65368595 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 14 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-volume-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/bounded-segment/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json` — `packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `contract_max` | maximum=50000, minimum=1, reason=schema-maximum |
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-archive-volume-parts |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| cardinality | items | `segmented_no_total_max` | maximum=1024, minimum=1, reason=bounded-archive-volume-parts |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json
- `title`: Riverhog v1 bounded immutable archive-volume metadata
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_generation` | yes | #/$defs/sha256 |  |
| `archive_tree_sha256` | yes | #/$defs/sha256 |  |
| `schema` | yes | object (1 fields) |  |
| `volume` | yes | object (1 fields) |  |

### Definitions

| Definition | Shape |
|---|---|
| `age_state` | object |
| `pack` | object |
| `part` | object |
| `segment` | object |
| `segment_file` | object |
| `sequence` | string |
| `sha256` | string |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9d5112f95a41ed1ffadda2d71dffc9d19931ceda73534b77f7ee4c321b951ccf -->

```json
{
  "$comment": "This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveVolumeDocument is the canonical semantic, identity, and canonical-JSON authority.",
  "$defs": {
    "age_state": {
      "additionalProperties": false,
      "properties": {
        "format": {
          "const": "age-v1-scrypt-resumable"
        },
        "header_b64": {
          "pattern": "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}|[A-Za-z0-9+/]{3})$",
          "type": "string"
        },
        "payload_nonce_b64": {
          "pattern": "^[A-Za-z0-9+/]{21}[AQgw]$",
          "type": "string"
        },
        "plaintext_size": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "format",
        "header_b64",
        "payload_nonce_b64",
        "plaintext_size"
      ],
      "type": "object"
    },
    "pack": {
      "additionalProperties": false,
      "properties": {
        "age_state": {
          "$ref": "#/$defs/age_state"
        },
        "files": {
          "maximum": 50000,
          "minimum": 1,
          "type": "integer"
        },
        "id": {
          "pattern": "^pack-[0-9a-f]{64}$",
          "type": "string"
        },
        "index_sha256": {
          "$ref": "#/$defs/sha256"
        },
        "kind": {
          "const": "pack"
        },
        "parts": {
          "items": {
            "$ref": "#/$defs/part"
          },
          "maxItems": 1024,
          "minItems": 1,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "ordered-archive-volume-sequence",
            "reason": "bounded-archive-volume-parts"
          }
        },
        "path": {
          "pattern": "^volumes/pack-[0-9a-f]{64}\\.tar\\.age$",
          "type": "string"
        },
        "plaintext_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "plan_sha256": {
          "$ref": "#/$defs/sha256"
        },
        "sequence": {
          "$ref": "#/$defs/sequence"
        },
        "source_bytes": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "id",
        "sequence",
        "kind",
        "path",
        "files",
        "source_bytes",
        "plaintext_bytes",
        "age_state",
        "index_sha256",
        "plan_sha256",
        "parts"
      ],
      "type": "object"
    },
    "part": {
      "additionalProperties": false,
      "properties": {
        "number": {
          "minimum": 1,
          "type": "integer"
        },
        "plaintext_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "plaintext_sha256": {
          "$ref": "#/$defs/sha256"
        },
        "plaintext_start": {
          "minimum": 0,
          "type": "integer"
        },
        "stored_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "stored_sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "number",
        "plaintext_start",
        "plaintext_bytes",
        "plaintext_sha256",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "segment": {
      "additionalProperties": false,
      "properties": {
        "age_state": {
          "$ref": "#/$defs/age_state"
        },
        "file": {
          "$ref": "#/$defs/segment_file"
        },
        "id": {
          "pattern": "^segment-[0-9a-f]{64}$",
          "type": "string"
        },
        "kind": {
          "const": "segment"
        },
        "parts": {
          "items": {
            "$ref": "#/$defs/part"
          },
          "maxItems": 1024,
          "minItems": 1,
          "type": "array",
          "x-riverhog-extent": {
            "policy": "segmented_no_total_max",
            "progression": "ordered-archive-volume-sequence",
            "reason": "bounded-archive-volume-parts"
          }
        },
        "path": {
          "pattern": "^volumes/segment-[0-9a-f]{64}\\.bin\\.age$",
          "type": "string"
        },
        "plaintext_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "sequence": {
          "$ref": "#/$defs/sequence"
        }
      },
      "required": [
        "id",
        "sequence",
        "kind",
        "path",
        "plaintext_bytes",
        "age_state",
        "file",
        "parts"
      ],
      "type": "object"
    },
    "segment_file": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "file_bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "offset": {
          "minimum": 0,
          "type": "integer"
        },
        "path": {
          "pattern": "^(?!/)(?!\\.riverhog/)(?!.*\\\\)(?!.*(?:^|/)\\.\\.?(?:/|$))[^/]+(?:/[^/]+)*$",
          "type": "string"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "path",
        "offset",
        "bytes",
        "file_bytes",
        "sha256"
      ],
      "type": "object"
    },
    "sequence": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "archive_tree_sha256": {
      "$ref": "#/$defs/sha256"
    },
    "schema": {
      "const": "collection-archive-volume/v1"
    },
    "volume": {
      "oneOf": [
        {
          "$ref": "#/$defs/pack"
        },
        {
          "$ref": "#/$defs/segment"
        }
      ]
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "archive_tree_sha256",
    "volume"
  ],
  "title": "Riverhog v1 bounded immutable archive-volume metadata",
  "type": "object"
}
```
