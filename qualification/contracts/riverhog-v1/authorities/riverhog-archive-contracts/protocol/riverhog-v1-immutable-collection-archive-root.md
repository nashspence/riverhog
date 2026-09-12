# Riverhog v1 immutable collection archive root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-immutable-collection-archive-root:2e0361021c -->

| Audit field | Value |
|---|---|
| Authority | `riverhog-archive-contracts` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 5 |

## Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-manifest-v1.schema.json`

## Effective policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json` — `packages/riverhog-archive-contracts/schemas/collection-archive-manifest-v1.schema.json`
- Proof: `make dist-smoke`
- Proof: `make build`

## Extent decisions

| Dimension | Unit | Policy | Bounds/reason |
|---|---|---|---|
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Contract summary

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json
- `title`: Riverhog v1 immutable collection archive root
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_generation` | yes | #/$defs/sha256 |  |
| `format` | yes | object |  |
| `provenance` | no | #/$defs/provenance |  |
| `schema` | yes | object (1 fields) |  |
| `tree` | yes | #/$defs/tree |  |
| `volume_sequence` | yes | object |  |

### Definitions

| Definition | Shape |
|---|---|
| `provenance` | object |
| `provenance_root` | object |
| `sha256` | string |
| `tree` | object |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4717825d249f0b6484c03aedc9c2d41d25dd99a15b6a9c895c8b300fe6f1156 -->

```json
{
  "$comment": "This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveManifest is the canonical semantic, identity, and canonical-JSON authority.",
  "$defs": {
    "provenance": {
      "additionalProperties": false,
      "properties": {
        "identity": {
          "$ref": "#/$defs/sha256"
        },
        "root": {
          "$ref": "#/$defs/provenance_root"
        }
      },
      "required": [
        "identity",
        "root"
      ],
      "type": "object"
    },
    "provenance_root": {
      "additionalProperties": false,
      "properties": {
        "id": {
          "const": "provenance-root"
        },
        "kind": {
          "const": "provenance-root"
        },
        "path": {
          "const": "provenance/root.json.age"
        },
        "plaintext_bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
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
        "id",
        "kind",
        "path",
        "plaintext_bytes",
        "sha256",
        "stored_bytes",
        "stored_sha256"
      ],
      "type": "object"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "tree": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 0,
          "type": "integer"
        },
        "files": {
          "minimum": 1,
          "type": "integer"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "files",
        "bytes",
        "sha256"
      ],
      "type": "object"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "format": {
      "additionalProperties": false,
      "properties": {
        "encryption": {
          "const": "age-v1-scrypt"
        },
        "pack_index": {
          "const": "riverhog-pack-index/v1"
        },
        "part_digest": {
          "const": "sha256"
        },
        "selective_read": {
          "const": "age-chunk-range/v1"
        }
      },
      "required": [
        "encryption",
        "pack_index",
        "part_digest",
        "selective_read"
      ],
      "type": "object"
    },
    "provenance": {
      "$ref": "#/$defs/provenance"
    },
    "schema": {
      "const": "collection-archive-manifest/v1"
    },
    "tree": {
      "$ref": "#/$defs/tree"
    },
    "volume_sequence": {
      "additionalProperties": false,
      "properties": {
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "sha256"
      ],
      "type": "object"
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "format",
    "tree",
    "volume_sequence"
  ],
  "title": "Riverhog v1 immutable collection archive root",
  "type": "object"
}
```
