# Riverhog v1 bounded provenance volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-bounded-provenance-volume:cf66d993f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog-provenance` |
| Interface | `protocol` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

- `$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json
- `title`: Riverhog v1 bounded provenance volume
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `archive_generation` | yes | #/$defs/sha256 |  |
| `archive_tree_sha256` | yes | #/$defs/sha256 |  |
| `binding_range` | no | type="object"; fields=`file_count`, `first_file_order`; additional keys=`additionalProperties`, `required` |  |
| `journal_range` | no | type="object"; fields=`bytes`, `journal_id`, `offset`, `sha256`; additional keys=`additionalProperties`, `required` |  |
| `payload` | yes | type="object"; fields=`bytes`, `kind`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |  |
| `schema` | yes | const="riverhog-provenance-volume/v1" |  |
| `sequence` | yes | #/$defs/sequence |  |

### Definitions

| Definition | Shape |
|---|---|
| `sequence` | type="string"; pattern="^[0-9a-f]{64}$" |
| `sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

| Dimension | Unit | Policy | Bounds or reason |
|---|---|---|---|
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| length | characters | `fixed` | maximum=64, minimum=64, reason=fixed-public-representation |
| value | schema-value | `contract_max` | maximum=512, minimum=1, reason=schema-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |
| value | schema-value | `operational_policy` | maximum=None, reason=no-declared-semantic-maximum |

## Governing policies

- `compatibility/components/v1`
- `extent-rule/no-semantic-maximum/v1`
- `extent-rule/schema-bound/v1`

## Evidence

### Qualification

- `make dist-smoke`
- `make build`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json` — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-volume-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-volume-v1.schema.json`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 651e89ee007b6d340ef86662f3f052343a26f03cfac91327491edf6b2931c768 -->

```json
{
  "$defs": {
    "sequence": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    },
    "sha256": {
      "pattern": "^[0-9a-f]{64}$",
      "type": "string"
    }
  },
  "$id": "https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json",
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "additionalProperties": false,
  "oneOf": [
    {
      "not": {
        "required": [
          "journal_range"
        ]
      },
      "required": [
        "binding_range"
      ]
    },
    {
      "not": {
        "required": [
          "binding_range"
        ]
      },
      "required": [
        "journal_range"
      ]
    }
  ],
  "properties": {
    "archive_generation": {
      "$ref": "#/$defs/sha256"
    },
    "archive_tree_sha256": {
      "$ref": "#/$defs/sha256"
    },
    "binding_range": {
      "additionalProperties": false,
      "properties": {
        "file_count": {
          "maximum": 512,
          "minimum": 1,
          "type": "integer"
        },
        "first_file_order": {
          "minimum": 0,
          "type": "integer"
        }
      },
      "required": [
        "first_file_order",
        "file_count"
      ],
      "type": "object"
    },
    "journal_range": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "journal_id": {
          "pattern": "^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
          "type": "string"
        },
        "offset": {
          "minimum": 0,
          "type": "integer"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "journal_id",
        "offset",
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "payload": {
      "additionalProperties": false,
      "properties": {
        "bytes": {
          "minimum": 1,
          "type": "integer"
        },
        "kind": {
          "enum": [
            "bindings",
            "journal"
          ]
        },
        "path": {
          "pattern": "^provenance/payloads/volume-[0-9a-f]{64}\\.bin\\.age$",
          "type": "string"
        },
        "sha256": {
          "$ref": "#/$defs/sha256"
        }
      },
      "required": [
        "kind",
        "path",
        "bytes",
        "sha256"
      ],
      "type": "object"
    },
    "schema": {
      "const": "riverhog-provenance-volume/v1"
    },
    "sequence": {
      "$ref": "#/$defs/sequence"
    }
  },
  "required": [
    "schema",
    "archive_generation",
    "archive_tree_sha256",
    "sequence",
    "payload"
  ],
  "title": "Riverhog v1 bounded provenance volume",
  "type": "object"
}
```
