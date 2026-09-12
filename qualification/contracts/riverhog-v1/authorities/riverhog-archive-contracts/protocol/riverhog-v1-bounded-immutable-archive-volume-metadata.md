# Riverhog v1 bounded immutable archive-volume metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-bounded-immutable-archive-vol-a08fdea82e:4b65368595 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-66d562e63c42) |
| Contract elements | 1 |
| Extent decisions | 14 |

## External contract

<a id="s-ce2cf87e5742"></a>
- <a id="s-bc2f6d661f59"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json
- <a id="s-2f76d289d3b6"></a>`title`: Riverhog v1 bounded immutable archive-volume metadata
- <a id="s-60d42bdc3cfb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de6b8071efc3"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-2b2ac2242507"></a>`archive_tree_sha256` | yes | #/$defs/sha256 |  |
| <a id="s-d994a7bad514"></a>`schema` | yes | const="collection-archive-volume/v1" |  |
| <a id="s-80ca73252867"></a>`volume` | yes | oneOf=#/$defs/pack \| #/$defs/segment |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-a24d2b35e2dd"></a>`age_state` | type="object"; fields=`format`, `header_b64`, `payload_nonce_b64`, `plaintext_size`; additional keys=`additionalProperties`, `required` |
| <a id="s-c6222c661b2c"></a>`pack` | type="object"; fields=`age_state`, `files`, `id`, `index_sha256`, `kind`, `parts`, `path`, `plaintext_bytes`, `plan_sha256`, `sequence`, `source_bytes`; additional keys=`additionalProperties`, `required` |
| <a id="s-014f1ca7cda4"></a>`part` | type="object"; fields=`number`, `plaintext_bytes`, `plaintext_sha256`, `plaintext_start`, `stored_bytes`, `stored_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-ebbbf2c54616"></a>`segment` | type="object"; fields=`age_state`, `file`, `id`, `kind`, `parts`, `path`, `plaintext_bytes`, `sequence`; additional keys=`additionalProperties`, `required` |
| <a id="s-e6e7129d1bb5"></a>`segment_file` | type="object"; fields=`bytes`, `file_bytes`, `offset`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-b18c6e158b8b"></a>`sequence` | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-ddfc70e9874d"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=1024; minimum=1; progression={"progression":"ordered-archive-volume-sequence"}; reason="bounded-archive-volume-parts"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-441b6efdf737"></a>definition pack · field parts | `cardinality · items · segmented_no_total_max` | shared above |
| <a id="s-a600910b2040"></a>definition segment · field parts | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-b2649969b16e"></a>definition age_state · field plaintext_size | `value · schema-value · operational_policy` | shared above |
| <a id="s-8ea23828264f"></a>definition pack · field plaintext_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-b77a5632ca22"></a>definition pack · field source_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-42f53b0221a0"></a>definition part · field plaintext_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-b2f19d26568a"></a>definition part · field stored_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-bc57786779e4"></a>definition segment · field plaintext_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-bffb1edd6d65"></a>definition segment_file · field bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-dd97b15ddc2d"></a>definition segment_file · field file_bytes | `value · schema-value · operational_policy` | shared above |
| <a id="s-bccb340a336e"></a>definition segment_file · field offset | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-a00b69b25682"></a>definition pack · field files | `value · schema-value · contract_max` | maximum=50000; minimum=1; reason="schema-maximum" |
| [definition sequence](#s-b18c6e158b8b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition sha256](#s-ddfc70e9874d) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Governing policies

- <a id="pa-59747a26401f"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a1225947)
- <a id="pa-42a267ada0c4"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)
- <a id="pa-e6986a89100c"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48af0)
- <a id="pa-4f8f223fdbc4"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3eb7)
- [make build](../../../evidence/sources.md#q-d1121e35fa7a)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../../../evidence/sources.md#src-c83e346f0656) — `packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-volume-v1.schema.json`

### Exact owned JSON

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
