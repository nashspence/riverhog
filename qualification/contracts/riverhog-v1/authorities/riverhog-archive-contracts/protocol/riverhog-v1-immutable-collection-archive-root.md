# Riverhog v1 immutable collection archive root

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-archive-contracts:riverhog-v1-immutable-collection-archive-root:2e0361021c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-66d562e63c) |
| Contract elements | 1 |
| Extent decisions | 5 |

## External contract

<a id="s-6eb5b547a1"></a>
- <a id="s-161d22fcb6"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json
- <a id="s-59b7295b1c"></a>`title`: Riverhog v1 immutable collection archive root
- <a id="s-37b1703eda"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-191b5cabb5"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-aa139f3599"></a>`format` | yes | type="object"; fields=`encryption`, `pack_index`, `part_digest`, `selective_read`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-c57132ccc2"></a>`provenance` | no | #/$defs/provenance |  |
| <a id="s-664ff9b8bc"></a>`schema` | yes | const="collection-archive-manifest/v1" |  |
| <a id="s-d428c80832"></a>`tree` | yes | #/$defs/tree |  |
| <a id="s-86c4eff453"></a>`volume_sequence` | yes | type="object"; fields=`sha256`; additional keys=`additionalProperties`, `required` |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-7606eb291c"></a>`provenance` | type="object"; fields=`identity`, `root`; additional keys=`additionalProperties`, `required` |
| <a id="s-d91794edfc"></a>`provenance_root` | type="object"; fields=`id`, `kind`, `path`, `plaintext_bytes`, `sha256`, `stored_bytes`, `stored_sha256`; additional keys=`additionalProperties`, `required` |
| <a id="s-e2bfff1bf6"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-9c81514a27"></a>`tree` | type="object"; fields=`bytes`, `files`, `sha256`; additional keys=`additionalProperties`, `required` |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-034a2d82de"></a>[definition provenance_root · field plaintext_bytes](#s-d91794edfc) | `value · schema-value · operational_policy` | shared above |
| <a id="s-cf7e974cb4"></a>[definition provenance_root · field stored_bytes](#s-d91794edfc) | `value · schema-value · operational_policy` | shared above |
| <a id="s-b8c328ddc3"></a>[definition tree · field bytes](#s-9c81514a27) | `value · schema-value · operational_policy` | shared above |
| <a id="s-77bf49c805"></a>[definition tree · field files](#s-9c81514a27) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sha256](#s-e2bfff1bf6) | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-ef3ae0c1f5"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-85a073cb41"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-6c1b5303cd"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-manifest-v1.schema.json](../../../evidence/sources.md#src-fc0a3e3cab) — `packages/riverhog-archive-contracts/schemas/collection-archive-manifest-v1.schema.json`

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-manifest-v1.schema.json`

### Exact owned JSON

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
