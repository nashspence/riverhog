# Riverhog v1 bounded provenance volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: protocol:riverhog-provenance:riverhog-v1-bounded-provenance-volume:cf66d993f3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [protocol](index.md) |
| Family | [schemas](index.md#f-6adfbad66e) |
| Contract elements | 1 |
| Extent decisions | 7 |

## External contract

<a id="s-533d48e7e1"></a>
- <a id="s-2e49c00f44"></a>`$id`: https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json
- <a id="s-10939df91d"></a>`title`: Riverhog v1 bounded provenance volume
- <a id="s-a77370bb6d"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95119c3060"></a>`archive_generation` | yes | #/$defs/sha256 |  |
| <a id="s-56edc337cb"></a>`archive_tree_sha256` | yes | #/$defs/sha256 |  |
| <a id="s-9e51e10024"></a>`binding_range` | no | type="object"; fields=`file_count`, `first_file_order`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-5520638242"></a>`journal_range` | no | type="object"; fields=`bytes`, `journal_id`, `offset`, `sha256`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-037496be78"></a>`payload` | yes | type="object"; fields=`bytes`, `kind`, `path`, `sha256`; additional keys=`additionalProperties`, `required` |  |
| <a id="s-88ab8801ba"></a>`schema` | yes | const="riverhog-provenance-volume/v1" |  |
| <a id="s-777a2a8777"></a>`sequence` | yes | #/$defs/sequence |  |

### Definitions

| Definition | Shape |
|---|---|
| <a id="s-897f94e1be"></a>`sequence` | type="string"; pattern="^[0-9a-f]{64}$" |
| <a id="s-4908acae04"></a>`sha256` | type="string"; pattern="^[0-9a-f]{64}$" |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-6db1e32d01"></a>[field binding_range · field first_file_order](#s-9e51e10024) | `value · schema-value · operational_policy` | shared above |
| <a id="s-e496fce130"></a>[field journal_range · field bytes](#s-5520638242) | `value · schema-value · operational_policy` | shared above |
| <a id="s-6557662501"></a>[field journal_range · field offset](#s-5520638242) | `value · schema-value · operational_policy` | shared above |
| <a id="s-3052600ce7"></a>[field payload · field bytes](#s-037496be78) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sequence](#s-897f94e1be) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition sha256](#s-4908acae04) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| <a id="s-0917e1580c"></a>[field binding_range · field file_count](#s-9e51e10024) | `value · schema-value · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-1a6a9490f8"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)
- <a id="pa-8bcb149113"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-f9fb66644a"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../../../evidence/sources.md#src-a3bfff3737) — `packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-volume-v1.schema.json`

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
