# Riverhog v1 bounded immutable archive-volume metadata

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-archive-contracts:riverhog-v1-bounded-immutable-archive-vol-a08fdea82e:d34e83129d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-archive-contracts](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-ce2cf87e57"></a>

- <a id="s-60d42bdc3c"></a>`type`: `"object"`
- <a id="s-a463467864"></a>`$comment`: `"This schema is the structural projection. riverhog_archive_contracts.CollectionArchiveVolumeDocument is the canonical semantic, identity, and canonical-JSON authority."`
- <a id="s-bc2f6d661f"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json"`
- <a id="s-b4f04bc7aa"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-80e67f1916"></a>`additionalProperties`: `false`
- <a id="s-6681c0593b"></a>`required`: `["schema","archive_generation","archive_tree_sha256","volume"]`
- <a id="s-2f76d289d3"></a>`title`: `"Riverhog v1 bounded immutable archive-volume metadata"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-de6b8071ef"></a>`archive_generation` | yes | [sha256](#s-ddfc70e987) |  |
| <a id="s-2b2ac22425"></a>`archive_tree_sha256` | yes | [sha256](#s-ddfc70e987) |  |
| <a id="s-d994a7bad5"></a>`schema` | yes | const="collection-archive-volume/v1" |  |
| <a id="s-80ca732528"></a>`volume` | yes | oneOf=[([pack](#s-c6222c661b)); ([segment](#s-ebbbf2c546))] |  |

### Definitions

- [age_state](#s-a24d2b35e2)
- [pack](#s-c6222c661b)
- [part](#s-014f1ca7cd)
- [segment](#s-ebbbf2c546)
- [segment_file](#s-e6e7129d1b)
- [sequence](#s-b18c6e158b)
- [sha256](#s-ddfc70e987)

### <a id="s-a24d2b35e2"></a>definition `age_state`

- <a id="s-56166ac4db"></a>`type`: `"object"`
- <a id="s-9bf2cbf4f2"></a>`additionalProperties`: `false`
- <a id="s-4718b88609"></a>`required`: `["format","header_b64","payload_nonce_b64","plaintext_size"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0dc8d691c1"></a>`format` | yes | const="age-v1-scrypt-resumable" |  |
| <a id="s-e0b7bf1db4"></a>`header_b64` | yes | type="string"; pattern="^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}\|[A-Za-z0-9+/]{3})$" |  |
| <a id="s-d97100a2c2"></a>`payload_nonce_b64` | yes | type="string"; pattern="^[A-Za-z0-9+/]{21}[AQgw]$" |  |
| <a id="s-b2649969b1"></a>`plaintext_size` | yes | type="integer"; minimum=0 |  |

### <a id="s-c6222c661b"></a>definition `pack`

- <a id="s-b01a7cdada"></a>`type`: `"object"`
- <a id="s-2949a9b419"></a>`additionalProperties`: `false`
- <a id="s-de56e4a329"></a>`required`: `["id","sequence","kind","path","files","source_bytes","plaintext_bytes","age_state","index_sha256","plan_sha256","parts"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-62a8734524"></a>`age_state` | yes | [age_state](#s-a24d2b35e2) |  |
| <a id="s-a00b69b256"></a>`files` | yes | type="integer"; minimum=1; maximum=50000 |  |
| <a id="s-b631d1dd5c"></a>`id` | yes | type="string"; pattern="^pack-[0-9a-f]{64}$" |  |
| <a id="s-70c7682c12"></a>`index_sha256` | yes | [sha256](#s-ddfc70e987) |  |
| <a id="s-166de2d721"></a>`kind` | yes | const="pack" |  |
| <a id="s-441b6efdf7"></a>`parts` | yes | type="array"; items=([part](#s-014f1ca7cd)); maxItems=1024; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"ordered-archive-volume-sequence","reason":"bounded-archive-volume-parts"} |  |
| <a id="s-490785bc32"></a>`path` | yes | type="string"; pattern="^volumes/pack-[0-9a-f]{64}\\.tar\\.age$" |  |
| <a id="s-8ea2382826"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-32da7b7d17"></a>`plan_sha256` | yes | [sha256](#s-ddfc70e987) |  |
| <a id="s-1744385743"></a>`sequence` | yes | [sequence](#s-b18c6e158b) |  |
| <a id="s-b77a5632ca"></a>`source_bytes` | yes | type="integer"; minimum=0 |  |

### <a id="s-014f1ca7cd"></a>definition `part`

- <a id="s-12530de252"></a>`type`: `"object"`
- <a id="s-36d46b588f"></a>`additionalProperties`: `false`
- <a id="s-3f71b4f56f"></a>`required`: `["number","plaintext_start","plaintext_bytes","plaintext_sha256","stored_bytes","stored_sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-83390965df"></a>`number` | yes | type="integer"; minimum=1 |  |
| <a id="s-42f53b0221"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-f67630bc79"></a>`plaintext_sha256` | yes | [sha256](#s-ddfc70e987) |  |
| <a id="s-fb9a0deae3"></a>`plaintext_start` | yes | type="integer"; minimum=0 |  |
| <a id="s-b2f19d2656"></a>`stored_bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-8c4daa8978"></a>`stored_sha256` | yes | [sha256](#s-ddfc70e987) |  |

### <a id="s-ebbbf2c546"></a>definition `segment`

- <a id="s-892d3d77c4"></a>`type`: `"object"`
- <a id="s-842ffb9e53"></a>`additionalProperties`: `false`
- <a id="s-2902825e7a"></a>`required`: `["id","sequence","kind","path","plaintext_bytes","age_state","file","parts"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-53030700e8"></a>`age_state` | yes | [age_state](#s-a24d2b35e2) |  |
| <a id="s-be6bf5af63"></a>`file` | yes | [segment_file](#s-e6e7129d1b) |  |
| <a id="s-e13ae5b696"></a>`id` | yes | type="string"; pattern="^segment-[0-9a-f]{64}$" |  |
| <a id="s-454da2ad8e"></a>`kind` | yes | const="segment" |  |
| <a id="s-a600910b20"></a>`parts` | yes | type="array"; items=([part](#s-014f1ca7cd)); maxItems=1024; minItems=1; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"ordered-archive-volume-sequence","reason":"bounded-archive-volume-parts"} |  |
| <a id="s-57e799246e"></a>`path` | yes | type="string"; pattern="^volumes/segment-[0-9a-f]{64}\\.bin\\.age$" |  |
| <a id="s-bc57786779"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-2d8923bce5"></a>`sequence` | yes | [sequence](#s-b18c6e158b) |  |

### <a id="s-e6e7129d1b"></a>definition `segment_file`

- <a id="s-615b559bc8"></a>`type`: `"object"`
- <a id="s-3460a5c531"></a>`additionalProperties`: `false`
- <a id="s-08eeb70537"></a>`required`: `["path","offset","bytes","file_bytes","sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bffb1edd6d"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-dd97b15ddc"></a>`file_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-bccb340a33"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-ff0c8b1f52"></a>`path` | yes | type="string"; pattern="^(?!/)(?!\\.riverhog/)(?!.*\\\\)(?!.*(?:^\|/)\\.\\.?(?:/\|$))[^/]+(?:/[^/]+)*$" |  |
| <a id="s-2862954f72"></a>`sha256` | yes | [sha256](#s-ddfc70e987) |  |

### <a id="s-b18c6e158b"></a>definition `sequence`

- <a id="s-b9cc90cffa"></a>`type`: `"string"`
- <a id="s-81e41a8de5"></a>`pattern`: `"^[0-9a-f]{64}$"`

### <a id="s-ddfc70e987"></a>definition `sha256`

- <a id="s-961ad6d906"></a>`type`: `"string"`
- <a id="s-10dff615fa"></a>`pattern`: `"^[0-9a-f]{64}$"`

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=1024; minimum=1; progression={"progression":"ordered-archive-volume-sequence"}; reason="bounded-archive-volume-parts"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition pack · field parts](#s-441b6efdf7) | `cardinality · items · segmented_no_total_max` | shared above |
| [definition segment · field parts](#s-a600910b20) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition age_state · field plaintext_size](#s-b2649969b1) | `value · schema-value · operational_policy` | shared above |
| [definition pack · field plaintext_bytes](#s-8ea2382826) | `value · schema-value · operational_policy` | shared above |
| [definition pack · field source_bytes](#s-b77a5632ca) | `value · schema-value · operational_policy` | shared above |
| [definition part · field plaintext_bytes](#s-42f53b0221) | `value · schema-value · operational_policy` | shared above |
| [definition part · field stored_bytes](#s-b2f19d2656) | `value · schema-value · operational_policy` | shared above |
| [definition segment · field plaintext_bytes](#s-bc57786779) | `value · schema-value · operational_policy` | shared above |
| [definition segment_file · field bytes](#s-bffb1edd6d) | `value · schema-value · operational_policy` | shared above |
| [definition segment_file · field file_bytes](#s-dd97b15ddc) | `value · schema-value · operational_policy` | shared above |
| [definition segment_file · field offset](#s-bccb340a33) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition pack · field files](#s-a00b69b256) | `value · schema-value · contract_max` | maximum=50000; minimum=1; reason="schema-maximum" |
| [definition sequence](#s-b18c6e158b) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition sha256](#s-ddfc70e987) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

### Evidence gaps

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

Required by: [extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594).

Exact evidence groups for this contract element:

- [riverhog-archive-volume-part-progression/v1](../../../evidence/qualifications/riverhog-archive-volume-part-progression-v1/index.md)

## Governing policies

[Extent principles](../../../policies/extent_principles/index.md) govern all extent rules and recorded decisions.

- <a id="pa-a267938e80"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-dcfd3160a7"></a>[extent-rule/bounded-segment/v1](../../extent-contract/extent/extent-rule-bounded-segment.md#p-2b3f3f1594)
- <a id="pa-595032d4f7"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-d57db52fb0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/collection-archive-volume-v1.schema.json](../../../evidence/sources/authorities.md#src-c83e346f06) — [packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json](../../../../../../packages/riverhog-archive-contracts/schemas/collection-archive-volume-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1collection-archive-volume-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
