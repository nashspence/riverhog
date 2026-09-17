# Riverhog v1 bounded provenance volume

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: schema:riverhog-provenance:riverhog-v1-bounded-provenance-volume:39378fceb8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-provenance](../index.md) |
| Interface | [Schemas](index.md) |

## External contract

<a id="s-533d48e7e1"></a>

- <a id="s-a77370bb6d"></a>`type`: `"object"`
- <a id="s-2e49c00f44"></a>`$id`: `"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json"`
- <a id="s-338f0870c6"></a>`$schema`: `"https://json-schema.org/draft/2020-12/schema"`
- <a id="s-ab473a6804"></a>`additionalProperties`: `false`
- <a id="s-e9ea457682"></a>`required`: `["schema","archive_generation","archive_tree_sha256","sequence","payload"]`
- <a id="s-10939df91d"></a>`title`: `"Riverhog v1 bounded provenance volume"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-95119c3060"></a>`archive_generation` | yes | [sha256](#s-4908acae04) |  |
| <a id="s-56edc337cb"></a>`archive_tree_sha256` | yes | [sha256](#s-4908acae04) |  |
| `binding_range` | no | [See field `binding_range`](#s-9e51e10024) |  |
| `journal_range` | no | [See field `journal_range`](#s-5520638242) |  |
| `payload` | yes | [See field `payload`](#s-037496be78) |  |
| <a id="s-88ab8801ba"></a>`schema` | yes | const="riverhog-provenance-volume/v1" |  |
| <a id="s-777a2a8777"></a>`sequence` | yes | [sequence](#s-897f94e1be) |  |

### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-2e314b9b9d"></a>1 | not=(required=["journal_range"]); required=["binding_range"] |
| <a id="s-03f6907dd2"></a>2 | not=(required=["binding_range"]); required=["journal_range"] |

### Definitions

- [sequence](#s-897f94e1be)
- [sha256](#s-4908acae04)

### <a id="s-9e51e10024"></a>field `binding_range`

- <a id="s-d5ee9e65e3"></a>`type`: `"object"`
- <a id="s-51c0e63fdc"></a>`additionalProperties`: `false`
- <a id="s-8d140f5fd6"></a>`required`: `["first_file_order","file_count"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-0917e1580c"></a>`file_count` | yes | type="integer"; minimum=1; maximum=512 |  |
| <a id="s-6db1e32d01"></a>`first_file_order` | yes | type="integer"; minimum=0 |  |

### <a id="s-5520638242"></a>field `journal_range`

- <a id="s-da670fa0ff"></a>`type`: `"object"`
- <a id="s-5a2e553a40"></a>`additionalProperties`: `false`
- <a id="s-a4665c326f"></a>`required`: `["journal_id","offset","bytes","sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e496fce130"></a>`bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-5059095bfb"></a>`journal_id` | yes | type="string"; pattern="^urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$" |  |
| <a id="s-6557662501"></a>`offset` | yes | type="integer"; minimum=0 |  |
| <a id="s-466436e15f"></a>`sha256` | yes | [sha256](#s-4908acae04) |  |

### <a id="s-037496be78"></a>field `payload`

- <a id="s-03bbcc79a4"></a>`type`: `"object"`
- <a id="s-67b10723f6"></a>`additionalProperties`: `false`
- <a id="s-d188942fc2"></a>`required`: `["kind","path","bytes","sha256"]`

#### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3052600ce7"></a>`bytes` | yes | type="integer"; minimum=1 |  |
| <a id="s-8b4a8b5ec1"></a>`kind` | yes | enum=["bindings","journal"] |  |
| <a id="s-7d230e7b40"></a>`path` | yes | type="string"; pattern="^provenance/payloads/volume-[0-9a-f]{64}\\.bin\\.age$" |  |
| <a id="s-0a7149a73c"></a>`sha256` | yes | [sha256](#s-4908acae04) |  |

### <a id="s-897f94e1be"></a>definition `sequence`

- <a id="s-5635e5c621"></a>`type`: `"string"`
- <a id="s-4a4e8f63b4"></a>`pattern`: `"^[0-9a-f]{64}$"`

### <a id="s-4908acae04"></a>definition `sha256`

- <a id="s-5398ae6573"></a>`type`: `"string"`
- <a id="s-d50e09f1f9"></a>`pattern`: `"^[0-9a-f]{64}$"`

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field binding_range · field first_file_order](#s-6db1e32d01) | `value · schema-value · operational_policy` | shared above |
| [field journal_range · field bytes](#s-e496fce130) | `value · schema-value · operational_policy` | shared above |
| [field journal_range · field offset](#s-6557662501) | `value · schema-value · operational_policy` | shared above |
| [field payload · field bytes](#s-3052600ce7) | `value · schema-value · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [definition sequence](#s-897f94e1be) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [definition sha256](#s-4908acae04) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |
| [field binding_range · field file_count](#s-0917e1580c) | `value · schema-value · contract_max` | maximum=512; minimum=1; reason="schema-maximum" |

## Governing policies

- <a id="pa-cac0e18f7e"></a>[compatibility/components/v1](../../release/compatibility-guarantees/compatibility-components.md#p-95e9a12259)
- <a id="pa-35684ce1ff"></a>[extent-rule/no-semantic-maximum/v1](../../extent-contract/extent/extent-rule-no-semantic-maximum.md#p-574724b48a)
- <a id="pa-f4c4abeed0"></a>[extent-rule/schema-bound/v1](../../extent-contract/extent/extent-rule-schema-bound.md#p-c0db822fc0)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [protocol:https://nashspence.github.io/riverhog/v1/schemas/riverhog-provenance-volume-v1.schema.json](../../../evidence/sources/authorities.md#src-a3bfff3737) — [packages/riverhog-provenance/src/riverhog\_provenance/schemas/riverhog-provenance-volume-v1.schema.json](../../../../../../packages/riverhog-provenance/src/riverhog_provenance/schemas/riverhog-provenance-volume-v1.schema.json)

### Machine authority

- `/external_contract/protocol_schemas/https:~1~1nashspence.github.io~1riverhog~1v1~1schemas~1riverhog-provenance-volume-v1.schema.json`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
