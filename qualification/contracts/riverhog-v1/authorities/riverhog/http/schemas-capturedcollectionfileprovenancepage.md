# schemas: CapturedCollectionFileProvenancePage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-capturedcollectionfileprovenancepage:c13934717c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 3 |

## External contract

<a id="s-38985fdc82"></a>
- <a id="s-07130d5b80"></a>`title`: CapturedCollectionFileProvenancePage
- <a id="s-7d024fd078"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e743e31934"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-c1a45d0053"></a>`files` | yes | type="array"; items=(#/components/schemas/CapturedCollectionFileProvenanceOut) |  |
| <a id="s-514f560e2d"></a>`next_page_token` | yes | anyOf=#/components/schemas/BrowsePageToken \| type="null" |  |
| <a id="s-9bfe818c3e"></a>`order` | yes | #/components/schemas/SortOrder |  |
| <a id="s-21122411f5"></a>`page_size` | yes | type="integer"; minimum=1; maximum=100 |  |
| <a id="s-b2d8829d1f"></a>`provenance_identity` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-61cca968ad"></a>`provenance_mode` | yes | type="string"; const="captured" |  |
| <a id="s-8af0c2d14a"></a>`query` | yes | anyOf=type="string" \| type="null" |  |
| <a id="s-89d66c5715"></a>`sort` | yes | #/components/schemas/ProvenanceSort |  |
| <a id="s-1d6d15527b"></a>`status` | yes | anyOf=#/components/schemas/ProvenanceStatus \| type="null" |  |

### Progression, limits, and lifecycle

#### [extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)

Shared facts for every subject below: capacity_authority={"declared_maximum":null,"hidden_maximum":"forbidden","owner":"riverhog"}; maximum=null; reason="no-declared-semantic-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field files](#s-c1a45d0053) | `cardinality · items · operational_policy` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field page_size](#s-21122411f5) | `value · schema-value · contract_max` | maximum=100; minimum=1; reason="schema-maximum" |
| [field provenance_identity](#s-b2d8829d1f) | `length · characters · fixed` | maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"} |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BrowsePageToken](schemas-browsepagetoken.md)
- [schemas: CapturedCollectionFileProvenanceOut](schemas-capturedcollectionfileprovenanceout.md)
- [schemas: CollectionId](schemas-collectionid.md)
- [schemas: ProvenanceSort](schemas-provenancesort.md)
- [schemas: ProvenanceStatus](schemas-provenancestatus.md)
- [schemas: SortOrder](schemas-sortorder.md)

## Governing policies

- <a id="pa-2b393c19a0"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-e40e0dbea2"></a>[extent-rule/no-semantic-maximum/v1](../../../policies/index.md#p-574724b48a)
- <a id="pa-e4363ff595"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CapturedCollectionFileProvenancePage`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4770e02654d22def70ddb602e07041998409a1ff68fb06f56b9ea36fce04cdfb -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "files": {
      "items": {
        "$ref": "#/components/schemas/CapturedCollectionFileProvenanceOut"
      },
      "title": "Files",
      "type": "array"
    },
    "next_page_token": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/BrowsePageToken"
        },
        {
          "type": "null"
        }
      ]
    },
    "order": {
      "$ref": "#/components/schemas/SortOrder"
    },
    "page_size": {
      "maximum": 100,
      "minimum": 1,
      "title": "Page Size",
      "type": "integer"
    },
    "provenance_identity": {
      "pattern": "^[0-9a-f]{64}$",
      "title": "Provenance Identity",
      "type": "string"
    },
    "provenance_mode": {
      "const": "captured",
      "title": "Provenance Mode",
      "type": "string"
    },
    "query": {
      "anyOf": [
        {
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Query"
    },
    "sort": {
      "$ref": "#/components/schemas/ProvenanceSort"
    },
    "status": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ProvenanceStatus"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "page_size",
    "next_page_token",
    "sort",
    "order",
    "query",
    "status",
    "collection_id",
    "provenance_mode",
    "provenance_identity",
    "files"
  ],
  "title": "CapturedCollectionFileProvenancePage",
  "type": "object"
}
```
