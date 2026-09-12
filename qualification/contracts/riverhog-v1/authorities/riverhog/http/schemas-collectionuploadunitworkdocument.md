# schemas: CollectionUploadUnitWorkDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadunitworkdocument:97931cc282 -->

One exact unit and its durable upload checkpoint state.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-29d4fa5d77dd"></a>
- <a id="s-2db9f005e31d"></a>`title`: CollectionUploadUnitWorkDocument
- <a id="s-266a855cc9c2"></a>`description`: One exact unit and its durable upload checkpoint state.
- <a id="s-50ac7bdce080"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5bbbdc396e67"></a>`payload_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-16c47a39dd6c"></a>`plaintext_bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-1922dd648122"></a>`sources` | yes | type="array"; maxItems=1000; items=(#/components/schemas/CollectionUploadUnitSourceDocument); additional keys=`x-riverhog-extent` |  |
| <a id="s-c2d6a2337559"></a>`state` | yes | type="string"; enum=["pending","committed"] |  |
| <a id="s-df608cbe0cc3"></a>`unit` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=1000; minimum=null; progression={"progression":"collection-volume-sequence"}; reason="bounded-upload-unit-source-map"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sources](#s-1922dd648122) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionUploadUnitSourceDocument](schemas-collectionuploadunitsourcedocument.md)

## Governing policies

- <a id="pa-46e2bce4ac61"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-c6747bb3bd60"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadUnitWorkDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 48b2f9a9abca13bad3e86663409b1fd07c2e03c86481f225818208887b2ac196 -->

```json
{
  "additionalProperties": false,
  "description": "One exact unit and its durable upload checkpoint state.",
  "properties": {
    "payload_bytes": {
      "minimum": 0,
      "title": "Payload Bytes",
      "type": "integer"
    },
    "plaintext_bytes": {
      "minimum": 0,
      "title": "Plaintext Bytes",
      "type": "integer"
    },
    "sources": {
      "items": {
        "$ref": "#/components/schemas/CollectionUploadUnitSourceDocument"
      },
      "maxItems": 1000,
      "title": "Sources",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "collection-volume-sequence",
        "reason": "bounded-upload-unit-source-map"
      }
    },
    "state": {
      "enum": [
        "pending",
        "committed"
      ],
      "title": "State",
      "type": "string"
    },
    "unit": {
      "minimum": 0,
      "title": "Unit",
      "type": "integer"
    }
  },
  "required": [
    "unit",
    "payload_bytes",
    "plaintext_bytes",
    "sources",
    "state"
  ],
  "title": "CollectionUploadUnitWorkDocument",
  "type": "object"
}
```
