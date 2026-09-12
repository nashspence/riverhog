# schemas: CollectionUploadRawDigestBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadrawdigestbatchdocument:088760615c -->

One append-only bounded slice of a registered raw source digest sequence.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 2 |

## External contract

<a id="s-8c2dbc8625ef"></a>
- <a id="s-383d2b173ef2"></a>`title`: CollectionUploadRawDigestBatchDocument
- <a id="s-04494a12222e"></a>`description`: One append-only bounded slice of a registered raw source digest sequence.
- <a id="s-eb89fa257f17"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-65e7a28bb357"></a>`first_part` | yes | type="integer"; minimum=0 |  |
| <a id="s-a0efbe2329d3"></a>`path` | yes | type="string" |  |
| <a id="s-2c064419380c"></a>`sha256s` | yes | type="array"; minItems=1; maxItems=1024; items=(type="string"; pattern="^[0-9a-f]{64}$"); additional keys=`x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)

Shared facts for every subject below: maximum=1024; minimum=1; progression={"progression":"first_part"}; reason="bounded-raw-digest-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field sha256s](#s-2c064419380c) | `cardinality · items · segmented_no_total_max` | shared above |

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

Shared facts for every subject below: maximum=64; minimum=64; reason="fixed-public-representation"; source_constraint={"pattern":"^[0-9a-f]{64}$"}

| Applies to | Contract | Bounds or reason |
|---|---|---|
| <a id="s-ef3601ab556b"></a>field sha256s · items | `length · characters · fixed` | shared above |

## Governing policies

- <a id="pa-1d7256555804"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)
- <a id="pa-4729aa64964a"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594af)
- <a id="pa-3142094a425b"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc034)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadRawDigestBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b3e9c783bf5afec4ecfc01d05a15564e73cead5a37ad60e657d59c0a4e586f01 -->

```json
{
  "additionalProperties": false,
  "description": "One append-only bounded slice of a registered raw source digest sequence.",
  "properties": {
    "first_part": {
      "minimum": 0,
      "title": "First Part",
      "type": "integer"
    },
    "path": {
      "title": "Path",
      "type": "string"
    },
    "sha256s": {
      "items": {
        "pattern": "^[0-9a-f]{64}$",
        "type": "string"
      },
      "maxItems": 1024,
      "minItems": 1,
      "title": "Sha256S",
      "type": "array",
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "first_part",
        "reason": "bounded-raw-digest-append"
      }
    }
  },
  "required": [
    "path",
    "first_part",
    "sha256s"
  ],
  "title": "CollectionUploadRawDigestBatchDocument",
  "type": "object"
}
```
