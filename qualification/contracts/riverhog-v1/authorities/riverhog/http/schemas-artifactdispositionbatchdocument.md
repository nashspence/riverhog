# schemas: ArtifactDispositionBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionbatchdocument:d059522e18 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-8ff1cc3d66"></a>
- <a id="s-c82e391012"></a>`title`: ArtifactDispositionBatchDocument
- <a id="s-286ef41898"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e310f8fc4d"></a>`dispositions` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/ArtifactDispositionDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |
| <a id="s-ad03539f91"></a>`fence` | yes | type="integer"; minimum=1 |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"sealed-disposition-authority"}; reason="bounded-disposition-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field dispositions](#s-e310f8fc4d) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)

## Governing policies

- <a id="pa-224d3d02a7"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-c3d9752d74"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b1905264e4be951db4e91aec60d3b0a67e294b4bbdd8bdaf7a98c87b545f79e3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "dispositions": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Dispositions",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "sealed-disposition-authority",
        "reason": "bounded-disposition-append"
      }
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "dispositions"
  ],
  "title": "ArtifactDispositionBatchDocument",
  "type": "object"
}
```
