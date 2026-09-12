# schemas: ArtifactDispositionOutputBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-artifactdispositionoutputbatchdocument:4253497f48 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-61e1fc1e96"></a>
- <a id="s-808b4bbacc"></a>`title`: ArtifactDispositionOutputBatchDocument
- <a id="s-e1ee278302"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-261db41370"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-c4cb257ac3"></a>`outputs` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/ArtifactDispositionOutputDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"sealed-disposition-authority"}; reason="bounded-disposition-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field outputs](#s-c4cb257ac3) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArtifactDispositionOutputDocument](schemas-artifactdispositionoutputdocument.md)

## Governing policies

- <a id="pa-abfce293be"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f15e89ee54"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArtifactDispositionOutputBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9a8553cb62301ed415596ad8bfe5a4c99594157b30d5ee67a82bc913a7e4f211 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "outputs": {
      "items": {
        "$ref": "#/components/schemas/ArtifactDispositionOutputDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Outputs",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "sealed-disposition-authority",
        "reason": "bounded-disposition-append"
      }
    }
  },
  "required": [
    "fence",
    "outputs"
  ],
  "title": "ArtifactDispositionOutputBatchDocument",
  "type": "object"
}
```
