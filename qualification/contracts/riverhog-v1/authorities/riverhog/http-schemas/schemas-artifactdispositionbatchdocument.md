# schemas: ArtifactDispositionBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-artifactdispositionbatchdocument:fc1c024835 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-8ff1cc3d66"></a>

- <a id="s-286ef41898"></a>`type`: `"object"`
- <a id="s-7ad5634dec"></a>`additionalProperties`: `false`
- <a id="s-fed69f5a6a"></a>`required`: `["fence","dispositions"]`
- <a id="s-c82e391012"></a>`title`: `"ArtifactDispositionBatchDocument"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-e310f8fc4d"></a>`dispositions` | yes | type="array"; items=([ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)); maxItems=128; minItems=1; title="Dispositions"; uniqueItems=true; x-riverhog-extent={"policy":"segmented_no_total_max","progression":"sealed-disposition-authority","reason":"bounded-disposition-append"} |  |
| <a id="s-ad03539f91"></a>`fence` | yes | type="integer"; minimum=1; title="Fence" |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"sealed-disposition-authority"}; reason="bounded-disposition-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field dispositions](#s-e310f8fc4d) | `cardinality · items · segmented_no_total_max` | shared above |

### Progression evidence and open obligations

These are candidate test bindings. Group-wide progression claims remain unestablished; inspect the test scopes before applying a result to this contract.

- [riverhog-work-disposition-append/v1](../../../evidence/sources.md#e-5707b3a2d3-401544f03b)

## Maintained corroboration

### Referenced contract dossiers

- [ArtifactDispositionDocument](schemas-artifactdispositiondocument.md)

## Governing policies

- <a id="pa-1e8998800e"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-62cf828ece"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

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

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
