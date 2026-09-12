# schemas: CollectionArtifactBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionartifactbatchdocument:921d81467b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-5d67106b76"></a>
- <a id="s-b4f3aea380"></a>`title`: CollectionArtifactBatchDocument
- <a id="s-97b4185eff"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1c0906aa0f"></a>`artifacts` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/CollectionArtifactIdentityDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |
| <a id="s-1c82d23b05"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-7ef1e1094b"></a>`start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"start_ordinal"}; reason="bounded-authority-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field artifacts](#s-1c0906aa0f) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionArtifactIdentityDocument](schemas-collectionartifactidentitydocument.md)

## Governing policies

- <a id="pa-f5d44bcfca"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-111fa7498d"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionArtifactBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 469c71d8d60e04f3a4aca348b18563579dcf35173ecb0758d55bb914324e21f3 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "artifacts": {
      "items": {
        "$ref": "#/components/schemas/CollectionArtifactIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Artifacts",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-authority-append"
      }
    },
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "start_ordinal": {
      "minimum": 0,
      "title": "Start Ordinal",
      "type": "integer"
    }
  },
  "required": [
    "fence",
    "start_ordinal",
    "artifacts"
  ],
  "title": "CollectionArtifactBatchDocument",
  "type": "object"
}
```
