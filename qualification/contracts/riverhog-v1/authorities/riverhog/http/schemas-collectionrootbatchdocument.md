# schemas: CollectionRootBatchDocument

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionrootbatchdocument:b8e60c7202 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-beeeba53c5"></a>
- <a id="s-b7d49ba41a"></a>`title`: CollectionRootBatchDocument
- <a id="s-fbd771a65a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-94a5c12b16"></a>`fence` | yes | type="integer"; minimum=1 |  |
| <a id="s-ab04ca5dfd"></a>`inputs` | yes | type="array"; minItems=1; maxItems=128; items=(#/components/schemas/CollectionRootIdentityDocument); additional keys=`uniqueItems`, `x-riverhog-extent` |  |
| <a id="s-9c6772d7f8"></a>`start_ordinal` | yes | type="integer"; minimum=0 |  |

### Progression, limits, and lifecycle

#### [extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

Shared facts for every subject below: maximum=128; minimum=1; progression={"progression":"start_ordinal"}; reason="bounded-authority-append"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field inputs](#s-ab04ca5dfd) | `cardinality · items · segmented_no_total_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionRootIdentityDocument](schemas-collectionrootidentitydocument.md)

## Governing policies

- <a id="pa-aa60b8e1c4"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-a8bd10df5a"></a>[extent-rule/bounded-segment/v1](../../../policies/index.md#p-2b3f3f1594)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionRootBatchDocument`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: acf1b1c5363045debca142fa788e0aa4a48f38956eb2841b6c5949e94253710c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "fence": {
      "minimum": 1,
      "title": "Fence",
      "type": "integer"
    },
    "inputs": {
      "items": {
        "$ref": "#/components/schemas/CollectionRootIdentityDocument"
      },
      "maxItems": 128,
      "minItems": 1,
      "title": "Inputs",
      "type": "array",
      "uniqueItems": true,
      "x-riverhog-extent": {
        "policy": "segmented_no_total_max",
        "progression": "start_ordinal",
        "reason": "bounded-authority-append"
      }
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
    "inputs"
  ],
  "title": "CollectionRootBatchDocument",
  "type": "object"
}
```
