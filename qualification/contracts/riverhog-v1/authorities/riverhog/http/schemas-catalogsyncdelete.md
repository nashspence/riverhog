# schemas: CatalogSyncDelete

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-catalogsyncdelete:16435f0b3c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 1 |

## External contract

<a id="s-1c0897aa29"></a>
- <a id="s-7364b55735"></a>`title`: CatalogSyncDelete
- <a id="s-121661ebff"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ba3c39e6c8"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-ca1b3b01b0"></a>`operation` | no | type="string"; const="delete" |  |
| <a id="s-8a05c66a42"></a>`revision` | yes | type="string"; minLength=1; maxLength=19; pattern="^(?:[1-9][0-9]{0,17}\|[1-8][0-9]{18})$" |  |

### Progression, limits, and lifecycle

#### [extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

Shared facts for every subject below: maximum=19; minimum=1; reason="schema-maximum"

| Applies to | Contract | Bounds or reason |
|---|---|---|
| [field revision](#s-8a05c66a42) | `length · characters · contract_max` | shared above |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-f2b226d84c"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)
- <a id="pa-f663d27736"></a>[extent-rule/schema-bound/v1](../../../policies/index.md#p-c0db822fc0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CatalogSyncDelete`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6f3fb51bea4d0b80c512399c325902293c78eb4ff2a0040187f78eff54653eb0 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "operation": {
      "const": "delete",
      "default": "delete",
      "title": "Operation",
      "type": "string"
    },
    "revision": {
      "maxLength": 19,
      "minLength": 1,
      "pattern": "^(?:[1-9][0-9]{0,17}|[1-8][0-9]{18})$",
      "title": "Revision",
      "type": "string"
    }
  },
  "required": [
    "collection_id",
    "revision"
  ],
  "title": "CatalogSyncDelete",
  "type": "object"
}
```
