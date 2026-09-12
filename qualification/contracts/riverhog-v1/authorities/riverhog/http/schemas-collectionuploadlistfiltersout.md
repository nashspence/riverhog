# schemas: CollectionUploadListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadlistfiltersout:b245bb0b24 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-a3bf92561f"></a>
- <a id="s-5f38231474"></a>`title`: CollectionUploadListFiltersOut
- <a id="s-26e41b9a11"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f0a343edfd"></a>`state` | yes | anyOf=#/components/schemas/CollectionUploadState \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionUploadState](schemas-collectionuploadstate.md)

## Governing policies

- <a id="pa-62326c9b62"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadListFiltersOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2367deaea2a301a7886ae46cf9df477060428a27b716384f27d61e80cd32b82c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionUploadState"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "state"
  ],
  "title": "CollectionUploadListFiltersOut",
  "type": "object"
}
```
