# schemas: ReplaceCollectionDescriptionRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-replacecollectiondescriptionrequest:ea324cf540 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-eb28174c7c"></a>
- <a id="s-ca4d2ff11d"></a>`title`: ReplaceCollectionDescriptionRequest
- <a id="s-21e4068b3a"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4e86da8103"></a>`description` | yes | anyOf=#/components/schemas/CollectionDescription \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionDescription](schemas-collectiondescription.md)

## Governing policies

- <a id="pa-c91c49b57b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ReplaceCollectionDescriptionRequest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4ad12a4df50fa39cb8315778965953000c4534f6e8a07749fb3d3fe84e2ad95 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "description": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/CollectionDescription"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "description"
  ],
  "title": "ReplaceCollectionDescriptionRequest",
  "type": "object"
}
```
