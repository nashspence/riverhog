# schemas: CollectionUploadTagsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectionuploadtagsout:afafee60df -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d7be81aae95d"></a>
- <a id="s-fc41810c248e"></a>`title`: CollectionUploadTagsOut
- <a id="s-6eeee3c107fc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-164d54cb28a9"></a>`added` | yes | type="integer"; minimum=0 |  |
| <a id="s-24612dce7f12"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-089a99c20e67"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-05f47dfd3d2b"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadTagsOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c2e980a84f0c1aca7b9eec8edfe249d27310773f6adbb3c8db687924abf9196 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "added": {
      "minimum": 0,
      "title": "Added",
      "type": "integer"
    },
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "tag_count": {
      "minimum": 0,
      "title": "Tag Count",
      "type": "integer"
    }
  },
  "required": [
    "collection_id",
    "added",
    "tag_count"
  ],
  "title": "CollectionUploadTagsOut",
  "type": "object"
}
```
