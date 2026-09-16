# schemas: CollectionUploadTagsOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-collectionuploadtagsout:4446f6c171 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-d7be81aae9"></a>

- <a id="s-6eeee3c107"></a>`type`: `"object"`
- <a id="s-ff94553d2d"></a>`additionalProperties`: `false`
- <a id="s-d10a4d1328"></a>`required`: `["collection_id","added","tag_count"]`
- <a id="s-fc41810c24"></a>`title`: `"CollectionUploadTagsOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-164d54cb28"></a>`added` | yes | type="integer"; minimum=0 |  |
| <a id="s-24612dce7f"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-089a99c20e"></a>`tag_count` | yes | type="integer"; minimum=0 |  |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-fe876e71f8"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionUploadTagsOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
