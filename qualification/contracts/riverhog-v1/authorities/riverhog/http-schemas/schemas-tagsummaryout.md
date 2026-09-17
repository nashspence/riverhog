# schemas: TagSummaryOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-tagsummaryout:3b264e7305 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-7fbf7db9d0"></a>

- <a id="s-e5e2b841db"></a>`type`: `"object"`
- <a id="s-e6c8d2639e"></a>`additionalProperties`: `false`
- <a id="s-cc6da783b3"></a>`required`: `["tag","collection_count"]`
- <a id="s-5b709f4f16"></a>`title`: `"TagSummaryOut"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f58a1f322e"></a>`collection_count` | yes | type="integer"; minimum=1; title="Collection Count" |  |
| <a id="s-06c71a59c8"></a>`tag` | yes | [CollectionTag](schemas-collectiontag.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [CollectionTag](schemas-collectiontag.md)

## Governing policies

- <a id="pa-0203b3c2ec"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/TagSummaryOut`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 354b8d7177581c064ad240b844abe463b451ddae7b4de49cf6616aec2584009a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_count": {
      "minimum": 1,
      "title": "Collection Count",
      "type": "integer"
    },
    "tag": {
      "$ref": "#/components/schemas/CollectionTag"
    }
  },
  "required": [
    "tag",
    "collection_count"
  ],
  "title": "TagSummaryOut",
  "type": "object"
}
```

</details>
