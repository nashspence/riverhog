# schemas: ArchiveCopyRetirementRequest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http-schemas:riverhog:schemas-archivecopyretirementrequest:0ad67dd4ae -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [HTTP Schemas](index.md) |

## External contract

<a id="s-f63a176ec0"></a>

- <a id="s-b6a61f9849"></a>`type`: `"object"`
- <a id="s-f463f9ce03"></a>`additionalProperties`: `false`
- <a id="s-b363c7f669"></a>`required`: `["collection_id","store"]`
- <a id="s-81229b9827"></a>`title`: `"ArchiveCopyRetirementRequest"`

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-30ce7a571b"></a>`collection_id` | yes | [CollectionId](schemas-collectionid.md) |  |
| <a id="s-166059e92d"></a>`store` | yes | [ArchiveStoreName](schemas-archivestorename.md) |  |

## Maintained corroboration

### Referenced contract dossiers

- [ArchiveStoreName](schemas-archivestorename.md)
- [CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-a16213482a"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — [scripts/operation\_qualification.py::application\_surfaces](../../../../../../scripts/operation_qualification.py#L333)

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementRequest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4447a83a2a2fdddbbfa03921214e61db3f906add5b347f352d0fe01d5d0327f6 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "collection_id",
    "store"
  ],
  "title": "ArchiveCopyRetirementRequest",
  "type": "object"
}
```

</details>
