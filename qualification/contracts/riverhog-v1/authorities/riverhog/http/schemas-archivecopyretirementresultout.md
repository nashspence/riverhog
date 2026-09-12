# schemas: ArchiveCopyRetirementResultOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementresultout:33b57e382a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-6edc3b9bb9"></a>
- <a id="s-621c7e006e"></a>`title`: ArchiveCopyRetirementResultOut
- <a id="s-618d3bf1a6"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-133f2d0690"></a>`collection_id` | yes | #/components/schemas/CollectionId |  |
| <a id="s-d6df7ab197"></a>`remote_storage_bytes` | yes | type="integer" |  |
| <a id="s-f3361fbed4"></a>`status` | yes | type="string"; enum=["retired","already_absent"] |  |
| <a id="s-03841b12de"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-f7c4101414"></a>`verified_store` | yes | anyOf=#/components/schemas/ArchiveStoreName \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)
- [schemas: CollectionId](schemas-collectionid.md)

## Governing policies

- <a id="pa-612aa82190"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementResultOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7e7caad5ef04fee87801c920d47e6384a7bd1d8b0929f69cc7816ad28f90cb51 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collection_id": {
      "$ref": "#/components/schemas/CollectionId"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "status": {
      "enum": [
        "retired",
        "already_absent"
      ],
      "title": "Status",
      "type": "string"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "verified_store": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveStoreName"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "required": [
    "status",
    "collection_id",
    "store",
    "remote_storage_bytes",
    "verified_store"
  ],
  "title": "ArchiveCopyRetirementResultOut",
  "type": "object"
}
```
