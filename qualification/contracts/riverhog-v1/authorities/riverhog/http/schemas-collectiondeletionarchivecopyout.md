# schemas: CollectionDeletionArchiveCopyOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-collectiondeletionarchivecopyout:1d22347208 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-3af3d570ebe3"></a>
- <a id="s-954c713a3839"></a>`title`: CollectionDeletionArchiveCopyOut
- <a id="s-18e3bfc95cbc"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3bfeff201672"></a>`objects` | yes | type="integer" |  |
| <a id="s-096c6e325d32"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-b0b6a85548c5"></a>`stored_bytes` | yes | type="integer" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-4bdb8105a6bd"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0ba)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459fb8)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241ba8)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4ffa) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc960) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/CollectionDeletionArchiveCopyOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 42d4bdcc75153940ccad496bbce0aa33749ab018e46dded25dddf95544b3db51 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    }
  },
  "required": [
    "store",
    "objects",
    "stored_bytes"
  ],
  "title": "CollectionDeletionArchiveCopyOut",
  "type": "object"
}
```
