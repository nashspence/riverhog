# schemas: ArchiveStoreOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivestoreout:b097b51a91 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-9491946c66"></a>
- <a id="s-f6da33e223"></a>`title`: ArchiveStoreOut
- <a id="s-b4d948d077"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed8363ddd4"></a>`collections` | yes | type="integer" |  |
| <a id="s-6d9173b8e5"></a>`download_allowance` | yes | anyOf=#/components/schemas/ArchiveDownloadAllowanceOut \| type="null" |  |
| <a id="s-e4bb5e060b"></a>`objects` | yes | type="integer" |  |
| <a id="s-831daffcba"></a>`read_mode` | yes | type="string"; enum=["immediate","restore_required"] |  |
| <a id="s-97417b240e"></a>`read_priority` | yes | type="integer" |  |
| <a id="s-889112d32d"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |
| <a id="s-61213aadde"></a>`stored_bytes` | yes | type="integer" |  |
| <a id="s-94e63b11f8"></a>`write_target` | yes | type="boolean" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveDownloadAllowanceOut](schemas-archivedownloadallowanceout.md)
- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-1afb360fae"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveStoreOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63b4747d492a9117cbf7db30dbe0694eed06b814ae9ed0648d2808cffe495d8e -->

```json
{
  "additionalProperties": false,
  "properties": {
    "collections": {
      "title": "Collections",
      "type": "integer"
    },
    "download_allowance": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveDownloadAllowanceOut"
        },
        {
          "type": "null"
        }
      ]
    },
    "objects": {
      "title": "Objects",
      "type": "integer"
    },
    "read_mode": {
      "enum": [
        "immediate",
        "restore_required"
      ],
      "title": "Read Mode",
      "type": "string"
    },
    "read_priority": {
      "title": "Read Priority",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    },
    "stored_bytes": {
      "title": "Stored Bytes",
      "type": "integer"
    },
    "write_target": {
      "title": "Write Target",
      "type": "boolean"
    }
  },
  "required": [
    "store",
    "read_mode",
    "read_priority",
    "write_target",
    "collections",
    "objects",
    "stored_bytes",
    "download_allowance"
  ],
  "title": "ArchiveStoreOut",
  "type": "object"
}
```
