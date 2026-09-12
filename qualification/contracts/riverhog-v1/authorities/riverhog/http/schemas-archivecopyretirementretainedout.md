# schemas: ArchiveCopyRetirementRetainedOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementretainedout:a78d4d3816 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-d8fb08e288"></a>
- <a id="s-c5d98f2579"></a>`title`: ArchiveCopyRetirementRetainedOut
- <a id="s-3913756386"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6548249a87"></a>`last_verified_at` | yes | type="string" |  |
| <a id="s-ae86cb6aca"></a>`remote_storage_bytes` | yes | type="integer" |  |
| <a id="s-a38cb1ae87"></a>`store` | yes | #/components/schemas/ArchiveStoreName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- <a id="pa-a0060b83aa"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementRetainedOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6180f1093894f89de000aeda85262465faadc86cc8d63c5f7dfccb30d6e0c50c -->

```json
{
  "additionalProperties": false,
  "properties": {
    "last_verified_at": {
      "title": "Last Verified At",
      "type": "string"
    },
    "remote_storage_bytes": {
      "title": "Remote Storage Bytes",
      "type": "integer"
    },
    "store": {
      "$ref": "#/components/schemas/ArchiveStoreName"
    }
  },
  "required": [
    "store",
    "last_verified_at",
    "remote_storage_bytes"
  ],
  "title": "ArchiveCopyRetirementRetainedOut",
  "type": "object"
}
```
