# schemas: ArchiveCopyJobListFiltersOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyjoblistfiltersout:e8738e9197 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-ad3fd2d59b"></a>
- <a id="s-5c3b9f581f"></a>`title`: ArchiveCopyJobListFiltersOut
- <a id="s-7e66e19fcf"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cccc11a0a0"></a>`state` | no | anyOf=#/components/schemas/ArchiveCopyState \| type="null" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyState](schemas-archivecopystate.md)

## Governing policies

- <a id="pa-03f7aa44a3"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyJobListFiltersOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ffadbfe6a984f8ff781319075ed287f5e4ad500921415287dad46c363a38e567 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "state": {
      "anyOf": [
        {
          "$ref": "#/components/schemas/ArchiveCopyState"
        },
        {
          "type": "null"
        }
      ]
    }
  },
  "title": "ArchiveCopyJobListFiltersOut",
  "type": "object"
}
```
