# schemas: ArchiveCopyRetirementTargetOut

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyretirementtargetout:2a5a28b673 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: ArchiveCopyRetirementTargetOut
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `last_verified_at` | yes | type="string" |  |
| `object_count` | yes | type="integer" |  |
| `remote_storage_bytes` | yes | type="integer" |  |
| `store` | yes | #/components/schemas/ArchiveStoreName |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveStoreName](schemas-archivestorename.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyRetirementTargetOut`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 644fcc1fb33a87da56a4ef230512696f5a547d855e15361bef9f27105e20a11a -->

```json
{
  "additionalProperties": false,
  "properties": {
    "last_verified_at": {
      "title": "Last Verified At",
      "type": "string"
    },
    "object_count": {
      "title": "Object Count",
      "type": "integer"
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
    "remote_storage_bytes",
    "object_count"
  ],
  "title": "ArchiveCopyRetirementTargetOut",
  "type": "object"
}
```
