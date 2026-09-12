# schemas: ArchiveCopyIssueEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyissueevent:c3eba814e1 -->

| Audit field | Value |
|---|---|
| Authority | `riverhog` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyIssueEvent`

## Effective policies

- `compatibility/http-api/v1`

## Executable sources and proof

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:riverhog` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`
- Proof: `make operation-qualification`
- Proof: `make compose-smoke`

## Referenced contract dossiers

- [schemas: ArchiveCopyIssueData](schemas-archivecopyissuedata.md)

## Contract summary

- `title`: ArchiveCopyIssueEvent
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `data` | yes | #/components/schemas/ArchiveCopyIssueData |  |
| `datacontenttype` | no | string |  |
| `id` | yes | string |  |
| `source` | yes | string |  |
| `specversion` | no | string |  |
| `subject` | no | object (2 fields) |  |
| `time` | yes | string |  |
| `type` | yes | string |  |

## Complete owned contract

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cd66db4c3187058a9b9944525713e0e20005e58d4cfecbf5b2500a257096a880 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/ArchiveCopyIssueData"
    },
    "datacontenttype": {
      "const": "application/json",
      "default": "application/json",
      "title": "Datacontenttype",
      "type": "string"
    },
    "id": {
      "minLength": 1,
      "title": "Id",
      "type": "string"
    },
    "source": {
      "minLength": 1,
      "title": "Source",
      "type": "string"
    },
    "specversion": {
      "const": "1.0",
      "default": "1.0",
      "title": "Specversion",
      "type": "string"
    },
    "subject": {
      "anyOf": [
        {
          "minLength": 1,
          "type": "string"
        },
        {
          "type": "null"
        }
      ],
      "title": "Subject"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.riverhog.archive_copy.issue",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "time",
    "data"
  ],
  "title": "ArchiveCopyIssueEvent",
  "type": "object"
}
```
