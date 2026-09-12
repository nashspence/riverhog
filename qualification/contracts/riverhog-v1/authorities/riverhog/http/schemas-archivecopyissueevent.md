# schemas: ArchiveCopyIssueEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:riverhog:schemas-archivecopyissueevent:c3eba814e1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog](../index.md) |
| Interface | [http](index.md) |
| Family | [schemas](families/schemas/index.md) |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

<a id="s-dcffbe5ba6"></a>
- <a id="s-ce4407197d"></a>`title`: ArchiveCopyIssueEvent
- <a id="s-5cec970feb"></a>`type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1f0e50ea7f"></a>`data` | yes | #/components/schemas/ArchiveCopyIssueData |  |
| <a id="s-e4e39ce12e"></a>`datacontenttype` | no | type="string"; const="application/json" |  |
| <a id="s-12e8a505b7"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ef33a70260"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-f8187d32b3"></a>`specversion` | no | type="string"; const="1.0" |  |
| <a id="s-ebf240e425"></a>`subject` | no | anyOf=type="string"; minLength=1 \| type="null" |  |
| <a id="s-d33159ed91"></a>`time` | yes | type="string" |  |
| <a id="s-54cd2a7184"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.issue" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: ArchiveCopyIssueData](schemas-archivecopyissuedata.md)

## Governing policies

- <a id="pa-590475ad67"></a>[compatibility/http-api/v1](../../../policies/index.md#p-5bc717c2c0)

## Evidence

### Qualification

- [make operation-qualification](../../../evidence/sources.md#q-dd95e4459f)
- [make compose-smoke](../../../evidence/sources.md#q-413b0b241b)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [openapi:riverhog](../../../evidence/sources.md#src-c42f268fc9) — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/riverhog/components/schemas/ArchiveCopyIssueEvent`

### Exact owned JSON

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
