# schemas: BranchSetAdmittedEvent

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: http:stove0:schemas-branchsetadmittedevent:23dd1d777b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | `stove0` |
| Interface | `http` |
| Family | `schemas` |
| Contract elements | 1 |
| Extent decisions | 0 |

## External contract

- `title`: BranchSetAdmittedEvent
- `type`: object

### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| `data` | yes | #/components/schemas/BranchSetAdmittedEventData |  |
| `datacontenttype` | no | type="string"; const="application/json" |  |
| `id` | yes | type="string"; minLength=1 |  |
| `source` | yes | type="string"; const="urn:riverhog:stove0" |  |
| `specversion` | no | type="string"; const="1.0" |  |
| `subject` | yes | type="string"; minLength=1 |  |
| `time` | yes | type="string" |  |
| `type` | yes | type="string"; const="io.riverhog.stove0.branch-set.admitted" |  |

## Maintained corroboration

### Referenced contract dossiers

- [schemas: BranchSetAdmittedEventData](schemas-branchsetadmittedeventdata.md)

## Governing policies

- `compatibility/http-api/v1`

## Evidence

### Qualification

- `make operation-qualification`
- `make compose-smoke`

### Executable sources

- `generator:contract-projection` — `scripts/contract_freeze.py::contract_projection`
- `openapi:stove0` — `.venv/lib/python3.12/site-packages/fastapi/applications.py::FastAPI`

### Machine authority

- `/external_contract/http_openapi/stove0/components/schemas/BranchSetAdmittedEvent`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8aa2c593f2c75b53aa50c593b8c5037816a0439827d84adb52b65acd2b7e84d4 -->

```json
{
  "additionalProperties": false,
  "properties": {
    "data": {
      "$ref": "#/components/schemas/BranchSetAdmittedEventData"
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
      "const": "urn:riverhog:stove0",
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
      "minLength": 1,
      "title": "Subject",
      "type": "string"
    },
    "time": {
      "title": "Time",
      "type": "string"
    },
    "type": {
      "const": "io.riverhog.stove0.branch-set.admitted",
      "title": "Type",
      "type": "string"
    }
  },
  "required": [
    "id",
    "source",
    "type",
    "subject",
    "time",
    "data"
  ],
  "title": "BranchSetAdmittedEvent",
  "type": "object"
}
```
